use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator,
    RecordBatchReader, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

const TABLE: &str = "data";

#[derive(Clone)]
struct AppState {
    storage_uri: String,
    storage_options: HashMap<String, String>,
    checkpoints: Arc<dyn ObjectStore>,
    auth_token: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CommitRequest {
    run_id: String,
    chunk_id: String,
    writer_id: String,
    start: u64,
    rows: usize,
    dimensions: i32,
    batch_size: usize,
    seed: u64,
    payload_shape: String,
    max_retries: u32,
    mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CheckpointState {
    Prepared,
    CommitAttempted,
    Committed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    run_id: String,
    chunk_id: String,
    state: CheckpointState,
    writer_id: String,
    source_start: u64,
    source_end: u64,
    row_count: usize,
    checksum: String,
    base_lance_version: Option<u64>,
    committed_lance_version: Option<u64>,
    retry_count: u32,
    created_at_ms: u128,
    updated_at_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct CommitResponse {
    checkpoint: Checkpoint,
    elapsed_ms: u128,
    commit_attempts: u32,
    conflict_retries: u32,
    peak_rss_kb: Option<u64>,
}

fn peak_rss_kb() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status").ok()?.lines()
        .find(|line| line.starts_with("VmHWM:"))?
        .split_whitespace().nth(1)?.parse().ok()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
fn checkpoint_path(request: &CommitRequest) -> Path {
    Path::from(format!(
        "control/runs/{}/chunks/{}.json",
        request.run_id,
        blake3::hash(request.chunk_id.as_bytes()).to_hex()
    ))
}

fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|actual| constant_time_eq(actual.as_bytes(), expected.as_bytes()))
        .unwrap_or(false)
}
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

fn schema(dimensions: i32) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("chunk_id", DataType::Utf8, false),
        Field::new("source_index", DataType::UInt64, false),
        Field::new("writer_id", DataType::Utf8, false),
        Field::new("seed", DataType::UInt64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("payload_bytes", DataType::UInt32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimensions,
            ),
            false,
        ),
    ]))
}

fn empty_batch(dimensions: i32) -> Result<RecordBatch> {
    let item = Arc::new(Field::new("item", DataType::Float32, true));
    let vectors = FixedSizeListArray::try_new(
        item,
        dimensions,
        Arc::new(Float32Array::from(Vec::<f32>::new())),
        None,
    )?;
    Ok(RecordBatch::try_new(
        schema(dimensions),
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(UInt64Array::from(Vec::<u64>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(UInt64Array::from(Vec::<u64>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(UInt32Array::from(Vec::<u32>::new())),
            Arc::new(vectors),
        ],
    )?)
}

fn chunk_checksum(request: &CommitRequest) -> String {
    let mut hash = blake3::Hasher::new();
    for index in 0..request.rows {
        hash.update(format!("{}:{}", request.run_id, request.start + index as u64).as_bytes());
        hash.update(&[0]);
    }
    hash.finalize().to_hex().to_string()
}

fn make_batch(request: &CommitRequest) -> Result<RecordBatch> {
    anyhow::ensure!(
        request.rows > 0 && request.dimensions > 0 && request.batch_size > 0,
        "rows, dimensions and batch_size must be positive"
    );
    anyhow::ensure!(
        matches!(request.mode.as_str(), "independent" | "funnel"),
        "invalid concurrency mode"
    );
    let ids: Vec<String> = (0..request.rows)
        .map(|i| format!("{}:{}", request.run_id, request.start + i as u64))
        .collect();
    let source: Vec<u64> = (0..request.rows)
        .map(|i| request.start + i as u64)
        .collect();
    let writers: Vec<String> = (0..request.rows)
        .map(|_| request.writer_id.clone())
        .collect();
    let seeds = vec![request.seed; request.rows];
    let texts: Vec<String> = source
        .iter()
        .map(|i| format!("synthetic row {i} seed {}", request.seed))
        .collect();
    let payload_bytes: Vec<u32> = ids.iter().zip(&texts).map(|(id, text)| {
        (id.len() + request.chunk_id.len() + request.writer_id.len() + text.len()
            + request.payload_shape.len() + request.dimensions as usize * size_of::<f32>()
            + size_of::<u64>() * 2) as u32
    }).collect();
    let mut values = Vec::with_capacity(request.rows * request.dimensions as usize);
    for index in &source {
        for dimension in 0..request.dimensions as u64 {
            let bits = blake3::hash(format!("{}:{index}:{dimension}", request.seed).as_bytes());
            values.push(
                (u32::from_le_bytes(bits.as_bytes()[0..4].try_into().unwrap()) as f64
                    / u32::MAX as f64) as f32,
            );
        }
    }
    let item = Arc::new(Field::new("item", DataType::Float32, true));
    let vectors = FixedSizeListArray::try_new(
        item,
        request.dimensions,
        Arc::new(Float32Array::from(values)),
        None,
    )?;
    let batch = RecordBatch::try_new(
        schema(request.dimensions),
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(vec![
                request.chunk_id.clone();
                request.rows
            ])),
            Arc::new(UInt64Array::from(source)),
            Arc::new(StringArray::from(writers)),
            Arc::new(UInt64Array::from(seeds)),
            Arc::new(StringArray::from(texts)),
            Arc::new(UInt32Array::from(payload_bytes)),
            Arc::new(vectors),
        ],
    )?;
    Ok(batch)
}

async fn read_checkpoint(state: &AppState, path: &Path) -> Result<Option<Checkpoint>> {
    match state.checkpoints.get(path).await {
        Ok(result) => Ok(Some(serde_json::from_slice(&result.bytes().await?)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}
async fn write_checkpoint(state: &AppState, path: &Path, checkpoint: &Checkpoint) -> Result<()> {
    state
        .checkpoints
        .put(
            path,
            Bytes::from(serde_json::to_vec_pretty(checkpoint)?).into(),
        )
        .await?;
    Ok(())
}
fn retryable(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    [
        "commit conflict",
        "already exists",
        "precondition",
        "412",
        "conflict",
        "version",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn chunk_rows(table: &lancedb::Table, chunk_id: &str) -> Result<usize> {
    Ok(table
        .count_rows(Some(format!("chunk_id = {}", sql_string(chunk_id))))
        .await?)
}

async fn health() -> &'static str {
    "ok"
}
async fn commit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CommitRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    if !authorized(&headers, &state.auth_token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error":"unauthorized"})),
        );
    }
    match commit_inner(&state, &request).await {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).unwrap()),
        ),
        Err(error) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": format!("{error:#}")})),
        ),
    }
}

async fn commit_inner(state: &AppState, request: &CommitRequest) -> Result<CommitResponse> {
    let started = Instant::now();
    let path = checkpoint_path(request);
    if let Some(checkpoint) = read_checkpoint(state, &path).await? {
        if checkpoint.state == CheckpointState::Committed {
            return Ok(CommitResponse {
                conflict_retries: checkpoint.retry_count,
                commit_attempts: 0,
                checkpoint,
                elapsed_ms: started.elapsed().as_millis(),
                peak_rss_kb: peak_rss_kb(),
            });
        }
    }
    let uri = format!(
        "{}/runs/{}",
        state.storage_uri.trim_end_matches('/'),
        request.run_id
    );
    let connection = lancedb::connect(&uri)
        .storage_options(state.storage_options.clone())
        .execute()
        .await?;
    let table = match connection.open_table(TABLE).execute().await {
        Ok(table) => table,
        Err(_) => {
            let empty = empty_batch(request.dimensions)?;
            let reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
                vec![Ok(empty)],
                schema(request.dimensions),
            ));
            match connection.create_table(TABLE, reader).execute().await {
                Ok(table) => table,
                Err(_) => connection.open_table(TABLE).execute().await?,
            }
        }
    };
    let base = table.version().await.ok();
    let timestamp = now_ms();
    let mut checkpoint = Checkpoint {
        run_id: request.run_id.clone(),
        chunk_id: request.chunk_id.clone(),
        state: CheckpointState::Prepared,
        writer_id: request.writer_id.clone(),
        source_start: request.start,
        source_end: request.start + request.rows as u64,
        row_count: request.rows,
        checksum: chunk_checksum(request),
        base_lance_version: base,
        committed_lance_version: None,
        retry_count: 0,
        created_at_ms: timestamp,
        updated_at_ms: timestamp,
        error: None,
    };
    write_checkpoint(state, &path, &checkpoint).await?;
    let batch = make_batch(request)?;
    for attempt in 0..=request.max_retries {
        table.checkout_latest().await?;
        if chunk_rows(&table, &request.chunk_id).await? == request.rows {
            checkpoint.state = CheckpointState::Committed;
            checkpoint.committed_lance_version = table.version().await.ok();
            checkpoint.retry_count = attempt;
            checkpoint.updated_at_ms = now_ms();
            write_checkpoint(state, &path, &checkpoint).await?;
            return Ok(CommitResponse {
                checkpoint,
                elapsed_ms: started.elapsed().as_millis(),
                commit_attempts: attempt,
                conflict_retries: attempt,
                peak_rss_kb: peak_rss_kb(),
            });
        }
        checkpoint.state = CheckpointState::CommitAttempted;
        checkpoint.retry_count = attempt;
        checkpoint.writer_id = request.writer_id.clone();
        checkpoint.updated_at_ms = now_ms();
        write_checkpoint(state, &path, &checkpoint).await?;
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(batch.clone())],
            batch.schema(),
        ));
        match table.add(reader).execute().await {
            Ok(_) => {
                checkpoint.state = CheckpointState::Committed;
                checkpoint.committed_lance_version = table.version().await.ok();
                checkpoint.updated_at_ms = now_ms();
                write_checkpoint(state, &path, &checkpoint).await?;
                return Ok(CommitResponse {
                    checkpoint,
                    elapsed_ms: started.elapsed().as_millis(),
                    commit_attempts: attempt + 1,
                    conflict_retries: attempt,
                    peak_rss_kb: peak_rss_kb(),
                });
            }
            Err(error) if retryable(&error.to_string()) && attempt < request.max_retries => {
                let jitter = (blake3::hash(format!("{}:{attempt}", request.chunk_id).as_bytes())
                    .as_bytes()[0] as u64)
                    % 41;
                tokio::time::sleep(Duration::from_millis((25u64 << attempt.min(6)) + jitter)).await;
            }
            Err(error) => {
                checkpoint.state = CheckpointState::Failed;
                checkpoint.error = Some(error.to_string());
                checkpoint.updated_at_ms = now_ms();
                write_checkpoint(state, &path, &checkpoint).await?;
                anyhow::bail!("commit exhausted after {} attempt(s): {error}", attempt + 1);
            }
        }
    }
    unreachable!()
}

#[derive(Debug, Serialize)]
struct RunStatus {
    run_id: String,
    checkpoints: Vec<Checkpoint>,
    prepared: usize,
    commit_attempted: usize,
    committed: usize,
    failed: usize,
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(run_id): AxumPath<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    if !authorized(&headers, &state.auth_token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error":"unauthorized"})),
        );
    }
    match status_inner(&state, &run_id).await {
        Ok(value) => (StatusCode::OK, Json(serde_json::to_value(value).unwrap())),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":format!("{error:#}")})),
        ),
    }
}

async fn status_inner(state: &AppState, run_id: &str) -> Result<RunStatus> {
    let prefix = Path::from(format!("control/runs/{run_id}/chunks"));
    let mut stream = state.checkpoints.list(Some(&prefix));
    let mut checkpoints = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        let body = state.checkpoints.get(&meta.location).await?.bytes().await?;
        checkpoints.push(serde_json::from_slice::<Checkpoint>(&body)?);
    }
    checkpoints.sort_by_key(|value| value.source_start);
    Ok(RunStatus {
        run_id: run_id.to_string(),
        prepared: checkpoints
            .iter()
            .filter(|value| value.state == CheckpointState::Prepared)
            .count(),
        commit_attempted: checkpoints
            .iter()
            .filter(|value| value.state == CheckpointState::CommitAttempted)
            .count(),
        committed: checkpoints
            .iter()
            .filter(|value| value.state == CheckpointState::Committed)
            .count(),
        failed: checkpoints
            .iter()
            .filter(|value| value.state == CheckpointState::Failed)
            .count(),
        checkpoints,
    })
}

async fn open_table(state: &AppState, run_id: &str) -> Result<lancedb::Table> {
    let uri = format!("{}/runs/{run_id}", state.storage_uri.trim_end_matches('/'));
    Ok(lancedb::connect(&uri)
        .storage_options(state.storage_options.clone())
        .execute()
        .await?
        .open_table(TABLE)
        .execute()
        .await?)
}

#[derive(Debug, Deserialize)]
struct VerifyRequest {
    run_id: String,
    rows: usize,
    dimensions: i32,
    seed: u64,
}

#[derive(Debug, Serialize)]
struct VerifyResponse {
    run_id: String,
    valid: bool,
    expected_rows: usize,
    actual_rows: usize,
    unique_ids: usize,
    missing_ids: Vec<String>,
    duplicate_ids: Vec<String>,
    schema_matches: bool,
    vector_dimensions: i32,
    seed_matches: bool,
    readable_versions: Vec<u64>,
    sampled_ids: Vec<String>,
    checksum: String,
}

async fn verify(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<VerifyRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    if !authorized(&headers, &state.auth_token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error":"unauthorized"})),
        );
    }
    match verify_inner(&state, &request).await {
        Ok(value) => (
            if value.valid {
                StatusCode::OK
            } else {
                StatusCode::CONFLICT
            },
            Json(serde_json::to_value(value).unwrap()),
        ),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":format!("{error:#}")})),
        ),
    }
}

async fn verify_inner(state: &AppState, request: &VerifyRequest) -> Result<VerifyResponse> {
    let table = open_table(state, &request.run_id).await?;
    table.checkout_latest().await?;
    let table_schema = table.schema().await?;
    let batches: Vec<RecordBatch> = table
        .query()
        .select(Select::columns(&["id", "seed", "vector"]))
        .execute()
        .await?
        .try_collect()
        .await?;
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut seed_matches = true;
    let mut checksum_pairs = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .context("id column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("id type")?;
        let seeds = batch
            .column_by_name("seed")
            .context("seed column")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("seed type")?;
        for row in 0..batch.num_rows() {
            let id = ids.value(row).to_string();
            *counts.entry(id.clone()).or_default() += 1;
            seed_matches &= seeds.value(row) == request.seed;
            let index = id
                .rsplit(':')
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(u64::MAX);
            checksum_pairs.push((index, id));
        }
    }
    checksum_pairs.sort_by_key(|(index, _)| *index);
    let mut hasher = blake3::Hasher::new();
    for (_, id) in &checksum_pairs {
        hasher.update(id.as_bytes());
        hasher.update(&[0]);
    }
    let expected: HashSet<String> = (0..request.rows)
        .map(|index| format!("{}:{index}", request.run_id))
        .collect();
    let actual: HashSet<String> = counts.keys().cloned().collect();
    let mut missing_ids: Vec<_> = expected.difference(&actual).cloned().collect();
    missing_ids.sort();
    let mut duplicate_ids: Vec<_> = counts
        .iter()
        .filter(|(_, count)| **count != 1)
        .map(|(id, _)| id.clone())
        .collect();
    duplicate_ids.sort();
    let schema_matches = table_schema.as_ref() == schema(request.dimensions).as_ref();
    let actual_rows = checksum_pairs.len();
    let current = table.version().await?;
    let mut readable_versions = Vec::new();
    for version in 1..=current {
        if table.checkout(version).await.is_ok() && table.count_rows(None).await.is_ok() {
            readable_versions.push(version);
        }
    }
    table.checkout_latest().await?;
    let sampled_ids = checksum_pairs
        .iter()
        .take(3)
        .chain(checksum_pairs.iter().rev().take(3))
        .map(|(_, id)| id.clone())
        .collect();
    Ok(VerifyResponse {
        run_id: request.run_id.clone(),
        valid: actual_rows == request.rows
            && counts.len() == request.rows
            && missing_ids.is_empty()
            && duplicate_ids.is_empty()
            && schema_matches
            && seed_matches
            && readable_versions.len() == current as usize,
        expected_rows: request.rows,
        actual_rows,
        unique_ids: counts.len(),
        missing_ids,
        duplicate_ids,
        schema_matches,
        vector_dimensions: request.dimensions,
        seed_matches,
        readable_versions,
        sampled_ids,
        checksum: hasher.finalize().to_hex().to_string(),
    })
}

#[derive(Debug, Deserialize)]
struct QueryRequest {
    run_id: String,
    dimensions: i32,
    seed: u64,
    source_index: u64,
    exact: bool,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    run_id: String,
    exact: bool,
    table_version: u64,
    elapsed_ms: u128,
    ids: Vec<String>,
    index_count: usize,
}

async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    if !authorized(&headers, &state.auth_token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error":"unauthorized"})),
        );
    }
    match query_inner(&state, &request).await {
        Ok(value) => (StatusCode::OK, Json(serde_json::to_value(value).unwrap())),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":format!("{error:#}")})),
        ),
    }
}

async fn query_inner(state: &AppState, request: &QueryRequest) -> Result<QueryResponse> {
    let started = Instant::now();
    let table = open_table(state, &request.run_id).await?;
    let mut vector = Vec::with_capacity(request.dimensions as usize);
    for dimension in 0..request.dimensions as u64 {
        let bits = blake3::hash(
            format!("{}:{}:{dimension}", request.seed, request.source_index).as_bytes(),
        );
        vector.push(
            (u32::from_le_bytes(bits.as_bytes()[0..4].try_into().unwrap()) as f64 / u32::MAX as f64)
                as f32,
        );
    }
    let mut builder = table
        .query()
        .nearest_to(vector)?
        .column("vector")
        .limit(request.limit.unwrap_or(10));
    if request.exact {
        builder = builder.bypass_vector_index();
    }
    let batches: Vec<RecordBatch> = builder
        .select(Select::columns(&["id"]))
        .execute()
        .await?
        .try_collect()
        .await?;
    let ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(str::to_string)
        })
        .collect();
    Ok(QueryResponse {
        run_id: request.run_id.clone(),
        exact: request.exact,
        table_version: table.version().await?,
        elapsed_ms: started.elapsed().as_millis(),
        ids,
        index_count: table.list_indices().await?.len(),
    })
}

#[derive(Debug, Deserialize)]
struct IndexRequest {
    run_id: String,
}

async fn create_index(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IndexRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    if !authorized(&headers, &state.auth_token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error":"unauthorized"})),
        );
    }
    let result: Result<serde_json::Value> = async {
        let table = open_table(&state, &request.run_id).await?;
        table
            .create_index(&["vector"], Index::Auto)
            .replace(true)
            .execute()
            .await?;
        Ok(serde_json::json!({
            "run_id": request.run_id,
            "table_version": table.version().await?,
            "indices": table.list_indices().await?.len()
        }))
    }
    .await;
    match result {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":format!("{error:#}")})),
        ),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let endpoint = std::env::var("BENCH_S3_ENDPOINT").context("BENCH_S3_ENDPOINT")?;
    let region = std::env::var("BENCH_S3_REGION").unwrap_or_else(|_| "auto".into());
    let access = std::env::var("BENCH_S3_ACCESS_KEY").context("BENCH_S3_ACCESS_KEY")?;
    let secret = std::env::var("BENCH_S3_SECRET_KEY").context("BENCH_S3_SECRET_KEY")?;
    let storage_uri = std::env::var("BENCH_STORAGE_URI").context("BENCH_STORAGE_URI")?;
    let bucket = storage_uri
        .strip_prefix("s3://")
        .context("BENCH_STORAGE_URI must be s3://bucket")?
        .split('/')
        .next()
        .unwrap();
    let allow_http = endpoint.starts_with("http://");
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&region)
        .with_endpoint(&endpoint)
        .with_access_key_id(&access)
        .with_secret_access_key(&secret)
        .with_virtual_hosted_style_request(false)
        .with_allow_http(allow_http);
    let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
    if let Some(token) = session_token.as_ref() {
        if !token.is_empty() {
            builder = builder.with_token(token);
        }
    }
    let checkpoints: Arc<dyn ObjectStore> = Arc::new(builder.build()?);
    let mut storage_options = HashMap::from([
        ("aws_endpoint".into(), endpoint),
        ("aws_region".into(), region),
        ("aws_access_key_id".into(), access),
        ("aws_secret_access_key".into(), secret),
        ("aws_virtual_hosted_style_request".into(), "false".into()),
        ("allow_http".into(), allow_http.to_string()),
    ]);
    if let Some(token) = session_token {
        if !token.is_empty() {
            storage_options.insert("aws_session_token".into(), token);
        }
    }
    let state = AppState {
        storage_uri,
        storage_options,
        checkpoints,
        auth_token: std::env::var("BENCH_AUTH_TOKEN").context("BENCH_AUTH_TOKEN")?,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/chunks/commit", post(commit))
        .route("/runs/{run_id}/status", get(status))
        .route("/verify", post(verify))
        .route("/query", post(query))
        .route("/index", post(create_index))
        .with_state(state);
    let bind = std::env::var("BENCH_BIND").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let listener = TcpListener::bind(&bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    fn request() -> CommitRequest {
        CommitRequest {
            run_id: "r".into(),
            chunk_id: "c".into(),
            writer_id: "w".into(),
            start: 0,
            rows: 3,
            dimensions: 4,
            batch_size: 2,
            seed: 9,
            payload_shape: "vector-text".into(),
            max_retries: 3,
            mode: "independent".into(),
        }
    }
    #[test]
    fn generator_is_deterministic_and_ids_are_unique() {
        let a = make_batch(&request()).unwrap();
        let b = make_batch(&request()).unwrap();
        assert_eq!(chunk_checksum(&request()), chunk_checksum(&request()));
        assert_eq!(a, b);
        let ids = a.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(ids.iter().flatten().collect::<HashSet<_>>().len(), 3);
    }
    #[test]
    fn retry_classifier_is_bounded_to_conflicts() {
        assert!(retryable("412 Precondition Failed"));
        assert!(!retryable("permission denied"));
    }
}
