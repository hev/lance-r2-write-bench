use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator,
    RecordBatchReader, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
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

fn make_batch(request: &CommitRequest) -> Result<(RecordBatch, String)> {
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
    let payload_bytes = vec![request.payload_shape.len() as u32; request.rows];
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
    let checksum = {
        let mut hash = blake3::Hasher::new();
        for id in &ids {
            hash.update(id.as_bytes());
            hash.update(&[0]);
        }
        hash.finalize().to_hex().to_string()
    };
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
            Arc::new(UInt64Array::from(source)),
            Arc::new(StringArray::from(writers)),
            Arc::new(UInt64Array::from(seeds)),
            Arc::new(StringArray::from(texts)),
            Arc::new(UInt32Array::from(payload_bytes)),
            Arc::new(vectors),
        ],
    )?;
    Ok((batch, checksum))
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
            });
        }
    }
    let (batch, checksum) = make_batch(request)?;
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
            let empty = batch.slice(0, 0);
            let reader: Box<dyn RecordBatchReader + Send> =
                Box::new(RecordBatchIterator::new(vec![Ok(empty)], batch.schema()));
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
        checksum,
        base_lance_version: base,
        committed_lance_version: None,
        retry_count: 0,
        created_at_ms: timestamp,
        updated_at_ms: timestamp,
        error: None,
    };
    write_checkpoint(state, &path, &checkpoint).await?;
    for attempt in 0..=request.max_retries {
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
    if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
        if !token.is_empty() {
            builder = builder.with_token(token);
        }
    }
    let checkpoints: Arc<dyn ObjectStore> = Arc::new(builder.build()?);
    let storage_options = HashMap::from([
        ("aws_endpoint".into(), endpoint),
        ("aws_region".into(), region),
        ("aws_access_key_id".into(), access),
        ("aws_secret_access_key".into(), secret),
        ("aws_virtual_hosted_style_request".into(), "false".into()),
        ("allow_http".into(), allow_http.to_string()),
    ]);
    let state = AppState {
        storage_uri,
        storage_options,
        checkpoints,
        auth_token: std::env::var("BENCH_AUTH_TOKEN").context("BENCH_AUTH_TOKEN")?,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/chunks/commit", post(commit))
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
        let (a, ca) = make_batch(&request()).unwrap();
        let (b, cb) = make_batch(&request()).unwrap();
        assert_eq!(ca, cb);
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
