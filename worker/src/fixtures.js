export const FIXTURES = Object.freeze({
  "synthetic-v1": Object.freeze({
    id: "synthetic-v1",
    text_template: "synthetic row {source_index}",
    payload_shape: "vector-text",
    transform: "deterministic-vector-v1",
  }),
});

export function readAndTransformFixture(name, input) {
  const fixture = FIXTURES[name];
  if (!fixture) throw new Error(`unknown source_fixture: ${name}`);
  return {
    fixture_id: fixture.id,
    transform: fixture.transform,
    text_template: fixture.text_template,
    payload_shape: input.payload_shape || fixture.payload_shape,
  };
}
