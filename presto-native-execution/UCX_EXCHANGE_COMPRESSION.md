# UCX exchange compression integration

GPU exchange compression is implemented in Velox's experimental UCX exchange.
This Presto branch contains only the native-worker hooks needed to use that
transport.

There are two related but distinct changes:

1. The native server starts and stops the UCX communicator when
   `cudf.exchange=true`. Each worker requires a unique
   `cudf.exchange.server.port`.
2. Plan conversion preserves the coordinator's output-transport choice.
   `ANY` selects the Velox UCX transport. Explicit `HTTP`, or an absent
   transport field, keeps the existing in-memory output path used by HTTP
   exchange.

The second change is covered by plan-converter tests for all three cases. It is
not compression policy. It ensures that a plan selected for UCX actually
reaches the UCX output operator.

The native worker already passes its complete configuration map to
`CudfConfig`. The matching Velox branch therefore owns the compression
properties, codecs, adaptive selector, and wire decoder. No duplicate
compression implementation is present in Presto.

## Required components

Use matching branches of:

- `mattgara/presto:ucx-exchange-compression`
- `mattgara/velox:ucx-exchange-compression`
- `rapidsai/velox-testing:mattgara/ucx-exchange-compression`

The Velox branch documents the codec and wire format in
`velox/experimental/ucx-exchange/COMPRESSION.md`. The velox-testing branch
provides the worker configuration example.

## Worker configuration

At minimum, a multi-worker native deployment needs:

```properties
cudf.enabled=true
cudf.exchange=true
cudf.exchange.server.port=<unique worker port>
```

Compression remains opt-in. Its properties belong in the same worker-native
configuration file. Do not enable a compressed wire format until every worker
uses the matching Velox decoder.

## Validation

The focused Presto test is:

```bash
cmake --build _build/release -j4 --target presto_expressions_test
ctest --test-dir _build/release -R '^presto_expressions_test$' --output-on-failure
```

For an end-to-end run, confirm the worker log reports that the cuDF exchange
server started and that the plan selected UCX rather than HTTP exchange. Then
follow the Velox compression document's byte-exact and result-correctness
checks.
