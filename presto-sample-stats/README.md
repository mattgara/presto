# Sample statistics provider

Presto's cost-based planner chooses join orders and join distribution from table statistics. For filters, joins and
aggregations it can only guess selectivities from those statistics (for example a `LIKE '%green%'` filter or a
`HAVING sum(...) > 300`), and wrong guesses give slow or memory-hungry plans on large scale factors. History-based
optimization (HBO) fixes the guesses with the row counts of earlier runs of the same query. That needs a warm-up
run of every query and a statistics store (Redis), and a benchmark that relies on it measures a planner that has
seen the queries before.

This provider gives the planner the same kind of estimates without history, and uses history once it exists. At
planning time Presto asks the history-based statistics provider for the output of each plan node. Statistics that
Presto recorded for the node in earlier runs come first. Otherwise, for a node over one table (a scan with its
pushed-down filters, then filters, projections and aggregations), a fragment engine evaluates the subtree on a small
sample of the table and the provider scales the row count to the full table. The samples are written once, after
`ANALYZE`, by `sample_tables.py`. No Presto changes are needed: the provider is a plugin that implements Presto's
`HistoryBasedPlanStatisticsProvider` SPI.

By default the provider estimates filter and projection nodes only. These carry the selectivities the planner cannot
estimate, while its own estimates of scans and joins stay in place.

Modules:

| Module | Contents |
|---|---|
| `presto-sample-stats` | the provider (recorded history first, then sample estimates), the fragment engine interface, `sample_tables.py`, `smoke_check.py` |
| `presto-sample-stats-duckdb` | the `duckdb` fragment engine: translates a fragment to SQL and runs it in an embedded DuckDB |
| `presto-sample-stats-plugin` | the coordinator plugin; picks the engine named by `engine` from the engines in its directory |

## Usage

1. Build the plugin with the Presto checkout the coordinator runs:

   ```bash
   ./build.sh
   ```

   The plugin directory is `presto-sample-stats-plugin/target/presto-sample-stats-plugin-<version>`.

2. With the cluster running and the tables analyzed, write the samples. The default sample size is 100,000 rows per
   table:

   ```bash
   python3 sample_tables.py -s <schema> -o <samples dir> -m <hive metastore dir>
   ```

   Tables of at most twice the sample size are copied whole. Larger tables keep the rows whose key is a multiple of
   a prime, so each sampled key keeps all of its rows (whole orders of `lineitem`, for example). The key is the
   table's first integer column unless `--key TABLE=COLUMN` names another.

3. Start the coordinator with the plugin and the samples. With velox-testing:

   ```bash
   PRESTO_COORDINATOR_PLUGIN_DIR=<plugin directory> PRESTO_COORDINATOR_PLUGIN_DATA=<samples dir> \
   PRESTO_DISABLED_PLUGINS=redis-hbo-provider PRESTO_HISTORY_BASED_PLANNING=1 \
       presto/scripts/start_native_gpu_presto.sh ...
   ```

   This mounts the plugin and the samples (at `/opt/presto-plugin-data`, where the plugin looks for
   `sample-stats.properties`), leaves out the Redis history provider (Presto accepts one provider), and turns on
   history-based planning. To prefer the statistics of earlier runs over sample estimates, also turn on recording
   (`optimizer.track-history-based-plan-statistics=true`); with recording off the provider answers from the samples
   only.

4. Check that the coordinator plans with the provider:

   ```bash
   ./smoke_check.py --schema <schema>
   ```

   It fails if a filter that table statistics cannot estimate gets no estimate from the provider. The coordinator
   log also warns once per reason when a node the provider should answer gets no estimate.

## Configuration

`sample_tables.py` writes `sample-stats.properties` into the samples directory if it does not exist:

| Property | Default | Meaning |
|---|---|---|
| `sample.dir` | the directory of the properties file | samples and `tables.properties` |
| `engine` | `duckdb` | the fragment engine that evaluates fragments on the samples |
| `node.types` | `FilterNode,ProjectNode` | plan nodes the provider estimates from the samples |
| `history.record` | `true` | keep the statistics Presto records from finished queries and prefer them over sample estimates |
| `history.max-nodes` | `100000` | the most plan nodes whose recorded statistics are kept (in memory) |
| `log.file` | none | one line per estimate, for checking what the provider answered |

## Limitations

- Estimates are only as good as the samples. A predicate that matches no sampled row is estimated at half a sample
  row, scaled to the table (rare rather than empty).
- Nodes over more than one table (joins) and subtrees with constructs the translator does not cover (for example
  window functions) get no estimate, and the planner uses its own.
- The samples describe the data when they were written. Write them again after the tables change.
- With Hive filter pushdown (`hive.pushdown-filter-enabled=true`) filters move into the table scan. Add
  `TableScanNode` to `node.types` on such clusters, or the provider has nothing to estimate.
- Recorded statistics live in coordinator memory and are lost on restart.
