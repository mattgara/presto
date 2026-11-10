# Sample statistics provider

Presto's cost-based planner chooses join orders and join distribution from table statistics. For filters, joins and
aggregations it can only guess selectivities from those statistics (for example a `LIKE '%green%'` filter or a
`HAVING sum(...) > 300`), and wrong guesses give slow or memory-hungry plans on large scale factors. History-based
optimization (HBO) fixes the guesses with the row counts of earlier runs of the same query. That needs a warm-up
run of every query and a statistics store (Redis), and a benchmark that relies on it measures a planner that has
seen the queries before.

This provider gives the planner the same kind of estimates without history. At planning time Presto asks the
history-based statistics provider for the output of each plan node. For a node over one table (a scan with its
pushed-down filters, then filters, projections and aggregations), the provider translates the subtree into SQL over
a small sample of the table, runs it in an embedded DuckDB and scales the row count to the full table. The samples
are written once, after `ANALYZE`, by `sample_tables.py`. No Presto changes are needed: the provider is a plugin
that implements Presto's `HistoryBasedPlanStatisticsProvider` SPI.

By default the provider answers filter and projection nodes only. These carry the selectivities the planner cannot
estimate, while its own estimates of scans and joins stay in place.

## Usage

1. Build the plugin against the Presto version of the sibling `../presto` checkout:

   ```bash
   presto/sample_stats/build_provider.sh
   ```

   The plugin directory is `presto/sample_stats/provider/target/plugin`.

2. With the cluster running and the tables analyzed (`setup_benchmark_tables.sh`), write the samples. The default
   sample size is 100,000 rows per table:

   ```bash
   python3 presto/sample_stats/sample_tables.py -s <schema> -o <samples dir> \
       -m presto/docker/.hive_metastore
   ```

   Tables of at most twice the sample size are copied whole. Larger tables keep the rows whose key is a multiple of
   a prime, so each sampled key keeps all of its rows (whole orders of `lineitem`, for example). The key is the
   table's first integer column unless `--key TABLE=COLUMN` names another.

3. Start Presto with `SAMPLE_STATS_DIR` set to the samples directory:

   ```bash
   SAMPLE_STATS_DIR=<samples dir> presto/scripts/start_native_gpu_presto.sh ...
   ```

   This turns on history-based planning in the coordinator config (with recording off), mounts the plugin and the
   samples into the coordinator, and leaves out the Redis history provider (Presto accepts one provider).
   `SAMPLE_STATS_PLUGIN_DIR` overrides the plugin directory.

## Configuration

`sample_tables.py` writes `sample-stats.properties` into the samples directory if it does not exist:

| Property | Default | Meaning |
|---|---|---|
| `sample.dir` | the directory of the properties file | samples and `tables.properties` |
| `node.types` | `FilterNode,ProjectNode` | plan nodes the provider answers |
| `log.file` | none | one line per estimate, for checking what the provider answered |

## Limitations

- Estimates are only as good as the samples. A predicate that matches no sampled row is estimated at half a sample
  row, scaled to the table (rare rather than empty).
- Nodes over more than one table (joins) and subtrees with constructs the translator does not cover (for example
  window functions) get no estimate, and the planner uses its own.
- The samples describe the data when they were written. Write them again after the tables change.
