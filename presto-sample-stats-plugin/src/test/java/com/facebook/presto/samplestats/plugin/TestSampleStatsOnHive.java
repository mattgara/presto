/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.samplestats.plugin;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveQueryRunner;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY;
import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * The plugin end to end on a Hive query runner with the TPC-H tiny tables. The samples are the whole tables (scale
 * 1), so every sample estimate must equal the row count the query actually returns, except for customer, whose
 * sample is declared as half the table (scale 2): its estimates are twice the true count until a finished query's
 * recorded statistics replace them.
 */
@Test(singleThreaded = true)
public class TestSampleStatsOnHive
        extends AbstractTestQueryFramework
{
    private static final String HALF_SAMPLED = "customer";
    private static final Pattern HISTORY_ESTIMATE = Pattern.compile("\\{source: HistoryBasedSourceInfo, rows: ([0-9,.]+)");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(TpchTable.getTables());
        Path samples = Files.createTempDirectory("sample-stats-hive");
        writeSamples(queryRunner, samples);
        Path config = samples.resolve("sample-stats.properties");
        Files.write(config, ImmutableList.of("sample.dir=" + samples, "engine=duckdb", "log.file=" + samples.resolve("estimates.log")), UTF_8);
        queryRunner.installPlugin(new SampleStatsPlugin(Optional.of(config)));
        return queryRunner;
    }

    @Test
    public void testSampleEstimatesMatchTrueCounts()
    {
        List<String> queries = ImmutableList.of(
                "SELECT partkey, name FROM part WHERE name LIKE '%green%'",
                "SELECT orderkey FROM lineitem WHERE shipdate BETWEEN DATE '1994-01-01' AND DATE '1994-12-31' AND discount BETWEEN 0.05 AND 0.07 AND quantity < 24",
                "SELECT orderkey, totalprice FROM orders WHERE orderpriority IN ('1-URGENT', '2-HIGH') AND orderstatus <> 'F'",
                "SELECT suppkey FROM supplier WHERE substr(phone, 1, 2) IN ('13', '31', '23')",
                "SELECT orderkey FROM lineitem WHERE commitdate < receiptdate AND shipmode IN ('MAIL', 'SHIP')",
                "SELECT extendedprice * (1 - discount) AS revenue FROM lineitem WHERE returnflag = 'R'",
                // Q18's HAVING: a filter over an aggregation grouped on the sample key.
                "SELECT orderkey FROM lineitem GROUP BY orderkey HAVING sum(quantity) > 250");
        Session session = session(false, false);
        for (String query : queries) {
            long count = count(query);
            assertTrue(count > 0, query);
            assertEquals(topSampleEstimate(session, query), Optional.of((double) count), query);
        }
    }

    @Test
    public void testPushedDownFiltersNeedScanEstimates()
    {
        // With Hive filter pushdown the filter moves into the table scan, which the default node types leave to the
        // planner (add TableScanNode to node.types for such clusters).
        assertEquals(topSampleEstimate(session(true, false), "SELECT partkey, name FROM part WHERE name LIKE '%green%'"), Optional.empty());
    }

    @Test
    public void testRecordedStatisticsReplaceSampleEstimates()
            throws InterruptedException
    {
        String query = "SELECT custkey, name FROM " + HALF_SAMPLED + " WHERE mktsegment = 'BUILDING' AND acctbal > 1000";
        Session session = session(false, true);
        double count = count(query);
        assertEquals(topSampleEstimate(session, query), Optional.of(2 * count));
        getQueryRunner().execute(session, query);
        // Presto records a finished query's statistics asynchronously.
        Optional<Double> estimate = Optional.empty();
        for (int attempt = 0; attempt < 100 && !estimate.equals(Optional.of(count)); attempt++) {
            Thread.sleep(100);
            estimate = topSampleEstimate(session, query);
        }
        assertEquals(estimate, Optional.of(count));
    }

    @Test
    public void testTpchResultsMatchPlansWithoutSampleStatistics()
            throws IOException
    {
        Session withSamples = session(false, false);
        Session withoutSamples = Session.builder(withSamples).setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "false").build();
        int estimated = 0;
        for (int q = 1; q <= 22; q++) {
            String query = tpchQuery(q);
            MaterializedResult expected = getQueryRunner().execute(withoutSamples, query);
            MaterializedResult actual = getQueryRunner().execute(withSamples, query);
            assertEquals(normalize(actual), normalize(expected), "Q" + q);
            estimated += sampleEstimates(withSamples, query).size();
        }
        assertTrue(estimated > 0);
        // Q9's LIKE filter is the estimate the planner most needs.
        assertFalse(sampleEstimates(withSamples, tpchQuery(9)).isEmpty());
    }

    private Session session(boolean pushdown, boolean track)
    {
        return Session.builder(getSession())
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(TRACK_HISTORY_BASED_PLAN_STATISTICS, String.valueOf(track))
                .setSystemProperty(RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY, "false")
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, String.valueOf(pushdown))
                .build();
    }

    private long count(String query)
    {
        return (long) getQueryRunner().execute(getSession(), "SELECT count(*) FROM (" + query + ")").getOnlyValue();
    }

    // The history-based estimate nearest the plan's root, as EXPLAIN shows it.
    private Optional<Double> topSampleEstimate(Session session, String query)
    {
        List<Double> estimates = sampleEstimates(session, query);
        return estimates.isEmpty() ? Optional.empty() : Optional.of(estimates.get(0));
    }

    // Row counts of the history-based estimates in the plan, from the root down.
    private List<Double> sampleEstimates(Session session, String query)
    {
        String plan = (String) getQueryRunner().execute(session, "EXPLAIN " + query).getOnlyValue();
        Matcher matcher = HISTORY_ESTIMATE.matcher(plan);
        ImmutableList.Builder<Double> estimates = ImmutableList.builder();
        while (matcher.find()) {
            estimates.add(Double.parseDouble(matcher.group(1).replace(",", "")));
        }
        return estimates.build();
    }

    private static String tpchQuery(int q)
            throws IOException
    {
        String sql = Resources.toString(Resources.getResource("tpch/queries/q" + q + ".sql"), UTF_8);
        return sql.replaceFirst("(?m);\\s*$", "").replaceAll("\\$\\{([^}]+)}", "$1");
    }

    // Rows in order, with doubles to 10 significant digits: plans may sum in a different order.
    private static List<List<Object>> normalize(MaterializedResult result)
    {
        List<List<Object>> rows = new ArrayList<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            List<Object> values = new ArrayList<>();
            for (Object value : row.getFields()) {
                values.add(value instanceof Double ? new BigDecimal((Double) value).round(new MathContext(10)) : value);
            }
            rows.add(values);
        }
        return rows;
    }

    /**
     * Writes each table whole as its sample, and tables.properties with the planner's input statistics.
     */
    private static void writeSamples(DistributedQueryRunner queryRunner, Path samples)
            throws IOException, SQLException
    {
        List<String> properties = new ArrayList<>();
        properties.add("schema=tpch");
        Session session = queryRunner.getDefaultSession();
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            for (TpchTable<?> table : TpchTable.getTables()) {
                String name = table.getTableName();
                List<String> columns = queryRunner.execute(session, "SHOW COLUMNS FROM " + name).getMaterializedRows().stream()
                        .map(row -> (String) row.getField(0))
                        .collect(toImmutableList());
                MaterializedResult rows = queryRunner.execute(session, "SELECT * FROM " + name);
                List<Type> types = rows.getTypes();
                List<String> definitions = new ArrayList<>();
                for (int i = 0; i < columns.size(); i++) {
                    definitions.add(format("\"%s\" %s", columns.get(i), duckdbType(types.get(i))));
                }
                try (Statement statement = connection.createStatement()) {
                    statement.execute(format("CREATE TABLE \"%s\" (%s)", name, String.join(", ", definitions)));
                }
                String placeholders = columns.stream().map(column -> "?").collect(Collectors.joining(", "));
                try (PreparedStatement insert = connection.prepareStatement(format("INSERT INTO \"%s\" VALUES (%s)", name, placeholders))) {
                    for (MaterializedRow row : rows.getMaterializedRows()) {
                        for (int i = 0; i < columns.size(); i++) {
                            Object value = row.getField(i);
                            insert.setObject(i + 1, value instanceof LocalDate ? java.sql.Date.valueOf((LocalDate) value) : value);
                        }
                        insert.addBatch();
                    }
                    insert.executeBatch();
                }
                try (Statement statement = connection.createStatement()) {
                    statement.execute(format("COPY \"%s\" TO '%s' (FORMAT PARQUET)", name, samples.resolve(name + ".parquet")));
                }
                TableStatistics statistics = tableStatistics(queryRunner, name);
                properties.add(format("%s.scale=%s", name, name.equals(HALF_SAMPLED) ? "2.0" : "1.0"));
                properties.add(format("%s.key=%s", name, columns.get(0)));
                properties.add(format("%s.input_rows=%s", name, statistics.getRowCount().getValue()));
                properties.add(format("%s.input_size=%s", name, statistics.getTotalSize().getValue()));
            }
        }
        Files.write(samples.resolve("tables.properties"), properties, UTF_8);
    }

    // The statistics the planner compares a statistics entry's input tables against.
    private static TableStatistics tableStatistics(DistributedQueryRunner queryRunner, String table)
    {
        Metadata metadata = queryRunner.getMetadata();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .readOnly()
                .execute(queryRunner.getDefaultSession(), session -> {
                    TableHandle handle = metadata.getMetadataResolver(session).getTableHandle(new QualifiedObjectName(HIVE_CATALOG, "tpch", table)).get();
                    List<ColumnHandle> columns = ImmutableList.copyOf(metadata.getColumnHandles(session, handle).values());
                    return metadata.getTableStatistics(session, handle, columns, Constraint.alwaysTrue());
                });
    }

    private static String duckdbType(Type type)
    {
        String base = type.getTypeSignature().getBase();
        switch (base) {
            case "bigint":
            case "integer":
            case "double":
            case "date":
                return base.toUpperCase();
            case "varchar":
                return "VARCHAR";
            case "decimal":
                DecimalType decimal = (DecimalType) type;
                return format("DECIMAL(%d, %d)", decimal.getPrecision(), decimal.getScale());
            default:
                throw new IllegalArgumentException("type " + type);
        }
    }
}
