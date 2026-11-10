/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.presto.samplestats;

import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntry;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.JoinNodeStatistics;
import com.facebook.presto.spi.statistics.PartialAggregationStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.TableWriterNodeStatistics;
import org.duckdb.DuckDBDriver;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static com.nvidia.presto.samplestats.ExpressionSql.quote;
import static java.lang.String.format;

/**
 * Answers the planner's statistics requests from per-table samples held in an in-process DuckDB database: a plan
 * node of a configured type that reads one table (a scan with its layout's filters, filters, projections, and
 * aggregations grouped on the table's sample key or with no grouping keys) is translated to SQL over that table's
 * sample, and its row count is scaled by table rows / sample rows. Output bytes use the workers' column widths (fixed
 * width per type, measured average length plus a 4-byte offset for strings), the measure history-based statistics
 * record. Other nodes get no statistics, so the planner derives them as usual. Nothing is recorded.
 *
 * <p>Estimates are cached per plan hash for the life of the coordinator; the samples are loaded once at startup.
 */
public class SampleStatsProvider
        implements HistoryBasedPlanStatisticsProvider
{
    private static final HistoricalPlanStatistics NONE = new HistoricalPlanStatistics(Collections.emptyList());
    // Presto routes java.util.logging to the server log.
    private static final Logger LOG = Logger.getLogger(SampleStatsProvider.class.getName());

    private final Map<String, SampleTable> tables;
    private final String schema;
    private final Connection connection;
    private final Optional<Path> log;
    // Plan node classes (simple names) to answer; the planner estimates the others itself.
    private final Set<String> nodeTypes;
    private final Map<String, Optional<double[]>> cache = new ConcurrentHashMap<>();
    // A node the provider should answer but cannot falls back to the planner's own estimate, which keeps results
    // correct but quietly loses the sample estimate. Each distinct reason is logged once as a warning.
    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    public SampleStatsProvider(Path sampleDir, Path log, Set<String> nodeTypes)
    {
        this.nodeTypes = nodeTypes;
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(sampleDir.resolve("tables.properties"))) {
            properties.load(in);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.schema = properties.getProperty("schema");
        this.tables = SampleTable.parse(properties);
        this.log = Optional.ofNullable(log);
        try {
            this.connection = new DuckDBDriver().connect("jdbc:duckdb:", new Properties());
            try (Statement statement = connection.createStatement()) {
                for (SampleTable table : tables.values()) {
                    statement.execute(format("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')",
                            quote(table.getName()), sampleDir.resolve(table.getName() + ".parquet")));
                }
            }
        }
        catch (SQLException e) {
            throw new IllegalStateException("Cannot load the samples in " + sampleDir, e);
        }
        String loaded = format("loaded %d sample tables of %s from %s; answering %s", tables.size(), schema, sampleDir, nodeTypes);
        LOG.info(loaded);
        write(loaded);
    }

    @Override
    public String getName()
    {
        return "sample_stats";
    }

    @Override
    public Map<PlanNodeWithHash, HistoricalPlanStatistics> getStats(List<PlanNodeWithHash> planNodesWithHash, long timeoutInMilliSeconds)
    {
        Map<PlanNodeWithHash, HistoricalPlanStatistics> result = new HashMap<>();
        for (PlanNodeWithHash node : planNodesWithHash) {
            Optional<String> hash = node.getHash();
            Optional<double[]> estimate = hash.isPresent() ? cache.computeIfAbsent(hash.get(), key -> estimate(node.getPlanNode(), key)) : Optional.empty();
            result.put(node, estimate.isPresent() ? statistics(estimate.get()) : NONE);
        }
        return result;
    }

    // estimate = {rows, bytes, input rows, input bytes}
    private static HistoricalPlanStatistics statistics(double[] estimate)
    {
        PlanStatistics statistics = new PlanStatistics(Estimate.of(estimate[0]), Estimate.of(estimate[1]), 1.0,
                JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty());
        // The planner matches these against the table's current statistics; an unknown size (NaN) matches unknown.
        PlanStatistics input = new PlanStatistics(Estimate.estimateFromDouble(estimate[2]), Estimate.estimateFromDouble(estimate[3]), 1.0,
                JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty());
        return new HistoricalPlanStatistics(Collections.singletonList(
                new HistoricalPlanStatisticsEntry(statistics, Collections.singletonList(input), null)));
    }

    @Override
    public void putStats(Map<PlanNodeWithHash, HistoricalPlanStatistics> hashesAndStatistics)
    {
        // Estimates come from the samples; nothing is recorded.
    }

    // Presto keys a node below a limiting node together with that node (StatsEquivalentPlanNodeWithLimit, an engine
    // class; before the TopN fix also below TopN, e.g. all of Q18). A LIMIT can stop its input early, which matters
    // for recorded history but not for an estimate of the subtree's output, so the wrapped subtree is estimated.
    private static PlanNode unwrapLimit(PlanNode node)
    {
        if (node.getClass().getSimpleName().equals("StatsEquivalentPlanNodeWithLimit")) {
            try {
                return (PlanNode) node.getClass().getMethod("getPlan").invoke(node);
            }
            catch (ReflectiveOperationException e) {
                return node;
            }
        }
        return node;
    }

    private synchronized Optional<double[]> estimate(PlanNode wrapped, String hash)
    {
        PlanNode node = unwrapLimit(wrapped);
        if (!nodeTypes.contains(node.getClass().getSimpleName())) {
            return Optional.empty();
        }
        String id = hash.substring(0, Math.min(16, hash.length())) + " " + node.getClass().getSimpleName();
        Optional<Relation> relation;
        try {
            relation = new PlanTranslator(schema, tables).translate(node);
        }
        catch (RuntimeException e) {
            warnOnce(node.getClass().getSimpleName() + " not translated: " + e.getMessage());
            write(id + " not translated: " + e.getMessage());
            return Optional.empty();
        }
        if (!relation.isPresent()) {
            return Optional.empty();
        }
        Relation r = relation.get();
        List<VariableReferenceExpression> outputs = node.getOutputVariables();
        List<VariableReferenceExpression> strings = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT count(*)");
        for (VariableReferenceExpression output : outputs) {
            if (isString(output.getType()) && !r.isOpaque(output.getName())) {
                strings.add(output);
                sql.append(", avg(strlen(").append(quote(output.getName())).append("))");
            }
        }
        sql.append(" FROM (").append(r.getSql()).append(") t");
        long count;
        Map<String, Double> lengths = new HashMap<>();
        long start = System.nanoTime();
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql.toString())) {
            rows.next();
            count = rows.getLong(1);
            for (int i = 0; i < strings.size(); i++) {
                double length = rows.getDouble(i + 2);
                lengths.put(strings.get(i).getName(), rows.wasNull() ? 0 : length);
            }
        }
        catch (SQLException e) {
            warnOnce(node.getClass().getSimpleName() + " failed on the sample: " + e.getMessage().split("\n")[0]);
            write(id + " failed: " + e.getMessage().split("\n")[0] + " | " + sql);
            return Optional.empty();
        }
        double ms = (System.nanoTime() - start) / 1e6;
        // An empty sample result still means "rare", not "empty": count it as half a sample row.
        double estimatedRows = r.isGlobal() ? 1 : (count == 0 ? 0.5 : count) * r.getTable().getScale();
        double width = 0;
        for (VariableReferenceExpression output : outputs) {
            width += width(output.getType(), lengths.getOrDefault(output.getName(), 0.0));
        }
        write(format("%s %s: %d sample rows -> %.0f rows, %.1f B/row, %.1f ms | %s", id, r.getTable().getName(), count,
                estimatedRows, width, ms, r.getSql()));
        return Optional.of(new double[] {estimatedRows, estimatedRows * width, r.getTable().getInputRows(), r.getTable().getInputSize()});
    }

    private static boolean isString(Type type)
    {
        String base = type.getTypeSignature().getBase();
        return base.equals("varchar") || base.equals("char");
    }

    // Bytes per row of a column as the workers count it (HBO's outputSize).
    private static double width(Type type, double averageLength)
    {
        switch (type.getTypeSignature().getBase()) {
            case "varchar":
            case "char":
                return averageLength + 4;
            case "boolean":
            case "tinyint":
                return 1;
            case "smallint":
                return 2;
            case "integer":
            case "date":
            case "real":
                return 4;
            case "decimal":
                return ((DecimalType) type).isShort() ? 8 : 16;
            default:
                return 8;
        }
    }

    private void warnOnce(String reason)
    {
        if (warned.add(reason)) {
            LOG.warning("No sample estimate, the planner's own estimate is used: " + reason);
        }
    }

    private void write(String line)
    {
        if (log.isPresent()) {
            try {
                Files.write(log.get(), (Instant.now() + " " + line + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            catch (IOException ignored) {
                // Logging is best effort.
            }
        }
    }
}
