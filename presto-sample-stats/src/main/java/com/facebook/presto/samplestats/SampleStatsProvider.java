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
package com.facebook.presto.samplestats;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntry;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntryInfo;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.JoinNodeStatistics;
import com.facebook.presto.spi.statistics.PartialAggregationStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.TableWriterNodeStatistics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntryInfo.WorkerType.JAVA;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Answers the planner's history-based statistics requests from two sources. Statistics that Presto recorded for a
 * plan node in earlier runs (when history tracking is on) come first. After them comes an estimate from the table
 * samples, for a node of a configured type that reads one sampled table: a {@link FragmentEngine} runs the node's
 * subtree on that table's sample, and its row count is scaled by table rows / sample rows. Presto uses the first
 * entry whose input table statistics match the tables' current ones, so a recorded run wins while its tables are
 * unchanged, and the sample estimate covers queries seen for the first time and tables that changed.
 *
 * <p>Output bytes of an estimate use the workers' column widths (a fixed width per type; for strings the measured
 * average length plus a 4-byte offset), the measure recorded history uses. Other nodes get no estimate, so the
 * planner derives them as usual. Estimates are cached per plan hash for the life of the coordinator, and recorded
 * statistics are kept in memory.
 */
public class SampleStatsProvider
        implements HistoryBasedPlanStatisticsProvider
{
    // Marks sample estimates among a node's entries, so that putStats does not store them as recorded history.
    @VisibleForTesting
    static final QueryId SAMPLE_ESTIMATE_ID = new QueryId("sample_estimate");
    private static final HistoricalPlanStatisticsEntryInfo SAMPLE_ESTIMATE = new HistoricalPlanStatisticsEntryInfo(JAVA, SAMPLE_ESTIMATE_ID, "sample_stats");
    private static final Logger log = Logger.get(SampleStatsProvider.class);

    private final FragmentEngine engine;
    // Plan node classes (simple names) to estimate; the planner estimates the others itself.
    private final Set<String> nodeTypes;
    private final boolean recordHistory;
    private final int maxRecordedNodes;
    private final Optional<Path> estimateLog;
    private final Map<String, Optional<HistoricalPlanStatisticsEntry>> estimates = new ConcurrentHashMap<>();
    private final Map<String, HistoricalPlanStatistics> recorded = new ConcurrentHashMap<>();
    // A node the provider should estimate but cannot falls back to the planner's own estimate, which keeps results
    // correct but quietly loses the sample estimate. Each distinct reason is logged once as a warning.
    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    public SampleStatsProvider(FragmentEngine engine, Set<String> nodeTypes, boolean recordHistory, int maxRecordedNodes, Optional<Path> estimateLog)
    {
        this.engine = requireNonNull(engine, "engine is null");
        this.nodeTypes = ImmutableSet.copyOf(requireNonNull(nodeTypes, "nodeTypes is null"));
        this.recordHistory = recordHistory;
        this.maxRecordedNodes = maxRecordedNodes;
        this.estimateLog = requireNonNull(estimateLog, "estimateLog is null");
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
            if (!node.getHash().isPresent()) {
                result.put(node, HistoricalPlanStatistics.empty());
                continue;
            }
            String hash = node.getHash().get();
            ImmutableList.Builder<HistoricalPlanStatisticsEntry> entries = ImmutableList.builder();
            entries.addAll(recorded.getOrDefault(hash, HistoricalPlanStatistics.empty()).getLastRunsStatistics());
            estimates.computeIfAbsent(hash, key -> estimate(node.getPlanNode(), key)).ifPresent(entries::add);
            result.put(node, new HistoricalPlanStatistics(entries.build()));
        }
        return result;
    }

    @Override
    public void putStats(Map<PlanNodeWithHash, HistoricalPlanStatistics> hashesAndStatistics)
    {
        if (!recordHistory) {
            return;
        }
        hashesAndStatistics.forEach((node, statistics) -> {
            if (!node.getHash().isPresent()) {
                return;
            }
            // Presto merges a finished run into the entries getStats returned, which include the sample estimate.
            List<HistoricalPlanStatisticsEntry> runs = statistics.getLastRunsStatistics().stream()
                    .filter(entry -> !isSampleEstimate(entry))
                    .collect(toImmutableList());
            String hash = node.getHash().get();
            if (runs.isEmpty()) {
                return;
            }
            if (recorded.size() >= maxRecordedNodes && !recorded.containsKey(hash)) {
                warnOnce(format("recorded history holds %d plan nodes, the maximum; later queries are not recorded", maxRecordedNodes));
                return;
            }
            recorded.put(hash, new HistoricalPlanStatistics(runs));
        });
    }

    @VisibleForTesting
    int getRecordedNodeCount()
    {
        return recorded.size();
    }

    private static boolean isSampleEstimate(HistoricalPlanStatisticsEntry entry)
    {
        HistoricalPlanStatisticsEntryInfo info = entry.getHistoricalPlanStatisticsEntryInfo();
        return info != null && SAMPLE_ESTIMATE_ID.equals(info.getQueryId());
    }

    // Presto keys a node below a limiting node together with that node (StatsEquivalentPlanNodeWithLimit, an engine
    // class not visible to plugins). A LIMIT can stop its input early, which matters for recorded history but not for
    // an estimate of the subtree's output, so the wrapped subtree is estimated.
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

    // The engine need not be thread safe, so estimates are made one at a time.
    private synchronized Optional<HistoricalPlanStatisticsEntry> estimate(PlanNode wrapped, String hash)
    {
        PlanNode node = unwrapLimit(wrapped);
        String type = node.getClass().getSimpleName();
        if (!nodeTypes.contains(type)) {
            return Optional.empty();
        }
        String id = hash.substring(0, Math.min(16, hash.length())) + " " + type;
        long start = System.nanoTime();
        Optional<FragmentResult> evaluated;
        try {
            evaluated = engine.evaluate(node);
        }
        catch (RuntimeException e) {
            String reason = String.valueOf(e.getMessage()).split("\n")[0];
            warnOnce(type + ": " + reason);
            write(id + " no estimate: " + reason);
            return Optional.empty();
        }
        if (!evaluated.isPresent()) {
            return Optional.empty();
        }
        FragmentResult result = evaluated.get();
        double milliseconds = (System.nanoTime() - start) / 1e6;
        SampleTable table = result.getTable();
        // An empty sample result still means "rare", not "empty": count it as half a sample row.
        double rows = result.isGlobal() ? 1 : Math.max(result.getSampleRows(), 0.5) * table.getScale();
        double width = 0;
        for (VariableReferenceExpression output : node.getOutputVariables()) {
            width += width(output.getType(), result.getAverageLengths().getOrDefault(output.getName(), 0.0));
        }
        write(format("%s %s: %d sample rows -> %.0f rows, %.1f B/row, %.1f ms | %s", id, table.getName(), result.getSampleRows(),
                rows, width, milliseconds, result.getDescription()));
        PlanStatistics statistics = new PlanStatistics(Estimate.of(rows), Estimate.of(rows * width), 1.0,
                JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty());
        // The planner matches these against the table's current statistics; an unknown size (NaN) matches unknown.
        PlanStatistics input = new PlanStatistics(Estimate.estimateFromDouble(table.getInputRows()), Estimate.estimateFromDouble(table.getInputSize()), 1.0,
                JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty());
        return Optional.of(new HistoricalPlanStatisticsEntry(statistics, ImmutableList.of(input), SAMPLE_ESTIMATE));
    }

    // Bytes per row of a column as the workers count it (the output size history records).
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
            log.warn("No sample estimate, the planner's own estimate is used: %s", reason);
        }
    }

    private void write(String line)
    {
        if (estimateLog.isPresent()) {
            try {
                Files.write(estimateLog.get(), (Instant.now() + " " + line + "\n").getBytes(UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            catch (IOException ignored) {
                // Logging is best effort.
            }
        }
    }
}
