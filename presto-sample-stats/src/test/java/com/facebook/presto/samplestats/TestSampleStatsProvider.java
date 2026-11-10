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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntry;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntryInfo;
import com.facebook.presto.spi.statistics.JoinNodeStatistics;
import com.facebook.presto.spi.statistics.PartialAggregationStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.TableWriterNodeStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntryInfo.WorkerType.CPP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * The provider with a stand-in engine: how sample estimates are scaled and how they combine with recorded history.
 * Presto uses the first entry whose input table statistics match the current ones, so the order of entries matters.
 */
public class TestSampleStatsProvider
{
    private static final Set<String> FILTERS = ImmutableSet.of("FilterNode");
    // 600,000 rows sampled to 1,000 (scale 600); input statistics as the planner reads them.
    private static final SampleTable PART = new SampleTable("part", 600.0, "partkey", 600_000, 20_000_000);
    private static final HistoricalPlanStatisticsEntryInfo RUN = new HistoricalPlanStatisticsEntryInfo(CPP, new QueryId("run_1"), "test");

    @Test
    public void testSampleEstimateIsScaled()
    {
        // 100 of 1,000 sample rows match; the name output averages 17 characters.
        SampleStatsProvider provider = provider(new FixedEngine(100), true);
        List<HistoricalPlanStatisticsEntry> entries = stats(provider, filter(), "h1");
        assertEquals(entries.size(), 1);
        PlanStatistics statistics = entries.get(0).getPlanStatistics();
        assertEquals(statistics.getRowCount().getValue(), 60_000.0);
        // partkey 8 bytes + name 17 characters + 4-byte offset.
        assertEquals(statistics.getOutputSize().getValue(), 60_000.0 * (8 + 17 + 4));
        assertEquals(entries.get(0).getInputTableStatistics().get(0).getRowCount().getValue(), 600_000.0);
        assertEquals(entries.get(0).getInputTableStatistics().get(0).getOutputSize().getValue(), 20_000_000.0);
    }

    @Test
    public void testEmptySampleResultMeansRare()
    {
        List<HistoricalPlanStatisticsEntry> entries = stats(provider(new FixedEngine(0), true), filter(), "h1");
        assertEquals(entries.get(0).getPlanStatistics().getRowCount().getValue(), 300.0);
    }

    @Test
    public void testRecordedRunComesBeforeSampleEstimate()
    {
        SampleStatsProvider provider = provider(new FixedEngine(100), true);
        PlanNodeWithHash node = new PlanNodeWithHash(filter(), Optional.of("h1"));
        // Presto merges a finished run into the entries getStats returned: here the run's tables match the estimate's
        // input statistics, so it replaces the estimate.
        provider.putStats(ImmutableMap.of(node, new HistoricalPlanStatistics(ImmutableList.of(run(55_000, 600_000)))));
        List<HistoricalPlanStatisticsEntry> entries = stats(provider, filter(), "h1");
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getPlanStatistics().getRowCount().getValue(), 55_000.0);
        assertEquals(entries.get(0).getHistoricalPlanStatisticsEntryInfo(), RUN);
        assertEquals(entries.get(1).getPlanStatistics().getRowCount().getValue(), 60_000.0);
    }

    @Test
    public void testSampleEstimatesAreNotRecorded()
    {
        SampleStatsProvider provider = provider(new FixedEngine(100), true);
        PlanNodeWithHash node = new PlanNodeWithHash(filter(), Optional.of("h1"));
        HistoricalPlanStatisticsEntry estimate = stats(provider, filter(), "h1").get(0);
        // A run on tables that changed does not replace the estimate; Presto hands both back.
        provider.putStats(ImmutableMap.of(node, new HistoricalPlanStatistics(ImmutableList.of(estimate, run(80_000, 800_000)))));
        assertEquals(provider.getRecordedNodeCount(), 1);
        List<HistoricalPlanStatisticsEntry> entries = stats(provider, filter(), "h1");
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getHistoricalPlanStatisticsEntryInfo(), RUN);
        assertEquals(entries.get(1).getHistoricalPlanStatisticsEntryInfo().getQueryId(), SampleStatsProvider.SAMPLE_ESTIMATE_ID);
        // Only sample estimates: nothing to record.
        provider.putStats(ImmutableMap.of(new PlanNodeWithHash(filter(), Optional.of("h2")), new HistoricalPlanStatistics(ImmutableList.of(estimate))));
        assertEquals(provider.getRecordedNodeCount(), 1);
    }

    @Test
    public void testRecordingCanBeOff()
    {
        SampleStatsProvider provider = provider(new FixedEngine(100), false);
        provider.putStats(ImmutableMap.of(new PlanNodeWithHash(filter(), Optional.of("h1")), new HistoricalPlanStatistics(ImmutableList.of(run(55_000, 600_000)))));
        assertEquals(provider.getRecordedNodeCount(), 0);
        assertEquals(stats(provider, filter(), "h1").size(), 1);
    }

    @Test
    public void testRecordedNodesAreBounded()
    {
        SampleStatsProvider provider = new SampleStatsProvider(new FixedEngine(100), FILTERS, true, 1, Optional.empty());
        provider.putStats(ImmutableMap.of(new PlanNodeWithHash(filter(), Optional.of("h1")), new HistoricalPlanStatistics(ImmutableList.of(run(1, 600_000)))));
        provider.putStats(ImmutableMap.of(new PlanNodeWithHash(filter(), Optional.of("h2")), new HistoricalPlanStatistics(ImmutableList.of(run(2, 600_000)))));
        assertEquals(provider.getRecordedNodeCount(), 1);
        // A node already recorded is still updated.
        provider.putStats(ImmutableMap.of(new PlanNodeWithHash(filter(), Optional.of("h1")), new HistoricalPlanStatistics(ImmutableList.of(run(3, 600_000)))));
        assertEquals(stats(provider, filter(), "h1").get(0).getPlanStatistics().getRowCount().getValue(), 3.0);
    }

    @Test
    public void testRecordedHistoryAnswersNodesWithoutEstimate()
    {
        SampleStatsProvider provider = provider(new FixedEngine(100), true);
        PlanNodeWithHash join = new PlanNodeWithHash(new JoinNode(filter()), Optional.of("j1"));
        provider.putStats(ImmutableMap.of(join, new HistoricalPlanStatistics(ImmutableList.of(run(42, 600_000)))));
        List<HistoricalPlanStatisticsEntry> entries = provider.getStats(ImmutableList.of(join), 1000).get(join).getLastRunsStatistics();
        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0).getPlanStatistics().getRowCount().getValue(), 42.0);
    }

    @Test
    public void testOnlyConfiguredNodeTypesAreEstimated()
    {
        FixedEngine engine = new FixedEngine(100);
        assertTrue(stats(provider(engine, true), new JoinNode(filter()), "j1").isEmpty());
        assertEquals(engine.calls.get(), 0);
    }

    @Test
    public void testEstimateIsCachedPerHash()
    {
        FixedEngine engine = new FixedEngine(100);
        SampleStatsProvider provider = provider(engine, true);
        stats(provider, filter(), "h1");
        stats(provider, filter(), "h1");
        stats(provider, filter(), "h2");
        assertEquals(engine.calls.get(), 2);
    }

    @Test
    public void testEngineFailureLeavesThePlannerEstimate()
    {
        FragmentEngine failing = node -> {
            throw new UnsupportedOperationException("function regexp_like");
        };
        assertTrue(stats(provider(failing, true), filter(), "h1").isEmpty());
    }

    @Test
    public void testNodeKeyedWithItsLimitIsUnwrapped()
    {
        FixedEngine engine = new FixedEngine(100);
        List<HistoricalPlanStatisticsEntry> entries = stats(provider(engine, true), new StatsEquivalentPlanNodeWithLimit(filter()), "h1");
        assertEquals(entries.size(), 1);
        assertTrue(engine.lastNode instanceof FilterNode);
    }

    @Test
    public void testNodesWithoutHashGetNothing()
    {
        SampleStatsProvider provider = provider(new FixedEngine(100), true);
        PlanNodeWithHash node = new PlanNodeWithHash(filter(), Optional.empty());
        assertTrue(provider.getStats(ImmutableList.of(node), 1000).get(node).getLastRunsStatistics().isEmpty());
        assertFalse(provider.getStats(ImmutableList.of(node), 1000).isEmpty());
    }

    private static SampleStatsProvider provider(FragmentEngine engine, boolean record)
    {
        return new SampleStatsProvider(engine, FILTERS, record, 1000, Optional.empty());
    }

    private static List<HistoricalPlanStatisticsEntry> stats(SampleStatsProvider provider, PlanNode node, String hash)
    {
        PlanNodeWithHash key = new PlanNodeWithHash(node, Optional.of(hash));
        return provider.getStats(ImmutableList.of(key), 1000).get(key).getLastRunsStatistics();
    }

    private static HistoricalPlanStatisticsEntry run(double rows, double inputRows)
    {
        return new HistoricalPlanStatisticsEntry(
                new PlanStatistics(Estimate.of(rows), Estimate.of(rows * 29), 1.0, JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty()),
                ImmutableList.of(new PlanStatistics(Estimate.of(inputRows), Estimate.of(20_000_000), 1.0, JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty())),
                RUN);
    }

    private static FilterNode filter()
    {
        return new FilterNode(ImmutableList.of(variable("partkey", BIGINT), variable("name", VARCHAR)));
    }

    private static VariableReferenceExpression variable(String name, Type type)
    {
        return new VariableReferenceExpression(Optional.empty(), name, type);
    }

    /**
     * Answers every node with the same sample row count over PART; the name output averages 17 characters.
     */
    private static class FixedEngine
            implements FragmentEngine
    {
        private final long sampleRows;
        private final AtomicInteger calls = new AtomicInteger();
        private PlanNode lastNode;

        FixedEngine(long sampleRows)
        {
            this.sampleRows = sampleRows;
        }

        @Override
        public Optional<FragmentResult> evaluate(PlanNode node)
        {
            calls.incrementAndGet();
            lastNode = node;
            return Optional.of(new FragmentResult(PART, sampleRows, false, ImmutableMap.of("name", 17.0), "fixed"));
        }
    }

    /**
     * Stand-in plan nodes; the provider selects nodes by simple class name.
     */
    private static class FilterNode
            extends LeafNode
    {
        FilterNode(List<VariableReferenceExpression> outputs)
        {
            super("filter", outputs);
        }
    }

    private static class JoinNode
            extends LeafNode
    {
        JoinNode(PlanNode source)
        {
            super("join", source.getOutputVariables());
        }
    }

    /**
     * Stand-in for Presto's StatsEquivalentPlanNodeWithLimit (an engine class), matched by simple name.
     */
    public static class StatsEquivalentPlanNodeWithLimit
            extends LeafNode
    {
        private final PlanNode plan;

        StatsEquivalentPlanNodeWithLimit(PlanNode plan)
        {
            super("limit", plan.getOutputVariables());
            this.plan = plan;
        }

        public PlanNode getPlan()
        {
            return plan;
        }
    }

    private abstract static class LeafNode
            extends PlanNode
    {
        private final List<VariableReferenceExpression> outputs;

        LeafNode(String id, List<VariableReferenceExpression> outputs)
        {
            super(Optional.empty(), new PlanNodeId(id), Optional.empty());
            this.outputs = ImmutableList.copyOf(outputs);
        }

        @Override
        public List<PlanNode> getSources()
        {
            return ImmutableList.of();
        }

        @Override
        public List<VariableReferenceExpression> getOutputVariables()
        {
            return outputs;
        }

        @Override
        public PlanNode replaceChildren(List<PlanNode> newChildren)
        {
            return this;
        }

        @Override
        public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
        {
            return this;
        }
    }
}
