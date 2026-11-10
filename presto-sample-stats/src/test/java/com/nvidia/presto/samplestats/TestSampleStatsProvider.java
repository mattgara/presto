/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.presto.samplestats;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntry;
import com.facebook.presto.spi.statistics.PlanStatistics;
import org.duckdb.DuckDBDriver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.nvidia.presto.samplestats.Plans.call;
import static com.nvidia.presto.samplestats.Plans.constant;
import static com.nvidia.presto.samplestats.Plans.scan;
import static com.nvidia.presto.samplestats.Plans.variable;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * End to end over small Parquet samples: part (1,000 sampled rows of 600,000; every 10th name contains "green") and
 * lineitem (800 sampled rows of 8,000,000: 200 whole orders of 4 lines; orders whose key is a multiple of 50 have
 * quantity 90 per line, so 4 orders have sum(l_quantity) > 300).
 */
public class TestSampleStatsProvider
{
    private static final Set<String> DEFAULT_TYPES = new HashSet<>(Arrays.asList(SampleStatsPlugin.DEFAULT_NODE_TYPES.split(",")));
    private static final DecimalType QUANTITY = createDecimalType(15, 2);
    private static final DecimalType SUM = createDecimalType(38, 2);

    private Path samples;

    @BeforeClass
    public void writeSamples()
            throws IOException, SQLException
    {
        samples = Files.createTempDirectory("sample-stats-test");
        try (Connection connection = new DuckDBDriver().connect("jdbc:duckdb:", new Properties());
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("COPY (SELECT i::BIGINT AS p_partkey, " +
                    "CASE WHEN i %% 10 = 0 THEN 'forest green lace' ELSE 'misty rose' END AS p_name " +
                    "FROM range(1000) r(i)) TO '%s' (FORMAT PARQUET)", samples.resolve("part.parquet")));
            statement.execute(String.format("COPY (SELECT o::BIGINT AS l_orderkey, " +
                    "(CASE WHEN o %% 50 = 0 THEN 90 ELSE 10 END)::DECIMAL(15, 2) AS l_quantity " +
                    "FROM range(1, 201) a(o), range(4) b(line)) TO '%s' (FORMAT PARQUET)", samples.resolve("lineitem.parquet")));
        }
        Files.write(samples.resolve("tables.properties"), String.join("\n",
                "schema=tpch",
                "part.rows=600000", "part.sample_rows=1000", "part.scale=600.0", "part.key=p_partkey",
                "part.input_rows=600000.0", "part.input_size=0.0",
                "lineitem.rows=8000000", "lineitem.sample_rows=800", "lineitem.scale=10000.0", "lineitem.key=l_orderkey",
                "lineitem.input_rows=8000000.0", "lineitem.input_size=NaN").getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testFilterRowsBytesAndInputStatistics()
    {
        PlanStatistics statistics = only(estimate(provider(DEFAULT_TYPES), greenParts(TupleDomain.all()), "green"));
        assertEquals(statistics.getRowCount().getValue(), 60_000.0);
        // p_partkey 8 bytes + p_name ("forest green lace", 17 characters) + 4-byte offset.
        assertEquals(statistics.getOutputSize().getValue(), 60_000.0 * (8 + 17 + 4));
        HistoricalPlanStatisticsEntry entry = estimate(provider(DEFAULT_TYPES), greenParts(TupleDomain.all()), "green")
                .getLastRunsStatistics().get(0);
        assertEquals(entry.getInputTableStatistics().get(0).getRowCount().getValue(), 600_000.0);
    }

    @Test
    public void testLayoutDomainIsApplied()
    {
        Domain below500 = Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 500L)), false);
        TupleDomain<Subfield> domain = TupleDomain.withColumnDomains(Collections.singletonMap(new Subfield("p_partkey"), below500));
        assertEquals(only(estimate(provider(DEFAULT_TYPES), greenParts(domain), "green-below-500")).getRowCount().getValue(), 30_000.0);
    }

    @Test
    public void testHavingOnSampleKeyScalesWholeGroups()
    {
        // Q18's shape: Project(l_orderkey) <- Filter(sum > 300) <- Aggregation(sum(l_quantity) GROUP BY l_orderkey).
        TableScanNode lineitem = lineitem();
        VariableReferenceExpression orderKey = lineitem.getOutputVariables().get(0);
        VariableReferenceExpression sum = variable("sum", SUM);
        AggregationNode aggregation = aggregation(lineitem, Collections.singletonList(orderKey), sum);
        FilterNode having = new FilterNode(Optional.empty(), new PlanNodeId("having"), aggregation,
                call("GREATER_THAN", BOOLEAN, sum, constant(Decimals.encodeUnscaledValue(BigInteger.valueOf(30000)), SUM)));
        ProjectNode keys = new ProjectNode(Optional.empty(), new PlanNodeId("keys"), having,
                Assignments.builder().put(orderKey, orderKey).build(), ProjectNode.Locality.LOCAL);
        HistoricalPlanStatistics statistics = estimate(provider(DEFAULT_TYPES), keys, "having");
        assertEquals(only(statistics).getRowCount().getValue(), 40_000.0);
        // lineitem's size is unknown (no metastore totalSize): reported as unknown, which the planner matches with unknown.
        assertTrue(statistics.getLastRunsStatistics().get(0).getInputTableStatistics().get(0).getOutputSize().isUnknown());
    }

    @Test
    public void testOnlyGroupsOnTheSampleKeyAreEstimated()
    {
        SampleStatsProvider provider = provider(new HashSet<>(Arrays.asList("AggregationNode", "FilterNode", "ProjectNode")));
        TableScanNode lineitem = lineitem();
        VariableReferenceExpression orderKey = lineitem.getOutputVariables().get(0);
        VariableReferenceExpression quantity = lineitem.getOutputVariables().get(1);
        assertEquals(only(estimate(provider, aggregation(lineitem, Collections.singletonList(orderKey), variable("sum", SUM)), "by-key"))
                .getRowCount().getValue(), 2_000_000.0);
        // Groups on another column do not scale like rows: no estimate, the planner uses its own.
        assertTrue(estimate(provider, aggregation(lineitem, Collections.singletonList(quantity), variable("sum", SUM)), "by-quantity")
                .getLastRunsStatistics().isEmpty());
    }

    @Test
    public void testOnlyConfiguredNodeTypesAreAnswered()
    {
        assertTrue(estimate(provider(DEFAULT_TYPES), parts(TupleDomain.all()), "scan").getLastRunsStatistics().isEmpty());
        assertEquals(only(estimate(provider(Collections.singleton("TableScanNode")), parts(TupleDomain.all()), "scan"))
                .getRowCount().getValue(), 600_000.0);
    }

    @Test
    public void testNodeKeyedWithItsLimitIsUnwrapped()
    {
        PlanNode wrapped = new Plans.StatsEquivalentPlanNodeWithLimit(greenParts(TupleDomain.all()));
        assertEquals(only(estimate(provider(DEFAULT_TYPES), wrapped, "wrapped")).getRowCount().getValue(), 60_000.0);
    }

    @Test
    public void testUntranslatableNodeGetsNoEstimate()
    {
        FilterNode regex = new FilterNode(Optional.empty(), new PlanNodeId("regex"), parts(TupleDomain.all()),
                call("regexp_like", BOOLEAN, parts(TupleDomain.all()).getOutputVariables().get(1), constant(utf8Slice("g.*"), VARCHAR)));
        assertTrue(estimate(provider(DEFAULT_TYPES), regex, "regex").getLastRunsStatistics().isEmpty());
    }

    private SampleStatsProvider provider(Set<String> nodeTypes)
    {
        return new SampleStatsProvider(samples, null, nodeTypes);
    }

    private static HistoricalPlanStatistics estimate(SampleStatsProvider provider, PlanNode node, String hash)
    {
        PlanNodeWithHash key = new PlanNodeWithHash(node, Optional.of(hash));
        return provider.getStats(Collections.singletonList(key), 1000).get(key);
    }

    private static PlanStatistics only(HistoricalPlanStatistics statistics)
    {
        assertEquals(statistics.getLastRunsStatistics().size(), 1);
        return statistics.getLastRunsStatistics().get(0).getPlanStatistics();
    }

    private static TableScanNode parts(TupleDomain<Subfield> domain)
    {
        Map<String, Type> columns = new LinkedHashMap<>();
        columns.put("p_partkey", BIGINT);
        columns.put("p_name", VARCHAR);
        return scan("tpch", "part", domain, columns);
    }

    private static FilterNode greenParts(TupleDomain<Subfield> domain)
    {
        TableScanNode part = parts(domain);
        return new FilterNode(Optional.empty(), new PlanNodeId("green"), part,
                call("LIKE", BOOLEAN, part.getOutputVariables().get(1), constant(utf8Slice("%green%"), VARCHAR)));
    }

    private static TableScanNode lineitem()
    {
        Map<String, Type> columns = new LinkedHashMap<>();
        columns.put("l_orderkey", BIGINT);
        columns.put("l_quantity", QUANTITY);
        return scan("tpch", "lineitem", TupleDomain.all(), columns);
    }

    private static AggregationNode aggregation(TableScanNode lineitem, List<VariableReferenceExpression> keys, VariableReferenceExpression sum)
    {
        VariableReferenceExpression quantity = lineitem.getOutputVariables().get(1);
        return new AggregationNode(
                Optional.empty(),
                new PlanNodeId("aggregation"),
                lineitem,
                Collections.singletonMap(sum, new AggregationNode.Aggregation(
                        call("sum", SUM, quantity), Optional.empty(), Optional.empty(), false, Optional.empty())),
                singleGroupingSet(keys),
                Collections.emptyList(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
