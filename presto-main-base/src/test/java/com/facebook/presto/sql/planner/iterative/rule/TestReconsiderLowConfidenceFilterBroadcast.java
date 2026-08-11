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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.DEFAULT_FILTER_FACTOR_ENABLED;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static com.facebook.presto.SystemSessionProperties.LOW_CONFIDENCE_FILTER_BROADCAST_SCALE_FACTOR;
import static com.facebook.presto.SystemSessionProperties.RECONSIDER_LOW_CONFIDENCE_FILTER_BROADCAST;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.HIGH;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.LOW;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

@Test(singleThreaded = true)
public class TestReconsiderLowConfidenceFilterBroadcast
{
    private static final int TASK_COUNT = 4;
    private static final double FILTERED_BUILD_ROWS = 900;
    private static final double BUILD_SOURCE_ROWS = 1_000;
    private static final String PROBE_ID = "probe";
    private static final String FILTER_ID = "build_filter";
    private static final String BUILD_SOURCE_ID = "build_source";
    private static final String BUILD_JOIN_ID = "build_join";
    private static final String BUILD_JOIN_LEFT_ID = "build_join_left";

    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(ImmutableList.of(), ImmutableMap.of(), Optional.of(TASK_COUNT));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @Test
    public void testReconsidersDistributionWithoutChangingJoinOrientation()
    {
        assertRule(
                stats("probe_key", 1_000_000, 1_000, HIGH),
                stats("build_key", FILTERED_BUILD_ROWS, 100_000, LOW),
                stats("build_key", BUILD_SOURCE_ROWS, 100_000, HIGH))
                .setSystemProperty(RECONSIDER_LOW_CONFIDENCE_FILTER_BROADCAST, "true")
                .setSystemProperty(LOW_CONFIDENCE_FILTER_BROADCAST_SCALE_FACTOR, "0.05")
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("probe_key", "build_key")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("probe_key", 0)),
                        filter("true", values(ImmutableMap.of("build_key", 0)))));
    }

    @Test
    public void testFindsUnknownSelectivityFilterInsideBuildJoin()
    {
        tester.assertThat(new ReconsiderLowConfidenceFilterBroadcast(
                        new CostComparator(75, 10, 15),
                        new TaskCountEstimator(() -> TASK_COUNT)))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "10MB")
                .setSystemProperty(DEFAULT_FILTER_FACTOR_ENABLED, "true")
                .setSystemProperty(RECONSIDER_LOW_CONFIDENCE_FILTER_BROADCAST, "true")
                .setSystemProperty(LOW_CONFIDENCE_FILTER_BROADCAST_SCALE_FACTOR, "0.05")
                .overrideStats(PROBE_ID, stats("probe_key", 1_000_000, 1_000, HIGH))
                .overrideStats(BUILD_JOIN_ID, stats("build_key", FILTERED_BUILD_ROWS, 100_000, LOW))
                .overrideStats(BUILD_JOIN_LEFT_ID, stats("build_join_left_key", 1_000, 8, HIGH))
                .overrideStats(FILTER_ID, stats("build_key", FILTERED_BUILD_ROWS, 100_000, HIGH))
                .overrideStats(BUILD_SOURCE_ID, stats("build_key", BUILD_SOURCE_ROWS, 100_000, HIGH))
                .on(q9ShapedJoinPlan())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("probe_key", "build_key")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("probe_key", 0)),
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("build_join_left_key", "build_key")),
                                Optional.empty(),
                                Optional.of(REPLICATED),
                                values(ImmutableMap.of("build_join_left_key", 0)),
                                filter("true", values(ImmutableMap.of("build_key", 0))))));
    }

    @Test
    public void testDisabledByDefault()
    {
        assertRule(
                stats("probe_key", 1_000_000, 1_000, HIGH),
                stats("build_key", FILTERED_BUILD_ROWS, 100_000, LOW),
                stats("build_key", BUILD_SOURCE_ROWS, 100_000, HIGH))
                .setSystemProperty(LOW_CONFIDENCE_FILTER_BROADCAST_SCALE_FACTOR, "0.05")
                .doesNotFire();
    }

    @Test
    public void testRequiresUnknownFilterCoefficientSignature()
    {
        assertRule(
                stats("probe_key", 1_000_000, 1_000, HIGH),
                stats("build_key", FILTERED_BUILD_ROWS, 100_000, LOW),
                stats("build_key", FILTERED_BUILD_ROWS, 100_000, HIGH))
                .setSystemProperty(RECONSIDER_LOW_CONFIDENCE_FILTER_BROADCAST, "true")
                .setSystemProperty(LOW_CONFIDENCE_FILTER_BROADCAST_SCALE_FACTOR, "0.05")
                .doesNotFire();
    }

    @Test
    public void testRequiresDefaultFilterFactorEnabled()
    {
        assertRule(
                stats("probe_key", 1_000_000, 1_000, HIGH),
                stats("build_key", FILTERED_BUILD_ROWS, 100_000, LOW),
                stats("build_key", BUILD_SOURCE_ROWS, 100_000, HIGH))
                .setSystemProperty(DEFAULT_FILTER_FACTOR_ENABLED, "false")
                .setSystemProperty(RECONSIDER_LOW_CONFIDENCE_FILTER_BROADCAST, "true")
                .setSystemProperty(LOW_CONFIDENCE_FILTER_BROADCAST_SCALE_FACTOR, "0.05")
                .doesNotFire();
    }

    @Test
    public void testRequiresLowConfidenceBuild()
    {
        assertRule(
                stats("probe_key", 1_000_000, 1_000, HIGH),
                stats("build_key", FILTERED_BUILD_ROWS, 100_000, HIGH),
                stats("build_key", BUILD_SOURCE_ROWS, 100_000, HIGH))
                .setSystemProperty(RECONSIDER_LOW_CONFIDENCE_FILTER_BROADCAST, "true")
                .setSystemProperty(LOW_CONFIDENCE_FILTER_BROADCAST_SCALE_FACTOR, "0.05")
                .doesNotFire();
    }

    @Test
    public void testRequiresAdjustedBuildBelowBroadcastLimit()
    {
        assertRule(
                stats("probe_key", 1_000_000, 1_000, HIGH),
                stats("build_key", FILTERED_BUILD_ROWS, 100_000, LOW),
                stats("build_key", BUILD_SOURCE_ROWS, 100_000, HIGH))
                .setSystemProperty(RECONSIDER_LOW_CONFIDENCE_FILTER_BROADCAST, "true")
                .setSystemProperty(LOW_CONFIDENCE_FILTER_BROADCAST_SCALE_FACTOR, "0.5")
                .doesNotFire();
    }

    @Test
    public void testRetainsPartitionedWhenReplicatedCostIsHigher()
    {
        assertRule(
                stats("probe_key", 1_000, 1_000, HIGH),
                stats("build_key", FILTERED_BUILD_ROWS, 100_000, LOW),
                stats("build_key", BUILD_SOURCE_ROWS, 100_000, HIGH))
                .setSystemProperty(RECONSIDER_LOW_CONFIDENCE_FILTER_BROADCAST, "true")
                .setSystemProperty(LOW_CONFIDENCE_FILTER_BROADCAST_SCALE_FACTOR, "0.05")
                .doesNotFire();
    }

    private RuleAssert assertRule(
            PlanNodeStatsEstimate probeStats,
            PlanNodeStatsEstimate filteredBuildStats,
            PlanNodeStatsEstimate buildSourceStats)
    {
        return tester.assertThat(new ReconsiderLowConfidenceFilterBroadcast(
                        new CostComparator(75, 10, 15),
                        new TaskCountEstimator(() -> TASK_COUNT)))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "10MB")
                .setSystemProperty(DEFAULT_FILTER_FACTOR_ENABLED, "true")
                .overrideStats(PROBE_ID, probeStats)
                .overrideStats(FILTER_ID, filteredBuildStats)
                .overrideStats(BUILD_SOURCE_ID, buildSourceStats)
                .on(joinPlan());
    }

    private static Function<PlanBuilder, PlanNode> joinPlan()
    {
        return p -> {
            VariableReferenceExpression probeKey = p.variable("probe_key", createUnboundedVarcharType());
            VariableReferenceExpression buildKey = p.variable("build_key", createUnboundedVarcharType());
            return p.join(
                    INNER,
                    p.values(new PlanNodeId(PROBE_ID), 1, probeKey),
                    p.filter(
                            new PlanNodeId(FILTER_ID),
                            TRUE_CONSTANT,
                            p.values(new PlanNodeId(BUILD_SOURCE_ID), 1, buildKey)),
                    ImmutableList.of(new EquiJoinClause(probeKey, buildKey)),
                    ImmutableList.of(probeKey, buildKey),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(PARTITIONED),
                    ImmutableMap.of());
        };
    }

    private static Function<PlanBuilder, PlanNode> q9ShapedJoinPlan()
    {
        return p -> {
            VariableReferenceExpression probeKey = p.variable("probe_key", createUnboundedVarcharType());
            VariableReferenceExpression buildJoinLeftKey = p.variable("build_join_left_key", createUnboundedVarcharType());
            VariableReferenceExpression buildKey = p.variable("build_key", createUnboundedVarcharType());
            PlanNode filteredBuildSource = p.filter(
                    new PlanNodeId(FILTER_ID),
                    TRUE_CONSTANT,
                    p.values(new PlanNodeId(BUILD_SOURCE_ID), 1, buildKey));
            PlanNode buildJoin = new JoinNode(
                    Optional.empty(),
                    new PlanNodeId(BUILD_JOIN_ID),
                    INNER,
                    p.values(new PlanNodeId(BUILD_JOIN_LEFT_ID), 1, buildJoinLeftKey),
                    filteredBuildSource,
                    ImmutableList.of(new EquiJoinClause(buildJoinLeftKey, buildKey)),
                    ImmutableList.of(buildJoinLeftKey, buildKey),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(REPLICATED),
                    ImmutableMap.of());
            return p.join(
                    INNER,
                    p.values(new PlanNodeId(PROBE_ID), 1, probeKey),
                    buildJoin,
                    ImmutableList.of(new EquiJoinClause(probeKey, buildKey)),
                    ImmutableList.of(probeKey, buildJoinLeftKey, buildKey),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(PARTITIONED),
                    ImmutableMap.of());
        };
    }

    private static PlanNodeStatsEstimate stats(String variableName, double rows, double averageRowSize, ConfidenceLevel confidence)
    {
        VariableReferenceExpression variable = new VariableReferenceExpression(Optional.empty(), variableName, createUnboundedVarcharType());
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rows)
                .setConfidence(confidence)
                .addVariableStatistics(ImmutableMap.of(
                        variable,
                        new VariableStatsEstimate(0, rows, 0, averageRowSize, rows)))
                .build();
    }
}
