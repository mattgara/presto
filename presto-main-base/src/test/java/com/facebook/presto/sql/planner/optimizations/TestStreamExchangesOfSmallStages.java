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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_BUCKET_MEMORY_FRACTION;
import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_MEMORY_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy.ALL;
import static com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy.AUTOMATIC;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStreamExchangesOfSmallStages
        extends BasePlanTest
{
    // The join builds on customer (1,500 rows in the tiny schema, about 27 kB), the aggregation keeps five groups.
    private static final String JOIN_THEN_SMALL_AGGREGATION = "SELECT c.mktsegment, count(*) " +
            "FROM orders o JOIN customer c ON o.custkey = c.custkey " +
            "GROUP BY c.mktsegment";
    // The join builds on customer's custkey (about 14 kB); the aggregation, in the stage after the join's, keeps one
    // group per order (about 262 kB).
    private static final String JOIN_THEN_LARGE_AGGREGATION = "SELECT o.orderkey, count(*) " +
            "FROM orders o JOIN customer c ON o.custkey = c.custkey " +
            "GROUP BY o.orderkey";

    @Test
    public void testStreamsEveryExchangeWhenEveryStageFits()
    {
        assertEquals(exchanges(distributedPlan(JOIN_THEN_SMALL_AGGREGATION, session(ALL.name(), "1TB")), REMOTE_MATERIALIZED).size(), 3);
        assertEquals(exchanges(distributedPlan(JOIN_THEN_SMALL_AGGREGATION, session(AUTOMATIC.name(), "1TB")), REMOTE_MATERIALIZED).size(), 0);
    }

    @Test
    public void testMaterializesLikeAllWhenNoStageFits()
    {
        assertEquals(exchanges(distributedPlan(JOIN_THEN_SMALL_AGGREGATION, session(AUTOMATIC.name(), "1B")), REMOTE_MATERIALIZED).size(), 3);
    }

    @Test
    public void testMaterializesOnlyTheExchangesOfLargeStages()
    {
        Plan plan = distributedPlan(JOIN_THEN_LARGE_AGGREGATION, session(AUTOMATIC.name(), "100kB"));
        // The aggregation's input, which comes from the join, is materialized.
        List<ExchangeNode> materialized = exchanges(plan, REMOTE_MATERIALIZED);
        assertEquals(materialized.size(), 1);
        assertFalse(searchFrom(materialized.get(0)).where(JoinNode.class::isInstance).findAll().isEmpty());
        // The join's inputs stream.
        List<ExchangeNode> streamed = exchanges(plan, REMOTE_STREAMING);
        assertEquals(streamed.size(), 2);
        streamed.forEach(exchange -> assertTrue(searchFrom(exchange).where(JoinNode.class::isInstance).findAll().isEmpty()));
        // The bucket memory fraction sizes buckets only: the same exchanges stream however small it is.
        Session smallBuckets = Session.builder(session(AUTOMATIC.name(), "100kB")).setSystemProperty(EXCHANGE_MATERIALIZATION_BUCKET_MEMORY_FRACTION, "0.001").build();
        Plan smallBucketsPlan = distributedPlan(JOIN_THEN_LARGE_AGGREGATION, smallBuckets);
        assertEquals(exchanges(smallBucketsPlan, REMOTE_MATERIALIZED).size(), materialized.size());
        assertEquals(exchanges(smallBucketsPlan, REMOTE_STREAMING).size(), streamed.size());
    }

    @Test
    public void testCountsTheStagesThatStreamIntoEachOther()
    {
        // The join's and the aggregation's stages each fit 270 kB, both together do not. Streamed, they would run at
        // the same time: the aggregation's input is materialized, so that the join runs first.
        List<ExchangeNode> materialized = exchanges(distributedPlan(JOIN_THEN_LARGE_AGGREGATION, session(AUTOMATIC.name(), "270kB")), REMOTE_MATERIALIZED);
        assertEquals(materialized.size(), 1);
        assertFalse(searchFrom(materialized.get(0)).where(JoinNode.class::isInstance).findAll().isEmpty());
        assertEquals(exchanges(distributedPlan(JOIN_THEN_LARGE_AGGREGATION, session(AUTOMATIC.name(), "280kB")), REMOTE_MATERIALIZED).size(), 0);
    }

    @Test
    public void testMaterializesWhenTheStageEstimateIsUnknown()
    {
        // Window functions have no statistics, so the join's build side has no size estimate. The window's own
        // stage, which holds customer, streams its input.
        String sql = "SELECT sum(c.total) FROM orders o " +
                "JOIN (SELECT custkey, sum(acctbal) OVER (PARTITION BY nationkey) total FROM customer) c ON o.custkey = c.custkey";
        assertEquals(exchanges(distributedPlan(sql, session(AUTOMATIC.name(), "1TB")), REMOTE_MATERIALIZED).size(), 2);
    }

    @Test
    public void testBoundsAnAggregationWithoutEstimateByItsInput()
    {
        // substr() has no distinct-value estimate, so the aggregation's output is unknown; it holds at most its input.
        String sql = "SELECT substr(c.name, 1, 3), count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey GROUP BY 1";
        assertEquals(exchanges(distributedPlan(sql, session(AUTOMATIC.name(), "1TB")), REMOTE_MATERIALIZED).size(), 0);
    }

    @Test
    public void testReplicatesOnlyBuildsThatFitTheThreshold()
    {
        // Under AUTOMATIC, the cost-based choice broadcasts customer only when its 27 kB fit the threshold: every worker
        // holds a replicated build whole, whatever the bucket count.
        assertEquals(joinDistribution(session(AUTOMATIC.name(), "1TB", FeaturesConfig.JoinDistributionType.AUTOMATIC)), JoinDistributionType.REPLICATED);
        assertEquals(joinDistribution(session(AUTOMATIC.name(), "5kB", FeaturesConfig.JoinDistributionType.AUTOMATIC)), JoinDistributionType.PARTITIONED);
        // The threshold does not limit broadcasts under another strategy.
        assertEquals(joinDistribution(session(ALL.name(), "5kB", FeaturesConfig.JoinDistributionType.AUTOMATIC)), JoinDistributionType.REPLICATED);
    }

    private JoinDistributionType joinDistribution(Session session)
    {
        JoinNode join = searchFrom(distributedPlan(JOIN_THEN_SMALL_AGGREGATION, session).getRoot()).where(JoinNode.class::isInstance).findOnlyElement();
        return join.getDistributionType().orElseThrow(() -> new AssertionError("join without a distribution type"));
    }

    private Plan distributedPlan(String sql, Session session)
    {
        return plan(sql, OPTIMIZED_AND_VALIDATED, false, session);
    }

    private static List<ExchangeNode> exchanges(Plan plan, ExchangeNode.Scope scope)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getType() == REPARTITION && ((ExchangeNode) node).getScope() == scope)
                .findAll();
    }

    private static Session session(String strategy, String threshold)
    {
        return session(strategy, threshold, PARTITIONED);
    }

    private static Session session(String strategy, String threshold, FeaturesConfig.JoinDistributionType joinDistributionType)
    {
        return TestingSession.testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, joinDistributionType.toString())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.toString())
                .setSystemProperty(EXCHANGE_MATERIALIZATION_STRATEGY, strategy)
                .setSystemProperty(EXCHANGE_MATERIALIZATION_MEMORY_THRESHOLD, threshold)
                .build();
    }
}
