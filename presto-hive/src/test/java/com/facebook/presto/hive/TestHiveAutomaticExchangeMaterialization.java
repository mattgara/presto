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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_BUCKET_MEMORY_FRACTION;
import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_MAX_BUCKET_COUNT;
import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_MEMORY_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveAutomaticExchangeMaterialization
        extends AbstractTestQueryFramework
{
    private static final String JOIN_THEN_AGGREGATION = "SELECT c.mktsegment, count(*) " +
            "FROM orders o JOIN customer c ON o.custkey = c.custkey " +
            "GROUP BY c.mktsegment";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(ImmutableList.of(ORDERS, CUSTOMER));
    }

    @Test
    public void testStreamedExchangesUseSystemPartitioning()
    {
        // Every stage fits, so every exchange streams and is partitioned as without materialization.
        Session session = automatic("1TB");
        List<ExchangeNode> exchanges = remoteRepartitions(plan(JOIN_THEN_AGGREGATION, session));
        assertFalse(exchanges.isEmpty());
        exchanges.forEach(exchange -> {
            assertFalse(exchange.getScope() == REMOTE_MATERIALIZED);
            assertEquals(exchange.getPartitioningScheme().getPartitioning().getHandle(), FIXED_HASH_DISTRIBUTION);
        });
        assertQuery(session, JOIN_THEN_AGGREGATION);
    }

    @Test
    public void testMaterializedExchangesKeepConnectorPartitioning()
    {
        Session session = automatic("1B");
        List<ExchangeNode> exchanges = remoteRepartitions(plan(JOIN_THEN_AGGREGATION, session));
        assertFalse(exchanges.isEmpty());
        exchanges.forEach(exchange -> {
            assertTrue(exchange.getScope() == REMOTE_MATERIALIZED);
            assertTrue(exchange.getPartitioningScheme().getPartitioning().getHandle().getConnectorId().isPresent());
        });
        assertQuery(session, JOIN_THEN_AGGREGATION);
    }

    @Test
    public void testMaterializedStageGetsTheBucketsItsStateNeeds()
    {
        // At 5 kB the join's build side needs a few buckets, fewer than the session's 64.
        Session session = Session.builder(automatic("5kB")).setSystemProperty(HASH_PARTITION_COUNT, "64").build();
        List<ExchangeNode> materialized = materialized(plan(JOIN_THEN_AGGREGATION, session));
        assertFalse(materialized.isEmpty());
        materialized.forEach(exchange -> {
            int buckets = bucketCount(exchange);
            assertTrue(buckets >= 2 && buckets < 64 && Integer.bitCount(buckets) == 1, "buckets " + buckets);
        });
        assertQuery(session, JOIN_THEN_AGGREGATION);

        // A stage that needs more buckets than the session allows gets that many.
        Session capped = Session.builder(automatic("1B")).setSystemProperty(HASH_PARTITION_COUNT, "16").build();
        materialized(plan(JOIN_THEN_AGGREGATION, capped)).forEach(exchange -> assertEquals(bucketCount(exchange), 16));
    }

    @Test
    public void testMaxBucketCountAllowsMoreBucketsThanThePartitionCount()
    {
        // Streamed exchanges keep hash_partition_count partitions, while a materialized stage that needs more buckets
        // gets up to exchange_materialization_max_bucket_count.
        Session session = Session.builder(automatic("1B"))
                .setSystemProperty(HASH_PARTITION_COUNT, "16")
                .setSystemProperty(EXCHANGE_MATERIALIZATION_MAX_BUCKET_COUNT, "64")
                .build();
        List<ExchangeNode> materialized = materialized(plan(JOIN_THEN_AGGREGATION, session));
        assertFalse(materialized.isEmpty());
        materialized.forEach(exchange -> assertEquals(bucketCount(exchange), 64));
        assertQuery(session, JOIN_THEN_AGGREGATION);
    }

    @Test
    public void testBucketMemoryFractionGivesMoreBuckets()
    {
        // The fraction sizes the buckets of the stages the threshold materializes, so the same exchanges are
        // materialized, each into more buckets than when a bucket may hold the whole threshold.
        Session whole = Session.builder(automatic("5kB")).setSystemProperty(HASH_PARTITION_COUNT, "64").build();
        Session quarter = Session.builder(whole).setSystemProperty(EXCHANGE_MATERIALIZATION_BUCKET_MEMORY_FRACTION, "0.25").build();
        List<ExchangeNode> wholeExchanges = materialized(plan(JOIN_THEN_AGGREGATION, whole));
        List<ExchangeNode> quarterExchanges = materialized(plan(JOIN_THEN_AGGREGATION, quarter));
        assertFalse(wholeExchanges.isEmpty());
        assertEquals(quarterExchanges.size(), wholeExchanges.size());
        int wholeBuckets = bucketCount(wholeExchanges.get(0));
        quarterExchanges.forEach(exchange -> {
            int buckets = bucketCount(exchange);
            assertTrue(buckets > wholeBuckets && buckets <= 64 && Integer.bitCount(buckets) == 1, "buckets " + buckets + " vs " + wholeBuckets);
        });
        assertQuery(quarter, JOIN_THEN_AGGREGATION);
    }

    private static List<ExchangeNode> materialized(Plan plan)
    {
        return remoteRepartitions(plan).stream()
                .filter(exchange -> exchange.getScope() == REMOTE_MATERIALIZED)
                .collect(toImmutableList());
    }

    private static int bucketCount(ExchangeNode exchange)
    {
        return ((HivePartitioningHandle) exchange.getPartitioningScheme().getPartitioning().getHandle().getConnectorHandle()).getBucketCount();
    }

    private static List<ExchangeNode> remoteRepartitions(Plan plan)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getType() == REPARTITION && ((ExchangeNode) node).getScope().isRemote())
                .findAll();
    }

    private Session automatic(String threshold)
    {
        return Session.builder(getSession())
                .setSystemProperty(PARTITIONING_PROVIDER_CATALOG, "hive")
                .setSystemProperty(EXCHANGE_MATERIALIZATION_STRATEGY, "AUTOMATIC")
                .setSystemProperty(EXCHANGE_MATERIALIZATION_MEMORY_THRESHOLD, threshold)
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .build();
    }
}
