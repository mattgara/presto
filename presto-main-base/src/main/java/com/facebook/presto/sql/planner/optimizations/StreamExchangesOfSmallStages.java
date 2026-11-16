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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getExchangeMaterializationBucketMemoryFraction;
import static com.facebook.presto.SystemSessionProperties.getExchangeMaterializationMemoryThreshold;
import static com.facebook.presto.SystemSessionProperties.getExchangeMaterializationStrategy;
import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy.AUTOMATIC;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

/**
 * Materializes exchanges only into the stages that need it. With the AUTOMATIC exchange materialization strategy,
 * AddExchanges materializes the same exchanges as ALL. This optimizer then streams the materialized exchanges into
 * every stage whose estimated memory per worker, together with that of the stages streaming into it, is at most
 * exchange_materialization_memory_threshold: stages connected by streamed exchanges run at the same time. A stage is
 * the part of the plan between remote exchanges. Its memory is the state of its operators: join and semi join build
 * sides, grouped final aggregations (bounded by their input when their output has no estimate), and the inputs of
 * window, row number and mark distinct. The partitioned part is divided among the workers, the replicated part is
 * not. A stage whose exchanges stay materialized runs after the stages that write them, and holds the state of one
 * bucket per worker at a time when it has a bucket count. A stage without an estimate keeps its exchanges
 * materialized, and so do the stages it streams into. The exchanges streamed into a stage are also
 * partitioned as without materialization (system hash partitioning) when all of the stage's repartitioning inputs
 * stream and the stage neither reads nor writes a table, whose connector partitioning the stage could depend on.
 * Under the same conditions, a stage that stays materialized gets the fewest buckets (a power of two, at least one
 * per worker, at most hash_partition_count) in which a bucket's share of the state fits
 * exchange_materialization_bucket_memory_fraction of the threshold: GPU operators can hold several times the state
 * they keep, so a bucket may need headroom that the decision to materialize does not.
 */
public class StreamExchangesOfSmallStages
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final StatsCalculator statsCalculator;
    private final TaskCountEstimator taskCountEstimator;

    public StreamExchangesOfSmallStages(Metadata metadata, StatsCalculator statsCalculator, TaskCountEstimator taskCountEstimator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public boolean isEnabled(Session session, boolean forceEnableOptimizer)
    {
        return getExchangeMaterializationStrategy(session) == AUTOMATIC;
    }

    @Override
    public boolean isCostBased(Session session)
    {
        return isEnabled(session, false);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, boolean forceEnableOptimizer)
    {
        if (!isEnabled(session, forceEnableOptimizer)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        StageMemory stageMemory = new StageMemory(
                new CachingStatsProvider(statsCalculator, session, types),
                taskCountEstimator.estimateHashedTaskCount(session),
                getExchangeMaterializationMemoryThreshold(session).toBytes(),
                getExchangeMaterializationMemoryThreshold(session).toBytes() * getExchangeMaterializationBucketMemoryFraction(session),
                getHashPartitionCount(session));
        Set<PlanNodeId> streamed = new HashSet<>();
        Set<PlanNodeId> systemPartitioned = new HashSet<>();
        Map<PlanNodeId, Integer> buckets = new HashMap<>();
        stageMemory.collectExchangesToStream(plan, streamed, systemPartitioned, buckets);
        if (streamed.isEmpty() && buckets.isEmpty()) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        return PlanOptimizerResult.optimizerResult(SimplePlanRewriter.rewriteWith(new Streamer(metadata, session, streamed, systemPartitioned, buckets), plan), true);
    }

    private static class StageMemory
    {
        private final StatsProvider statsProvider;
        private final int workers;
        private final double threshold;
        private final double bucketThreshold;
        private final int maxBuckets;

        private StageMemory(StatsProvider statsProvider, int workers, double threshold, double bucketThreshold, int maxBuckets)
        {
            this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
            this.workers = workers;
            this.threshold = threshold;
            this.bucketThreshold = bucketThreshold;
            this.maxBuckets = maxBuckets;
        }

        // Decides for the stage rooted at stageRoot after the stages that feed it. Returns the stage's memory per worker
        // while it runs: its state and that of the stages that stream into it, which run at the same time. A stage whose
        // inputs stay materialized runs after the stages that write them, one bucket per worker at a time.
        private double collectExchangesToStream(PlanNode stageRoot, Set<PlanNodeId> streamed, Set<PlanNodeId> systemPartitioned, Map<PlanNodeId, Integer> buckets)
        {
            List<ExchangeNode> inputs = new ArrayList<>();
            double partitionedBytes = 0;
            double replicatedBytes = 0;
            boolean usesTable = false;
            Deque<PlanNode> pending = new ArrayDeque<>();
            pending.push(stageRoot);
            while (!pending.isEmpty()) {
                PlanNode node = pending.pop();
                if (node instanceof ExchangeNode && ((ExchangeNode) node).getScope().isRemote()) {
                    inputs.add((ExchangeNode) node);
                    continue;
                }
                usesTable |= node instanceof TableScanNode || node instanceof TableWriterNode;
                if (isReplicated(node)) {
                    replicatedBytes += stateBytes(node);
                }
                else {
                    partitionedBytes += stateBytes(node);
                }
                node.getSources().forEach(pending::push);
            }
            double streamingInputBytes = 0;
            double materializedInputBytes = 0;
            for (ExchangeNode input : inputs) {
                for (PlanNode source : input.getSources()) {
                    double inputBytes = collectExchangesToStream(source, streamed, systemPartitioned, buckets);
                    if (input.getScope() == REMOTE_MATERIALIZED) {
                        materializedInputBytes += inputBytes;
                    }
                    else {
                        streamingInputBytes += inputBytes;
                    }
                }
            }
            List<ExchangeNode> repartitions = inputs.stream()
                    .filter(input -> input.getType() == REPARTITION)
                    .collect(toImmutableList());
            boolean repartitionable = !usesTable && !repartitions.isEmpty() && repartitions.stream().allMatch(input -> input.getScope() == REMOTE_MATERIALIZED);
            double stageBytes = partitionedBytes / workers + replicatedBytes;
            // An unknown estimate is NaN, which fails the comparison.
            if (stageBytes + streamingInputBytes + materializedInputBytes <= threshold) {
                inputs.stream()
                        .filter(input -> input.getScope() == REMOTE_MATERIALIZED)
                        .forEach(input -> streamed.add(input.getId()));
                if (repartitionable) {
                    repartitions.forEach(input -> systemPartitioned.add(input.getId()));
                }
                return stageBytes + streamingInputBytes + materializedInputBytes;
            }
            if (repartitionable && repartitions.stream().allMatch(input -> input.getPartitioningScheme().getPartitioning().getHandle().getConnectorId().isPresent())) {
                // One bucket per lifespan: a worker holds the partitioned state of one bucket at a time. Only a
                // connector partitioning has a bucket count to choose.
                double needed = Math.ceil(partitionedBytes / (bucketThreshold - replicatedBytes));
                if (replicatedBytes < bucketThreshold && needed < maxBuckets) {
                    int count = Math.max(workers, Integer.highestOneBit((int) Math.max(1, needed - 1)) << 1);
                    if (count < maxBuckets) {
                        repartitions.forEach(input -> buckets.put(input.getId(), count));
                        return partitionedBytes / count + replicatedBytes + streamingInputBytes;
                    }
                }
            }
            return stageBytes + streamingInputBytes;
        }

        private double stateBytes(PlanNode node)
        {
            if (node instanceof JoinNode) {
                return outputBytes(((JoinNode) node).getRight());
            }
            if (node instanceof SemiJoinNode) {
                return outputBytes(((SemiJoinNode) node).getFilteringSource());
            }
            if (node instanceof AggregationNode) {
                AggregationNode aggregation = (AggregationNode) node;
                boolean holdsGroups = (aggregation.getStep() == FINAL || aggregation.getStep() == SINGLE) && !aggregation.getGroupingKeys().isEmpty();
                if (!holdsGroups) {
                    return 0;
                }
                // An aggregation holds at most one group per input row, which bounds a missing estimate.
                double groups = outputBytes(aggregation);
                return isNaN(groups) ? outputBytes(aggregation.getSource()) : groups;
            }
            if (node instanceof WindowNode || node instanceof RowNumberNode || node instanceof MarkDistinctNode) {
                return outputBytes(node.getSources().get(0));
            }
            return 0;
        }

        private static boolean isReplicated(PlanNode node)
        {
            if (node instanceof JoinNode) {
                return ((JoinNode) node).getDistributionType().map(REPLICATED::equals).orElse(false);
            }
            if (node instanceof SemiJoinNode) {
                return ((SemiJoinNode) node).getDistributionType().map(SemiJoinNode.DistributionType.REPLICATED::equals).orElse(false);
            }
            return false;
        }

        private double outputBytes(PlanNode node)
        {
            return statsProvider.getStats(node).getOutputSizeInBytes(node);
        }
    }

    private static class Streamer
            extends SimplePlanRewriter<Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final Set<PlanNodeId> streamed;
        private final Set<PlanNodeId> systemPartitioned;
        private final Map<PlanNodeId, Integer> buckets;

        private Streamer(Metadata metadata, Session session, Set<PlanNodeId> streamed, Set<PlanNodeId> systemPartitioned, Map<PlanNodeId, Integer> buckets)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.streamed = requireNonNull(streamed, "streamed is null");
            this.systemPartitioned = requireNonNull(systemPartitioned, "systemPartitioned is null");
            this.buckets = requireNonNull(buckets, "buckets is null");
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            ExchangeNode rewritten = (ExchangeNode) context.defaultRewrite(node);
            Integer bucketCount = buckets.get(node.getId());
            if (bucketCount != null) {
                return withPartitioningScheme(rewritten, rewritten.getScope(), withBuckets(rewritten.getPartitioningScheme(), bucketCount));
            }
            if (!streamed.contains(node.getId())) {
                return rewritten;
            }
            return withPartitioningScheme(
                    rewritten,
                    REMOTE_STREAMING,
                    systemPartitioned.contains(node.getId()) ? withSystemPartitioning(rewritten.getPartitioningScheme()) : rewritten.getPartitioningScheme());
        }

        private static ExchangeNode withPartitioningScheme(ExchangeNode node, ExchangeNode.Scope scope, PartitioningScheme scheme)
        {
            return new ExchangeNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    node.getType(),
                    scope,
                    scheme,
                    node.getSources(),
                    node.getInputs(),
                    node.isEnsureSourceOrdering(),
                    node.getOrderingScheme());
        }

        // The connector's exchange partitioning with 'bucketCount' buckets.
        private PartitioningScheme withBuckets(PartitioningScheme scheme, int bucketCount)
        {
            Partitioning partitioning = scheme.getPartitioning();
            List<Type> types = partitioning.getArguments().stream()
                    .map(RowExpression::getType)
                    .collect(toImmutableList());
            String catalog = partitioning.getHandle().getConnectorId().get().getCatalogName();
            return withHandle(scheme, metadata.getPartitioningHandleForExchange(session, catalog, bucketCount, types));
        }

        private static PartitioningScheme withSystemPartitioning(PartitioningScheme scheme)
        {
            return withHandle(scheme, FIXED_HASH_DISTRIBUTION);
        }

        private static PartitioningScheme withHandle(PartitioningScheme scheme, PartitioningHandle handle)
        {
            return new PartitioningScheme(
                    new Partitioning(handle, scheme.getPartitioning().getArguments()),
                    scheme.getOutputLayout(),
                    scheme.getHashColumn(),
                    scheme.isReplicateNullsAndAny(),
                    scheme.isScaleWriters(),
                    scheme.getEncoding(),
                    scheme.getBucketToPartition());
        }
    }
}
