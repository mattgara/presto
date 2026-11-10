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

import com.facebook.presto.spi.plan.PlanNode;

import java.util.Optional;

/**
 * Evaluates plan fragments on table samples: a subtree that reads one sampled table runs on that table's sample, and
 * the result says how many sample rows it outputs. The provider scales the count to the full table.
 */
public interface FragmentEngine
{
    /**
     * Evaluates the subtree rooted at {@code node} on the sample of the one table it reads. Returns empty when the
     * subtree is not one the engine evaluates (it reads no sampled table or contains a node the engine does not
     * support), so the planner keeps its own estimate. Throws when the engine supports the subtree but cannot
     * evaluate it (for example an unsupported function), which the provider reports as a warning.
     */
    Optional<FragmentResult> evaluate(PlanNode node);
}
