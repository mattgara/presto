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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The result of evaluating a fragment on a sample: the sampled table, the number of sample rows the fragment outputs,
 * whether it is a global aggregation (one row whatever the sample holds), the average length of each string output
 * (by output variable name), and a description for the estimate log (for example the SQL that ran).
 */
public class FragmentResult
{
    private final SampleTable table;
    private final long sampleRows;
    private final boolean global;
    private final Map<String, Double> averageLengths;
    private final String description;

    public FragmentResult(SampleTable table, long sampleRows, boolean global, Map<String, Double> averageLengths, String description)
    {
        this.table = requireNonNull(table, "table is null");
        this.sampleRows = sampleRows;
        this.global = global;
        this.averageLengths = ImmutableMap.copyOf(requireNonNull(averageLengths, "averageLengths is null"));
        this.description = requireNonNull(description, "description is null");
    }

    public SampleTable getTable()
    {
        return table;
    }

    public long getSampleRows()
    {
        return sampleRows;
    }

    public boolean isGlobal()
    {
        return global;
    }

    public Map<String, Double> getAverageLengths()
    {
        return averageLengths;
    }

    public String getDescription()
    {
        return description;
    }
}
