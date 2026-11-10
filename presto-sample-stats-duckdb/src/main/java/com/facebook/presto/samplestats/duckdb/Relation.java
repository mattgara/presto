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
package com.facebook.presto.samplestats.duckdb;

import com.facebook.presto.samplestats.SampleTable;

import java.util.Set;

/**
 * A plan subtree over one table's sample as SQL whose columns are named after the subtree's output variables.
 * 'keyVariables' carry the table's sample key unchanged; 'opaque' variables are projections that could not be
 * translated (selected as NULL, so nothing above may depend on their values); 'global' marks a global aggregation
 * (one row whatever the sample holds).
 */
final class Relation
{
    private final String sql;
    private final SampleTable table;
    private final Set<String> keyVariables;
    private final Set<String> opaque;
    private final boolean global;

    Relation(String sql, SampleTable table, Set<String> keyVariables, Set<String> opaque, boolean global)
    {
        this.sql = sql;
        this.table = table;
        this.keyVariables = keyVariables;
        this.opaque = opaque;
        this.global = global;
    }

    String getSql()
    {
        return sql;
    }

    SampleTable getTable()
    {
        return table;
    }

    Set<String> getKeyVariables()
    {
        return keyVariables;
    }

    Set<String> getOpaque()
    {
        return opaque;
    }

    boolean isOpaque(String variable)
    {
        return opaque.contains(variable);
    }

    boolean isGlobal()
    {
        return global;
    }
}
