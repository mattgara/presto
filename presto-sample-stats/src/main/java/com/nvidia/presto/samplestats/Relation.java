/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.presto.samplestats;

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
