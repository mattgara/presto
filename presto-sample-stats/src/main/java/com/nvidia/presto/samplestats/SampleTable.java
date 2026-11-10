/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.presto.samplestats;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * One table's sample: its row count and the table's, and its sample key, the column whose sampled values keep all
 * their rows (so groups on it are whole and their count scales like rows). The input statistics are what the
 * planner reads for the table (metastore row count and total size) and compares a provider's entry against.
 */
final class SampleTable
{
    private final String name;
    private final double scale;
    private final String key;
    private final double inputRows;
    private final double inputSize;

    private SampleTable(String name, double scale, String key, double inputRows, double inputSize)
    {
        this.name = name;
        this.scale = scale;
        this.key = key;
        this.inputRows = inputRows;
        this.inputSize = inputSize;
    }

    /** Tables from properties written by build_samples.py: {@code <table>.<field>=<value>}. */
    static Map<String, SampleTable> parse(Properties properties)
    {
        Map<String, SampleTable> tables = new HashMap<>();
        for (String name : properties.stringPropertyNames()) {
            if (!name.endsWith(".scale")) {
                continue;
            }
            String table = name.substring(0, name.length() - ".scale".length());
            tables.put(table, new SampleTable(
                    table,
                    Double.parseDouble(properties.getProperty(table + ".scale")),
                    properties.getProperty(table + ".key"),
                    Double.parseDouble(properties.getProperty(table + ".input_rows")),
                    Double.parseDouble(properties.getProperty(table + ".input_size"))));
        }
        return tables;
    }

    String getName()
    {
        return name;
    }

    double getScale()
    {
        return scale;
    }

    String getKey()
    {
        return key;
    }

    double getInputRows()
    {
        return inputRows;
    }

    double getInputSize()
    {
        return inputSize;
    }
}
