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
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * One table's sample: the factor from sample rows to table rows, and its sample key, the column whose sampled values
 * keep all their rows (so groups on it are whole and their count scales like rows). The input statistics are what the
 * planner reads for the table (row count and total size) and compares a statistics entry against.
 */
public class SampleTable
{
    private final String name;
    private final double scale;
    private final String key;
    private final double inputRows;
    private final double inputSize;

    public SampleTable(String name, double scale, String key, double inputRows, double inputSize)
    {
        this.name = requireNonNull(name, "name is null");
        this.scale = scale;
        this.key = key;
        this.inputRows = inputRows;
        this.inputSize = inputSize;
    }

    /**
     * Tables from properties written by the sample writer: {@code <table>.<field>=<value>}.
     */
    static Map<String, SampleTable> parse(Properties properties)
    {
        ImmutableMap.Builder<String, SampleTable> tables = ImmutableMap.builder();
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
        return tables.build();
    }

    public String getName()
    {
        return name;
    }

    public double getScale()
    {
        return scale;
    }

    public String getKey()
    {
        return key;
    }

    public double getInputRows()
    {
        return inputRows;
    }

    public double getInputSize()
    {
        return inputSize;
    }
}
