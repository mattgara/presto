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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * The samples of one schema: a directory holding {@code tables.properties} (written by {@code sample_tables.py}) and
 * one Parquet file per table, {@code <table>.parquet}.
 */
public class SampleSet
{
    private final String schema;
    private final Path directory;
    private final Map<String, SampleTable> tables;

    public SampleSet(String schema, Path directory, Map<String, SampleTable> tables)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.directory = requireNonNull(directory, "directory is null");
        this.tables = ImmutableMap.copyOf(requireNonNull(tables, "tables is null"));
    }

    public static SampleSet load(Path directory)
    {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(directory.resolve("tables.properties"))) {
            properties.load(in);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new SampleSet(requireNonNull(properties.getProperty("schema"), "tables.properties has no schema"), directory, SampleTable.parse(properties));
    }

    public String getSchema()
    {
        return schema;
    }

    public Map<String, SampleTable> getTables()
    {
        return tables;
    }

    public Path getFile(SampleTable table)
    {
        return directory.resolve(table.getName() + ".parquet");
    }
}
