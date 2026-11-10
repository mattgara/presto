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

import com.facebook.presto.samplestats.FragmentEngine;
import com.facebook.presto.samplestats.FragmentEngineFactory;
import com.facebook.presto.samplestats.SampleSet;

public class DuckDbFragmentEngineFactory
        implements FragmentEngineFactory
{
    @Override
    public String getName()
    {
        return "duckdb";
    }

    @Override
    public FragmentEngine create(SampleSet samples)
    {
        return new DuckDbFragmentEngine(samples);
    }
}
