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

/**
 * Creates a {@link FragmentEngine}. Engines are found with {@link java.util.ServiceLoader} in the plugin's class
 * path and chosen by name with the {@code engine} configuration property.
 */
public interface FragmentEngineFactory
{
    String getName();

    FragmentEngine create(SampleSet samples);
}
