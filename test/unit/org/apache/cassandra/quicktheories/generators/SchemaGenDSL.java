/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.quicktheories.generators;

import org.quicktheories.core.Gen;

import static org.quicktheories.generators.SourceDSL.*;

// TODO (jwest): extends SchemaGen is nasty hack to import non-public scope
public class SchemaGenDSL extends SchemaGen
{
    public SchemaGenBuilder keyspace(String keyspace) {
        return new SchemaGenBuilder(keyspace);
    }

    public static class SchemaGenBuilder {
        private final String keyspace;

        private int minPks = 1;
        private int maxPks = 1;
        private int minStatics = 0;
        private int maxStatics = 0;
        private int minCluster = 0;
        private int maxCluster = 0;

        public SchemaGenBuilder(String keyspace) {
            this.keyspace = keyspace;
        }

        public SchemaGenBuilder partitionKeyColumnCount(int numCols) {
            return partitionKeyColumnCountBetween(numCols, numCols);
        }

        public SchemaGenBuilder partitionKeyColumnCountBetween(int minCols, int maxCols) {
            this.minPks = minCols;
            this.maxPks = maxCols;
            return this;
        }

        public SchemaGenBuilder clusteringColumnCount(int numCols) {
            return clusteringColumnCountBetween(numCols, numCols);
        }

        public SchemaGenBuilder clusteringColumnCountBetween(int minCols, int maxCols) {
            this.minCluster = minCols;
            this.maxCluster = maxCols;
            return this;
        }

        public SchemaGenBuilder staticColumnCount(int numCols) {
            return staticColumnCountBetween(numCols, numCols);
        }

        public SchemaGenBuilder staticColumnCountBetween(int minCols, int maxCols) {
            this.minStatics = minCols;
            this.maxStatics = maxCols;
            return this;
        }

        public Gen<SchemaSpec> regularColumnCount(int numCols) {
            return regularColumnCountBetween(numCols, numCols);
        }

        public Gen<SchemaSpec> regularColumnCountBetween(int minCols, int maxCols) {
            return tableNameGenerator.zip(lists().of(pkColumnGenerator()).ofSizeBetween(minPks, maxPks),
                                   lists().of(ckColumnGenerator()).ofSizeBetween(minCluster, maxCluster),
                                   lists().of(staticColumnGenerator()).ofSizeBetween(minStatics, maxStatics),
                                   lists().of(regularColumnGenerator()).ofSizeBetween(minCols, maxCols),
                                   (name, pks, cks, statics, regular) -> new SchemaSpec(keyspace, name, pks,cks, statics, regular));
        }

    }
}
