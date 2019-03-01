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

package org.apache.cassandra.db.compaction;

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.test.DistributedTestBase;
import org.apache.cassandra.quicktheories.generators.FullKey;
import org.apache.cassandra.quicktheories.tests.InMemoryModel;
import org.apache.cassandra.quicktheories.tests.StatefulModel;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.data;
import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.operations;
import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.schemas;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.impl.stateful.StatefulTheory.builder;

// TODO (jwest): make it pick the compaction strategy
public class CompactionQTTest extends DistributedTestBase
{
    public static class Analytics {
        private long examples = 0;
        private long writes = 0;
        private long pks = 0;
        private long flushes = 0;
        private long compacts = 0;
        private double writesAvg = 0.0;
        private double pksAvg = 0.0;
        private double flushesAvg = 0.0;
        private double compactsAvg = 0.0;

        public void write(boolean newPK) {
            writes++;
            if (newPK)
                pks++;
        }

        public void flush() {
            flushes++;
        }

        public void compact() {
            compacts++;
        }

        public void newExample() {
            examples++;

            writesAvg += (writes - writesAvg) / examples;
            pksAvg += (pks - pksAvg) / examples;
            flushesAvg = (flushes - flushesAvg) / examples;
            compactsAvg = (compacts - compactsAvg) / examples;

            writes = pks = flushes = compacts = 0;
        }

        public long numExamples() {
            return examples;
        }

        public double averageWrites() {
            return writesAvg;
        }

        public double averagePartitionKeyCount() {
            return pksAvg;
        }

        public double averageFlushes() {
            return flushesAvg;
        }

        public double averageCompactions() {
            return compactsAvg;
        }

    }

    @Test
    public void simpleCompactionTest() throws Throwable {
        try (AbstractCluster compactedCluster = Cluster.create(1);
             AbstractCluster uncompactedCluster = Cluster.create(1))
        {
            init(compactedCluster);
            init(uncompactedCluster);

            compactedCluster.disableAutoCompaction(KEYSPACE);
            uncompactedCluster.disableAutoCompaction(KEYSPACE);
            IInstance compacted =  compactedCluster.get(1);
            IInstance uncompacted = uncompactedCluster.get(1);
            Analytics analytics = new Analytics();

            qt().withShrinkCycles(0)
                .withMinStatefulSteps(1000)
                .withMaxStatefulSteps(10000)
                .withExamples(10)
                .stateful(() -> new StatefulModel(new InMemoryModel(),
                                                  compactedCluster,
                                                  uncompactedCluster)
                {
                    private int flushes = 0;

                    @Override
                    public void insertRow(Pair<FullKey, Insert> rows, int node)
                    {
                        super.insertRow(rows, node);
                        analytics.write(true);
                    }

                    // TODO (jwest): lots here borrowed from QuorumReadModel::run(Select)
                    public boolean validateAllKeys() {
                        Select select = QueryBuilder.select().from(schemaSpec.ksName, schemaSpec.tableName);
                        assertRows(uncompactedCluster.coordinator(1)
                                                     .executeWithPaging(select.toString(),
                                                                        ConsistencyLevel.ALL,
                                                                        10),
                                   compactedCluster.coordinator(1)
                                                   .executeWithPaging(select.toString(),
                                                                      ConsistencyLevel.ALL,
                                                                      10));

                        return true;
                    }

                    public boolean worthCompacting()
                    {
                        // TODO (jwest): fix arbitrary thresholds
                        return flushes > 2 || modelState.partitionKeys().size() > 1000;
                    }

                    public void compact()
                    {
                        compacted.compact(KEYSPACE, schemaSpec.tableName);
                        analytics.compact();
                    }

                    public boolean worthFlushing() {
                        return true;
                    }

                    public void flush()
                    {
                        compacted.flush(KEYSPACE, schemaSpec.tableName);
                        analytics.flush();
                        flushes++;
                    }

                    public Gen<Pair<FullKey, Insert>> writes() {
                        return operations().writes().write(schemaSpec, newOrExistingPartitionKeys()).withCurrentTimestamp();
                    }

                    public Gen<Object[]> newOrExistingPartitionKeys()
                    {
                        // TODO (jwest): fix arbitrary threshold
                        return modelState.partitionKeys().size() < 100
                               ? data().partitionKeys(schemaSpec)
                               : modelState.primaryKeyGen();
                    }

                    protected void initSteps()
                    {
                        analytics.newExample();

                        addSetupStep(builder("initSchema",
                                             this::initSchema,
                                             schemas().keyspace(KEYSPACE)
                                                      .partitionKeyColumnCount(1, 5)
                                                      .clusteringColumnCount(1, 4)
                                                      .staticColumnCount(1, 5)
                                                      .regularColumnCount(1, 8)
                                                      .build())
                                     .build());

                        addStep(100, builder("write", this::insertRow, this::writes, () -> nodeSelector).build());
                        addStep(5, builder("flush", this::flush).precondition(this::worthFlushing).build());
                        addStep(1, builder("compactAndValidate", this::compact)
                                   .precondition(this::worthCompacting)
                                   .postcondition(this::validateAllKeys).build());
                    }
                });

            System.out.println(String.format("Test Completed after %d examples %nAverage Writes: %f%nAverage Partition Keys: %f%nAverage Flushes: %f%nAverage Compacts: %f",
                                             analytics.numExamples(), analytics.averageWrites(), analytics.averagePartitionKeyCount(), analytics.averageFlushes(), analytics.averageCompactions()));
        }
    }
}
