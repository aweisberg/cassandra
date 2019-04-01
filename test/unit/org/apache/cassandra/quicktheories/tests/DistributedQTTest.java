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

package org.apache.cassandra.quicktheories.tests;

import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.test.DistributedTestBase;
import org.quicktheories.impl.stateful.StatefulTheory;

import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.operations;
import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.schemas;
import static org.quicktheories.impl.stateful.StatefulTheory.builder;

public class DistributedQTTest extends DistributedTestBase
{
    private void stateful(Supplier<StatefulTheory<StatefulTheory.Step>> supplier)
    {
        qt().withProfile(DistributedTestBase.class, "ci")
            .stateful(supplier::get);
    }

    @Test
    public void readWriteTest() throws Throwable
    {
        try (AbstractCluster testCluster = init(Cluster.create(3));
             AbstractCluster modelCluster = init(Cluster.create(1)))
        {
            modelCluster.disableAutoCompaction(KEYSPACE);

            stateful(() -> new StatefulModel(new InMemoryModel(),
                                             testCluster,
                                             modelCluster)
            {
                @Override
                public void initSteps()
                {
                    addSetupStep(builder("Initialize Schema",
                                         this::initSchema,
                                         schemas().keyspace(KEYSPACE)
                                                  .partitionKeyColumnCount(1, 5)
                                                  .clusteringColumnCount(0, 5)
                                                  .staticColumnCount(0, 5)
                                                  .regularColumnCount(0, 5)
                                                  .build()));

                    addSetupStep(builder("Preload Rows",
                                         this::insertRows,
                                         () -> operations().writes().rows(schemaSpec)
                                                           .rowCountBetween(10, 100)
                                                           .withCurrentTimestamp()
                                                           .inserts(),
                                         () -> nodeSelector));

                    addStep(builder("Generate Partition",
                                    this::insertRows,
                                    () -> operations().writes().rows(schemaSpec)
                                                      .rowCountBetween(10, 100)
                                                      .withCurrentTimestamp()
                                                      .inserts(),
                                    () -> nodeSelector));

                    addStep(builder("Generate Read",
                                    this::run,
                                    () -> operations().reads().anyRead(schemaSpec,
                                                                       modelState.primaryKeyGen(),
                                                                       modelState::clusteringKeyGen)
                                                      .build(),
                                    () -> nodeSelector));

                    addStep(builder("Generate Deletea",
                                    this::run,
                                    () -> operations().deletes().anyDelete(schemaSpec,
                                                                           modelState.primaryKeyGen(),
                                                                           modelState::clusteringKeyGen)
                                                      .build(),
                                    () -> nodeSelector));
                    // !!! Test keys that do not exist
                }
            });
        }
    }
}