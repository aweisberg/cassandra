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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.test.DistributedTestBase;
import org.apache.cassandra.quicktheories.generators.ReadsDSL;
import org.apache.cassandra.quicktheories.generators.SchemaSpec;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.stateful.StatefulTheory;

import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.operations;
import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.schemas;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.impl.stateful.StatefulTheory.builder;

public class DistributedQTTest extends DistributedTestBase
{
    private static AtomicInteger opcount = new AtomicInteger();

    private void stateful(Supplier<StatefulTheory<StatefulTheory.Step>> supplier)
    {
        qt().withExamples(500)
            .withMinStatefulSteps(10000)
            .withMaxStatefulSteps(100000)
            .stateful(supplier::get);
    }

    private static final Gen<SchemaSpec> testSchemas = schemas().keyspace(KEYSPACE)
                                                                .partitionKeyColumnCount(1, 4)
                                                                .clusteringColumnCount(0, 4)
                                                                .staticColumnCount(0, 4)
                                                                .regularColumnCount(0, 4)
                                                                .build();


    // TODO: this is tmp
    private static <T> Gen<Pair<SchemaSpec, List<T>>> pairWithSchema(Function<SchemaSpec, Gen<T>> fn)
    {
        return testSchemas.flatMap(schema -> {
            return SourceDSL.lists()
                            .of(fn.apply(schema)).ofSizeBetween(10, 100)
                            .map(item -> Pair.create(schema, item));
        });
    }

    @Test
    public void readTest() throws Throwable
    {
        Function<SchemaSpec, Gen<ReadsDSL.Select>> reads = schema -> operations()
                                                                     .reads()
                                                                     .partitionRead(schema)
//                                                                     .withColumnSelection()
                                                                     .withOrder()
                                                                     .build();

        try (AbstractCluster testCluster = init(Cluster.create(1)))
        {
            qt().forAll(pairWithSchema(reads))
                .check(p -> {
                    testCluster.schemaChange(p.left.toCQL());
                    for (ReadsDSL.Select select : p.right)
                    {
                        Pair<String, Object[]> compiled = select.compile();
                        testCluster.coordinator(1).execute(compiled.left,
                                                           ConsistencyLevel.ALL,
                                                           compiled.right);
                    }
                    return true;
                });
        }
    }

    @Test
    public void simpleTest() throws Throwable
    {
        try (AbstractCluster testCluster = Cluster.create(3);
             AbstractCluster modelCluster = Cluster.create(1))
        {
            init(testCluster);
            init(modelCluster);

            modelCluster.disableAutoCompaction(KEYSPACE);

            stateful(() -> new StatefulModel(new InMemoryModel(),
                                             testCluster,
                                             modelCluster)
            {
                @Override
                public void initSteps()
                {
                    addSetupStep(builder("initSchema",
                                         this::initSchema,
                                         schemas().keyspace(KEYSPACE)
                                                  .partitionKeyColumnCount(1, 5)
                                                  .clusteringColumnCount(0, 5)
                                                  .staticColumnCount(0, 5)
                                                  .regularColumnCount(0, 5)
                                                  .build())
                                 .build());
                    addSetupStep(builder("generatePartitionKeys",
                                         this::insertRows,
                                         () -> operations().writes().writes(schemaSpec)
                                                           .partitionCountBetween(1, 100)
                                                           .rowCountBetween(10, 100)
                                                           .withCurrentTimestamp(),
                                         () -> nodeSelector)
                                 .build());
                    addStep(builder("generateRead",
                                    this::run,
                                    this::generateSelect,
                                    () -> nodeSelector)
                                 .build());

                    // !!! Test keys that do not exist
                }
            });
        }
    }
}