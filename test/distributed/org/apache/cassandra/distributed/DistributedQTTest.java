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

package org.apache.cassandra.distributed;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.Versions;
import org.apache.cassandra.distributed.test.DistributedTestBase;
import org.apache.cassandra.quicktheories.generators.FullKey;
import org.apache.cassandra.quicktheories.generators.SchemaGen;
import org.apache.cassandra.quicktheories.generators.SchemaSpec;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.stateful.StatefulTheory;

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

    public static class PartitionGenerator implements Gen<Map<Object[], FullKey>>
    {
        public PartitionGenerator(SchemaSpec schemaSpec,
                                  List<Object[]> partitionKeys,
                                  int minPartitionSize,
                                  int maxPartitionSize) {
            schemaSpec.partitionGenerator
        }
        public Map<Object[], FullKey> generate(RandomnessSource randomnessSource)
        {
            Map<Object[], FullKey> res = new HashMap<>();

            return res;
        }
    }

    @Test
    public void quorumReadsTest() throws Throwable
    {
        Versions versions = Versions.find();

        try (AbstractCluster testCluster = Cluster.create(3);
             AbstractCluster modelCluster = UpgradeableCluster.create(1, versions.getLatest(Versions.Major.v22)))
        {
            init(testCluster);
            init(modelCluster);

            modelCluster.disableAutoCompaction(KEYSPACE);

            stateful(() -> new QuorumReadModel(KEYSPACE, modelCluster, testCluster)
                {
                    @Override
                    public void initSteps()
                    {
                        Gen<Integer> nodeSelector = SourceDSL.integers().between(1, testCluster.size());

                        addSetupStep(builder("initSchema",
                                             this::initSchema, SchemaGen.schemaDefinitionGen(DistributedTestBase.KEYSPACE))
                                     .build());
                        addSetupStep(builder("generatePartitionKeys",
                                             this::setInitialKeys, () -> SourceDSL.lists().of(pkGen()).ofSize(10000))
                                     .build());
                        addSetupStep(builder("generateClusteringKeys",
                                             this::setInitialKeys, () -> SourceDSL.lists().of(pkGen()).ofSize(10000))
                                     .build());

                        // Start with something simple: a model that would first generate a bunch of partition keys
                        // Then for each partition key, generate a partition itself
                        // After that, turn all rows into writes

                        // Instead of saving the keys here, we could just sample partition keys to the separate table;
                        // Essentially we can write a key to a separate table with a certain probability, like 1/100 and
                        // read from the

                        // !!! Test keys that do not exist

//                        addSetupStep(builder("generateInitialKeys",
//                                             wrap1(this::setInitialKeys), () -> SourceDSL.lists().of(pkGen()).ofSize(100))
//                                     .build());
//
//                        addStep(1, builder("newPk", this::savePk, this::pkGen).build());
//                        addStep(10, builder("write", this::run, this::writeGen, () -> nodeSelector).build());
//                        addStep(1, builder("delete", this::run, this::delGen, () -> nodeSelector)
//                                     .precondition(this::hasWritten)
//                                     .build());
//                        addStep(5, builder("read", this::run, this::readGen, () -> nodeSelector)
//                                     .precondition(this::hasWritten)
//                                     .build());
//                        addStep(5, builder("read2", this::run, this::clusteringRangeReadGen, () -> nodeSelector)
//                                   .precondition(this::hasWritten).build());

//                        addStep(1, builder("flush", this::flush, nodeSelector).build());
//                        addStep(1, builder("compact", this::compact, nodeSelector).build());

                    }
                });

            System.out.println("counter.get() = " + opcount.get());
        }
    }

    public static <T1> Consumer<T1> wrap1(Consumer<T1> consumer) {
        return (v) -> {
            try
            {
                System.out.println(1111);
                opcount.getAndIncrement();
                consumer.accept(v);
            }
            catch (Throwable t)
            {
                System.out.println("t; = " + t);
                t.printStackTrace();
                System.exit(0);
            }
        };
    }

    public static <T1, T2> BiConsumer<T1, T2> wrap(BiConsumer<T1, T2> consumer) {
        return (t1, t2) -> {
            try
            {
                System.out.println(1111);
                opcount.getAndIncrement();
                consumer.accept(t1, t2);
            }
            catch (Throwable t)
            {
                System.out.println("t; = " + t);
                t.printStackTrace();
                System.exit(0);
            }
        };
    }
    public static Runnable wrap(Runnable runnable)
    {
        return () -> {
            try
            {
                System.out.println(1111);
                opcount.getAndIncrement();
                runnable.run();
            }
            catch (Throwable t)
            {
                System.out.println("t; = " + t);
                t.printStackTrace();
                System.exit(0);
            }
        };
    }
//    @Test
//    public void repairTest() throws Throwable
//    {
//        try (TestCluster testCluster = createCluster(0, 3);
//             TestCluster modelCluster = createCluster(1, 1))
//        {
//            modelCluster.disableAutoCompaction(KEYSPACE);
//
//            qt().withMinStatefulSteps(1000)
//                .withMaxStatefulSteps(5000)
//                .stateful(() -> new QuorumReadModel(KEYSPACE, modelCluster, testCluster)
//                {
//                    public void initSteps()
//                    {
//                        Gen<Integer> nodeSelector = SourceDSL.integers().between(1, testCluster.size());
//
//                        addSetupStep(builder("initSchema",
//                                             this::initSchema, SchemaGen.schemaDefinitionGen(DistributedTestBase.KEYSPACE))
//                                     .build());
//                        addSetupStep(builder("generateInitialKeys",
//                                             this::setInitialKeys, () -> SourceDSL.lists().of(pkGen()).ofSize(100))
//                                     .build());
//
//                        addSetupStep(builder("initSchema",
//                                             this::initSchema, SchemaGen.schemaDefinitionGen(DistributedTestBase.KEYSPACE))
//                                     .build());
//                        addSetupStep(builder("generateInitialKeys",
//                                             this::setInitialKeys, () -> SourceDSL.lists().of(pkGen()).ofSize(100))
//                                     .build());
//
//                        addStep(1, builder("newPk", this::savePk, this::pkGen).build());
//                        addStep(100, builder("write", this::run, this::writeGen, () -> nodeSelector).build());
//                        addStep(10, builder("delete", this::run, this::delGen, () -> nodeSelector).precondition(this::hasWritten).build());
//                    }
//                });
//        }
//    }

//    @Test
//    public void readsWithTimeouts() throws Throwable
//    {
//        try (TestCluster testCluster = createCluster(0, 3);
//             TestCluster modelCluster = createCluster(1, 1))
//        {
//            qt().withMinStatefulSteps(1000)
//                .withMaxStatefulSteps(10000)
//                .forAll(StatefulCore.generator(() -> {
//                    return new QuorumReadModel(KEYSPACE, modelCluster, testCluster)
//                    {
//                        public void initSteps()
//                        {
//                            Gen<Integer> nodeSelector = SourceDSL.integers().between(1, 3);
//
//                            addStep(100, suppressException(step("write", this::run, this::writeGen, () -> nodeSelector), WriteTimeoutException.class));
//                            addStep(10, suppressException(step("delete", this::hasWritten, this::run, this::delGen, () -> nodeSelector), WriteTimeoutException.class));
//                            addStep(100, ensure(step("read", this::hasWritten, this::run, this::readGen, () -> nodeSelector), this::restoreNetwork));
//                            addStep(10, step("inject_failure", this::injectFailure, nodeSelector));
//                            addStep(1, step("flush", this::flush, nodeSelector));
//                            addStep(1, step("compact", this::compact, nodeSelector));
//                        }
//                    };
//                }))
//                .checkAssert(StatefulCore::run);
//        }
//    }

    // TODO: TEST REPAIR
}
