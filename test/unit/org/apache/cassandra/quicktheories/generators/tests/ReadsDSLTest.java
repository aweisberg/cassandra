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

package org.apache.cassandra.quicktheories.generators.tests;

import java.util.Arrays;
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
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.operations;
import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.schemas;
import static org.quicktheories.QuickTheory.qt;

public class ReadsDSLTest extends DistributedTestBase
{
    private static final Gen<SchemaSpec> testSchemas = schemas().keyspace(KEYSPACE)
                                                                .partitionKeyColumnCount(1, 4)
                                                                .clusteringColumnCount(1, 4)
                                                                .staticColumnCount(0, 4)
                                                                .regularColumnCount(1, 4)
                                                                .build();


    private static <T> Gen<Pair<SchemaSpec, List<T>>> pairWithSchema(Function<SchemaSpec, Gen<T>> fn)
    {
        return testSchemas.flatMap(schema -> {
            return SourceDSL.lists()
                            .of(fn.apply(schema)).ofSizeBetween(10, 100)
                            .map(item -> Pair.create(schema, item));
        });
    }

    // Makes sure that we generally generate correct CQL, withouot regard to its semantics
    @Test
    public void readDSLTest() throws Throwable
    {
        List<Function<SchemaSpec, ReadsDSL.ReadsBuilder>> readBuilders = Arrays.asList(operations().reads()::partitionRead,
                                                                                       operations().reads()::rowRead,
                                                                                       operations().reads()::rowSlice,
                                                                                       operations().reads()::rowRange);

        Function<SchemaSpec, Gen<ReadsDSL.Select>> toBuilder =
        (spec) -> SourceDSL.arbitrary().pick(readBuilders)
                           .zip(SourceDSL.booleans().all(),
                                SourceDSL.booleans().all(),
                                SourceDSL.booleans().all(),
                                (fn, withSelection, withLimit, withOrder) -> {
                                    ReadsDSL.ReadsBuilder builder = fn.apply(spec);
                                    if (withSelection)
                                        builder.withColumnSelection();
                                    if (withLimit)
                                        builder.withColumnSelection();
                                    if (withOrder)
                                        builder.withOrder();
                                    return builder;
                                })
                           .flatMap(ReadsDSL.ReadsBuilder::build);

        AtomicInteger a = new AtomicInteger();
        try (AbstractCluster testCluster = init(Cluster.create(1)))
        {
            qt().withShrinkCycles(0)
                .forAll(pairWithSchema(toBuilder))
                .checkAssert(p -> {
                    testCluster.schemaChange(p.left.toCQL());

                    for (ReadsDSL.Select select : p.right)
                    {
                        Pair<String, Object[]> compiled = select.compile();
                        testCluster.coordinator(1).execute(compiled.left,
                                                           ConsistencyLevel.ALL,
                                                           compiled.right);
                    }
                });
        }
    }
}
