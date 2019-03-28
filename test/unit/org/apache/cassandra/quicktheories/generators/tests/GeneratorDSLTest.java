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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.test.DistributedTestBase;
import org.apache.cassandra.quicktheories.generators.CompiledStatement;
import org.apache.cassandra.quicktheories.generators.DeletesDSL;
import org.apache.cassandra.quicktheories.generators.ReadsDSL;
import org.apache.cassandra.quicktheories.generators.SchemaSpec;
import org.apache.cassandra.quicktheories.generators.WritesDSL;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.operations;
import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.schemas;
import static org.quicktheories.QuickTheory.qt;

public class GeneratorDSLTest extends DistributedTestBase
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


        try (AbstractCluster testCluster = init(Cluster.create(1)))
        {
            qt().withShrinkCycles(0)
                .forAll(pairWithSchema(toBuilder))
                .checkAssert(p -> {
                    testCluster.schemaChange(p.left.toCQL());

                    for (ReadsDSL.Select select : p.right)
                    {
                        CompiledStatement compiled = select.compile();
                        testCluster.coordinator(1).execute(compiled.cql(),
                                                           ConsistencyLevel.ALL,
                                                           compiled.bindings());
                    }
                });
        }
    }

    @Test
    public void insertDSLTest() throws Throwable
    {
        writesDSLTest(true);
    }

    @Test
    public void updateDSLTest() throws Throwable
    {
        writesDSLTest(false);
    }

    private void writesDSLTest(boolean insert) throws Throwable
    {
        Function<SchemaSpec, Gen<WritesDSL.DataRow>> makeBuilder =
        (spec) -> SourceDSL.booleans().all().zip(SourceDSL.booleans().all(),
                                                 (withTimestamp, withTTL) -> {
                                                     WritesDSL.WriteBuilder builder = operations().writes().row(spec);

                                                     if (withTimestamp)
                                                         builder.withTimestamp(SourceDSL.longs().between(1, Long.MAX_VALUE - 1));
                                                     if (withTTL)
                                                         builder.withTTL(SourceDSL.integers().between(1, (int) TimeUnit.DAYS.toSeconds(365)));

                                                     return builder;
                                                 }).flatMap(builder -> insert ? builder.insert() : builder.update());

        try (AbstractCluster testCluster = init(Cluster.create(1)))
        {
            qt().withShrinkCycles(0)
                .forAll(pairWithSchema(makeBuilder))
                .checkAssert(p -> {
                    testCluster.schemaChange(p.left.toCQL());

                    for (WritesDSL.DataRow select : p.right)
                    {
                        CompiledStatement compiled = select.compile();
                        testCluster.coordinator(1).execute(compiled.cql(),
                                                           ConsistencyLevel.ALL,
                                                           compiled.bindings());
                    }
                });
        }
    }

    @Test
    public void deletesDSLTest() throws Throwable
    {
        List<Function<SchemaSpec, DeletesDSL.DeletesBuilder>> deleteBuilders = Arrays.asList(operations().deletes()::partitionDelete,
                                                                                             operations().deletes()::rowDelete,
                                                                                             operations().deletes()::rowSliceDelete,
                                                                                             operations().deletes()::rowRangeDelete);

        Function<SchemaSpec, Gen<DeletesDSL.Delete>> toBuilder =
        (spec) -> SourceDSL.arbitrary().pick(deleteBuilders)
                           .zip(SourceDSL.booleans().all(),
                                SourceDSL.booleans().all(),
                                (fn, withColumns, withTimestamp) -> {
                                    DeletesDSL.DeletesBuilder builder = fn.apply(spec);
                                    if (withColumns && builder.deleteType() == DeletesDSL.DeleteType.SINGLE_ROW)
                                        builder.deleteColumns();
                                    if (withTimestamp)
                                        builder.withTimestamp(SourceDSL.longs().between(1, Long.MAX_VALUE - 1));
                                    return builder;
                                })
                           .flatMap(DeletesDSL.DeletesBuilder::build);

        try (AbstractCluster testCluster = init(Cluster.create(1)))
        {
            qt().withShrinkCycles(0)
                .forAll(pairWithSchema(toBuilder))
                .checkAssert(p -> {
                    testCluster.schemaChange(p.left.toCQL());

                    for (DeletesDSL.Delete delete : p.right)
                    {
                        CompiledStatement compiled = delete.compile();
                        testCluster.coordinator(1).execute(compiled.cql(),
                                                           ConsistencyLevel.ALL,
                                                           compiled.bindings());
                    }
                });
        }


    }
}