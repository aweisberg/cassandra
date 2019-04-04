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

import java.util.Collection;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.schemas;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class SchemaGenTest extends CQLTester
{
    @Test
    public void createTableRoundTrip()
    {
        qt().withExamples(10000).forAll(schemas().keyspace(KEYSPACE).partitionKeyColumnCount(1, 10)
                                              .clusteringColumnCount(0, 10)
                                              .staticColumnCount(0, 10)
                                              .regularColumnCount(0, 10)
                                              .build())
            .check(schemaDefinition -> {
                String tableDef = schemaDefinition.compile().cql();

                createTable(KEYSPACE, tableDef);

                TableMetadata tableMetadata = Keyspace.open(KEYSPACE).getColumnFamilyStore(schemaDefinition.tableName).metadata.get();
                compareColumns(schemaDefinition.partitionKeys, tableMetadata.partitionKeyColumns());
                compareColumns(schemaDefinition.clusteringKeys, tableMetadata.clusteringColumns());
                compareColumns(schemaDefinition.regularColumns, tableMetadata.regularColumns());
                compareColumns(schemaDefinition.staticColumns, tableMetadata.staticColumns());
                return true;
            });
    }

    @Test
    public void testSchemaGeneration()
    {
        Gen<Pair<Integer, Integer>> min0 = integers().between(0, 4).zip(integers().between(0, 6), Pair::create);
        Gen<Pair<Integer, Integer>> min1 = integers().between(1, 4).zip(integers().between(0, 6), Pair::create);
        Gen<SchemaGenerationInputs> inputs = min1.zip(min0, min0, min1, (pks, cks, statics, regs) ->
                                                                        new SchemaGenerationInputs(pks.left, pks.left + pks.right,
                                                                                                   cks.left, cks.left + cks.right,
                                                                                                   statics.left, statics.left + statics.right,
                                                                                                   regs.left, regs.left + regs.right));

        Gen<Pair<SchemaGenerationInputs, SchemaSpec>> schemaAndInputs = inputs.flatMap(input -> schemas().keyspace("test")
                                                                                                         .partitionKeyColumnCount(input.minPk, input.maxPk)
                                                                                                         .clusteringColumnCount(input.minCks, input.maxCks)
                                                                                                         .staticColumnCount(input.minStatics, input.maxStatics)
                                                                                                         .regularColumnCount(input.minRegs, input.maxRegs)
                                                                                                         .build()
                                                                                                         .map(schema -> Pair.create(input, schema)));
        qt().forAll(schemaAndInputs)
            .check(schemaAndInput -> {
                SchemaGenerationInputs input = schemaAndInput.left;
                SchemaSpec schema = schemaAndInput.right;

                return schema.partitionKeys.size() <= input.maxPk && schema.partitionKeys.size() >= input.minPk &&
                       schema.clusteringKeys.size() <= input.maxCks && schema.clusteringKeys.size() >= input.minCks &&
                       schema.staticColumns.size() <= input.maxStatics && schema.staticColumns.size() >= input.minStatics &&
                       schema.regularColumns.size() <= input.maxRegs && schema.regularColumns.size() >= input.minRegs;
            });
    }


    private static class SchemaGenerationInputs {
        private final int minPk;
        private final int maxPk;
        private final int minCks;
        private final int maxCks;
        private final int minStatics;
        private final int maxStatics;
        private final int minRegs;
        private final int maxRegs;

        public SchemaGenerationInputs(int minPk, int maxPk, int minCks, int maxCks,
                                      int minStatics, int maxStatics, int minRegs, int maxRegs)
        {
            this.minPk = minPk;
            this.maxPk = maxPk;
            this.minCks = minCks;
            this.maxCks = maxCks;
            this.minStatics = minStatics;
            this.maxStatics = maxStatics;
            this.minRegs = minRegs;
            this.maxRegs = maxRegs;
        }
    }

    private static boolean compareColumns(Collection<ColumnSpec<?>> expectedColl, Collection<ColumnMetadata> actualColl)
    {
        Iterator<ColumnSpec<?>> expectedIter = expectedColl.iterator();
        Iterator<ColumnMetadata> actualIter = actualColl.iterator();

        while (expectedIter.hasNext() && actualIter.hasNext())
        {
            ColumnSpec expected = expectedIter.next();
            ColumnMetadata actual = actualIter.next();

            Assert.assertSame(expected.kind, actual.kind);
            Assert.assertEquals(expected.name, actual.name.toString());
            Assert.assertEquals(expected.type, actual.type);
        }

        Assert.assertEquals(String.format("Collections %s and %s have different sizes", expectedColl, actualColl),
                            expectedIter.hasNext(), actualIter.hasNext());
        return true;
    }

}


