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

import static org.quicktheories.QuickTheory.qt;

public class SchemaGenTest extends CQLTester
{
    @Test
    public void createTableRoundTrip()
    {
        qt().withExamples(10).forAll(SchemaGen.schemaDefinitionGen(KEYSPACE))
            .check(schemaDefinition -> {
                String tableDef = schemaDefinition.toCQL();

                createTable(KEYSPACE, tableDef);

                TableMetadata tableMetadata = Keyspace.open(KEYSPACE).getColumnFamilyStore(schemaDefinition.tableName).metadata.get();
                compareColumns(schemaDefinition.partitionKeys, tableMetadata.partitionKeyColumns());
                compareColumns(schemaDefinition.clusteringKeys, tableMetadata.clusteringColumns());
                compareColumns(schemaDefinition.regularColumns, tableMetadata.regularColumns());
                compareColumns(schemaDefinition.staticColumns, tableMetadata.staticColumns());
                return true;
            });
    }

    public static boolean compareColumns(Collection<ColumnSpec<?>> expectedColl, Collection<ColumnMetadata> actualColl)
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
