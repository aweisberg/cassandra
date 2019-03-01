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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.*;
import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.*;

public class GeneratorsTest
{
    private static final Gen<SchemaSpec> testSchemas = schemas().keyspace("test")
                                                         .partitionKeyColumnCount(1, 4)
                                                         .clusteringColumnCount(0, 4)
                                                         .staticColumnCount(0)
                                                         .regularColumnCount(1)
                                                         .build();


    private static <T> Gen<Pair<SchemaSpec, T>> pairWithSchema(Function<SchemaSpec, Gen<T>> fn)
    {
        return testSchemas.flatMap(schema -> fn.apply(schema).map(item -> Pair.create(schema, item)));
    }

    @Test
    public void partitionDeleteOnlyGeneration()
    {
        Function<SchemaSpec, Gen<Delete>>deletes = schema -> operations().deletes()
                                                                         .deletePartition(schema,
                                                                                          data().partitionKeys(schema))
                                                                         .withCurrentTimestamp();

        // TODO (jwest): the way this test splits strings and validates the query is fragile
        qt().forAll(pairWithSchema(deletes))
            .check(deleteAndSchema -> {
                int clauseCount = 0;
                for (String clause : extractClauses(deleteAndSchema.right)) {
                    clauseCount++;
                    if (!clause.matches("pk.*=.*"))
                        return false;
                }

                return clauseCount == deleteAndSchema.left.partitionKeys.size();
            });
    }

    @Test
    public void pointDeleteOnlyGeneration()
    {
        Function<SchemaSpec, Gen<Delete>> deletes = schema -> operations().deletes()
                                                                          .delete(schema,
                                                                                  data().partitionKeys(schema),
                                                                                  data().clusterings(schema))
                                                                          .pointDeletesOnly()
                                                                          .withCurrentTimestamp();

        qt().forAll(pairWithSchema(deletes))
            .check(deleteAndSchema -> {
                int clauseCount = 0;
                for (String clause : extractClauses(deleteAndSchema.right)) {
                    clauseCount++;
                    if (clause.matches("pk.*=.*"))
                        continue;

                    if (clause.matches("ck.*=.*"))
                        continue;

                    return false;
                }

                SchemaSpec schema = deleteAndSchema.left;
                return clauseCount == schema.partitionKeys.size() + schema.clusteringKeys.size();
            });
    }

    @Test
    public void rangeDeleteOnlyGeneration()
    {
        Function<SchemaSpec, Gen<Delete>> deletes = schema ->
                                                    operations().deletes()
                                                                .delete(schema, data().partitionKeys(schema), data().clusterings(schema))
                                                                .rangeDeletesOnly()
                                                                .withCurrentTimestamp();

        qt().forAll(pairWithSchema(deletes).assuming(pair -> pair.left.clusteringKeys.size() > 0))
            .check(deleteAndSchema -> {
                SchemaSpec schema = deleteAndSchema.left;
                int count = schema.partitionKeys.size() + schema.clusteringKeys.size();

                int clauseCount = 0;
                boolean sawBound = false;
                for (String clause : extractClauses(deleteAndSchema.right)) {
                    clauseCount++;
                    if (clause.matches("ck.*(<|>).*"))
                    {
                        sawBound = true;
                        continue;
                    }

                    if (clause.matches("pk.*=.*"))
                        continue;

                    if (clause.matches("ck.*=.*"))
                        continue;

                    return false;
                }

                return sawBound || clauseCount < count;
            });
    }

    @Test
    public void mixedDeletesAllGenerated()
    {
        AtomicInteger partitionOnly = new AtomicInteger();
        AtomicInteger rowDelete = new AtomicInteger();
        AtomicInteger rangeDelete = new AtomicInteger();

        Function<SchemaSpec, Gen<Delete>> deletes = schema ->
                                                    operations().deletes()
                                                                .delete(schema, data().partitionKeys(schema), data().clusterings(schema))
                                                                .withCurrentTimestamp();


        qt().forAll(pairWithSchema(deletes).assuming(pair -> pair.left.clusteringKeys.size() > 0))
            .check(deleteAndSchema -> {
                SchemaSpec schema = deleteAndSchema.left;
                int count = schema.partitionKeys.size() + schema.clusteringKeys.size();

                int clauseCount = 0;
                boolean sawClustering = false;
                boolean sawBound = false;
                for (String clause : extractClauses(deleteAndSchema.right))
                {
                    clauseCount++;
                    if (clause.matches("ck.*(<|>).*"))
                    {
                        sawClustering = true;
                        sawBound = true;
                        continue;
                    }

                    if (clause.matches("pk.*=.*"))
                        continue;

                    if (clause.matches("ck.*=.*"))
                    {
                        sawClustering = true;
                        continue;
                    }

                    return false;
                }

                if (!sawClustering)
                    partitionOnly.incrementAndGet();
                else if (sawClustering && !sawBound && clauseCount == count)
                    rowDelete.incrementAndGet();
                else if (sawClustering && (sawBound || clauseCount < count))
                    rangeDelete.incrementAndGet();


                return true;
            });

        Assert.assertTrue("no partition deletes", partitionOnly.get() > 0);
        Assert.assertTrue("no row deletes", rowDelete.get() > 0);
        Assert.assertTrue("no range deletes", rangeDelete.get() > 0);
    }

    @Test
    public void manyWritesToSinglePartition()
    {
        qt().forAll(testSchemas.flatMap(schema -> operations().writes().writes(schema).partitionCount(1).rowCountBetween(1, 10).withCurrentTimestamp()))
            .check(writes -> {
                FullKey last = null;
                for (Pair<FullKey, Insert> write : writes)
                {
                    if (last == null)
                    {
                        last = write.left;
                        continue;
                    }

                    if (!last.partition.equals(write.left.partition))
                        return false;
                }

                return true;
            });

    }

    private String[] extractClauses(Delete delete)
    {
        return extractWhere(delete).split(" AND ");
    }

    private String extractWhere(Delete delete)
    {
        String qs = delete.toString();
        return qs.split(" WHERE ")[1];
    }

}
