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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.querybuilder.Delete;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.*;
import static org.apache.cassandra.quicktheories.generators.CassandraGenDSL.*;

public class GeneratorsTest
{
    private final Gen<SchemaSpec> schemasForDeletion = schemas().keyspace("test")
                                                                .partitionKeyColumnCountBetween(1, 4)
                                                                .clusteringColumnCountBetween(0, 4)
                                                                .staticColumnCount(0)
                                                                .regularColumnCount(1);


    private Gen<Pair<SchemaSpec, Delete>> pairWithSchema(Function<SchemaSpec, Gen<Delete>> fn)
    {
        return schemasForDeletion.flatMap(schema -> fn.apply(schema).map(delete -> Pair.create(schema, delete)));
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
                                                                                                         .partitionKeyColumnCountBetween(input.minPk, input.maxPk)
                                                                                                         .clusteringColumnCountBetween(input.minCks, input.maxCks)
                                                                                                         .staticColumnCountBetween(input.minStatics, input.maxStatics)
                                                                                                         .regularColumnCountBetween(input.minRegs, input.maxRegs).map(schema -> Pair.create(input, schema)));
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

    @Test
    public void partitionDeleteOnlyGeneration()
    {
        Function<SchemaSpec, Gen<Delete>>deletes = schema -> operations().writes()
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
        Function<SchemaSpec, Gen<Delete>> deletes = schema -> operations().writes()
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
                                                    operations().writes()
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
                                                    operations().writes()
                                                                .delete(schema, data().partitionKeys(schema), data().clusterings(schema))
                                                                .withCurrentTimestamp();


        qt().forAll(pairWithSchema(deletes).assuming(pair -> pair.left.clusteringKeys.size() > 0))
            .check(deleteAndSchema -> {
                SchemaSpec schema = deleteAndSchema.left;
                int count = schema.partitionKeys.size() + schema.clusteringKeys.size();

                int clauseCount = 0;
                boolean sawClustering = false;
                boolean sawBound = false;
                for (String clause : extractClauses(deleteAndSchema.right)) {
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
