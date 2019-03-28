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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.querybuilder.Delete;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;

import static org.quicktheories.QuickTheory.qt;
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
    public void manyWritesToSinglePartition()
    {
        qt().forAll(testSchemas.flatMap(schema -> operations().writes().writes(schema).partitionCount(1).rowCountBetween(1, 10).withCurrentTimestamp().inserts()))
            .check(writes -> {
                FullKey last = null;
                for (WritesDSL.Insert write : writes)
                {
                    if (last == null)
                    {
                        last = write.key();
                        continue;
                    }

                    if (!Arrays.equals(last.partition, write.key().partition))
                        return false;
                }

                return true;
            });

    }
}
