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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.quicktheories.generators.SourceDSL.*;

public class WritesDSL
{

    /**
     * Generate a write to the given partition (randomly generated row and values)
     * @param schema the schema to generate writes for
     * @param partitionKey the partition key to write to
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public static WriteBuilder write(SchemaSpec schema, Object[] partitionKey)
    {
        return write(schema, Generate.constant(partitionKey));
    }

    /**
     * Generate a random write
     * @param schema the schema to generate writes for
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public static WriteBuilder write(SchemaSpec schema)
    {
        return write(schema, schema.partitionKeyGenerator);
    }


    /**
     * Generate a random write
     * @param schema the schema to generate writes for
     * @param partitionKeys the generator to use when generating partition keys
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public static WriteBuilder write(SchemaSpec schema, Gen<Object[]> partitionKeys)
    {
        return new WriteBuilder(schema, partitionKeys);
    }

    /**
     * Generate multiple writes to a single partition
     * @param schemaSpec the schema to generate writes for
     * @param partitionKey the partition to generates writes to
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder writes(SchemaSpec schemaSpec, Object[] partitionKey)
    {
        return new WritesBuilder(schemaSpec, Generate.constant(partitionKey), schemaSpec.clusteringKeyGenerator);
    }

    /**
     * Generate writes to one or more partitions
     * @param schemaSpec the schema to generate writes for
     * @param partitionKeys the generator to use when generating partition keys
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder writes(SchemaSpec schemaSpec, Gen<Object[]> partitionKeys)
    {
        return new WritesBuilder(schemaSpec, partitionKeys, schemaSpec.clusteringKeyGenerator);
    }

    /**
     * Generate writes to one or more partitions
     * @param schemaSpec the schema to generate writes for
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder writes(SchemaSpec schemaSpec)
    {
        return new WritesBuilder(schemaSpec);
    }


    public static class WriteBuilder {
        private final Gen<DataRow> rows;

        private WriteBuilder(SchemaSpec schema, Gen<Object[]> partitionKeys)
        {
            this(rowGenerator(schema, partitionKeys, schema.clusteringKeyGenerator));
        }

        private WriteBuilder(Gen<DataRow> rows)
        {
            this.rows = rows;
        }

        public Gen<Pair<FullKey, Insert>> withTimestamp(Gen<Long> timestamps)
        {
            return this.rows.zip(timestamps, (r, ts) -> r.toInsert(ts));
        }

        public Gen<Pair<FullKey, Insert>> withTimestamp(long ts)
        {
            return withTimestamp(arbitrary().constant(ts));
        }


        public Gen<Pair<FullKey, Insert>> withCurrentTimestamp()
        {
            return withTimestamp(FBUtilities.timestampMicros());
        }
    }

    public static class WritesBuilder {
        private final SchemaSpec schema;
        private final Gen<Object[]> partitionKeys;
        private final Gen<Object[]> clusteringKeys;

        private int minPartitions = 1;
        private int maxPartitions = 1;
        private int minRows = 1;
        private int maxRows = 1;

        private WritesBuilder(SchemaSpec schema)
        {
            this(schema, schema.partitionKeyGenerator, schema.clusteringKeyGenerator);
        }

        private WritesBuilder(SchemaSpec schema,
                              Gen<Object[]> partitionKeys,
                              Gen<Object[]> clusteringKeys)
        {
            this.schema = schema;
            this.partitionKeys = partitionKeys;
            this.clusteringKeys = clusteringKeys;
        }

        public WritesBuilder partitionCount(int count)
        {
            return partitionCountBetween(count, count);
        }

        public WritesBuilder partitionCountBetween(int min, int max)
        {
            assert min > 0 : "Minimum partition count should be non-negative but was " + min;
            assert min <= max : "Minimum partition count not exceed maximum partition count";

            minPartitions = min;
            maxPartitions = max;
            return this;
        }

        public WritesBuilder rowCount(int count)
        {
            return rowCountBetween(count, count);
        }

        public WritesBuilder rowCountBetween(int min, int max)
        {
            assert min > 0 : "Minimum row count should be non-negative but was " + min;
            assert min <= max : "Minimum row count not exceed maximum partition count";

            minRows = min;
            maxRows = max;
            return this;
        }

        public Gen<List<Pair<FullKey, Insert>>> withTimestamp(Gen<Long> timestamps)
        {
            Gen<List<Pair<FullKey, Insert>>> singlePartitionManyRows =
            partitionKeys.zip(clusteringKeys,
                              Pair::create)
                         .flatMap(p ->  lists().of(rowGenerator(schema, p.left, p.right).zip(timestamps, DataRow::toInsert))
                                               .ofSizeBetween(minRows, maxRows));

            return lists().of(singlePartitionManyRows)
                          .ofSizeBetween(minPartitions, maxPartitions)
                          .map(writes -> writes.stream().reduce(new ArrayList<>(), (a, b) -> { a.addAll(b); return a; } ));

        }

        public Gen<List<Pair<FullKey, Insert>>> withTimestamp(long ts)
        {
            return withTimestamp(arbitrary().constant(ts));
        }


        public Gen<List<Pair<FullKey, Insert>>> withCurrentTimestamp()
        {
            return withTimestamp(in -> FBUtilities.timestampMicros());
        }

    }

    /**
     * Auxilitary intermediate row representation
     */
    private static class DataRow
    {
        private final SchemaSpec schemaSpec;
        private final FullKey fullKey;
        private final List<Pair<ColumnSpec<?>, Object>> rowData;

        public DataRow(SchemaSpec schemaSpec,
                       FullKey fullKey,
                       List<Pair<ColumnSpec<?>, Object>> rowData)
        {
            this.schemaSpec = schemaSpec;
            this.fullKey = fullKey;
            this.rowData = rowData;
        }

        public Pair<FullKey, Insert> toInsert(long ts)
        {
            Insert insert = QueryBuilder.insertInto(schemaSpec.ksName, schemaSpec.tableName);

            for (int i = 0; i < schemaSpec.partitionKeys.size(); i++)
            {
                insert.value(schemaSpec.partitionKeys.get(i).name,
                             fullKey.partition[i]);
            }

            for (int i = 0; i < schemaSpec.clusteringKeys.size(); i++)
            {
                insert.value(schemaSpec.clusteringKeys.get(i).name,
                             fullKey.clustering[i]);
            }


            for (Pair<ColumnSpec<?>, Object> row : rowData)
            {
                insert.value(row.left.name,
                             row.right);
            }

            insert.using(timestamp(ts));
            return Pair.create(fullKey, insert);
        }

    }


    private static Gen<DataRow> rowGenerator(SchemaSpec schema, Gen<Object[]> pkGen, Gen<Object[]> ckGen) {
        return pkGen.zip(ckGen, schema.rowDataGenerator,
                         (pk, ck, data) -> new DataRow(schema, new FullKey(pk, ck), data));
    }


    private static Gen<DataRow> rowGenerator(SchemaSpec schema, Object[] pk, Object[] ck) {
        return schema.rowDataGenerator.map((data) -> new DataRow(schema, new FullKey(pk, ck), data));
    }

}
