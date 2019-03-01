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

import java.util.function.Supplier;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.utils.FBUtilities;
import org.quicktheories.core.Gen;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.integers;

public class DeletesDSL
{

    /**
     * Generate a single partition delete
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator used when generating which partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder deletePartition(SchemaSpec schema,
                                          Gen<Object[]> partitionKeys)
    {
        return deletePartition(schema, () -> partitionKeys);
    }

    /**
     * Generate a single partition delete
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator supplier used when generating which partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder deletePartition(SchemaSpec schema,
                                          Supplier<Gen<Object[]>> partitionKeys)
    {
        return delete(schema, partitionKeys, () -> arbitrary().constant(new Object[]{})).partitionDeletesOnly();
    }


    /**
     * Generate a delete within a partition. Deletes can be point or range deletes depending on further customization.
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator used when generating which partition to delete
     * @param clusterings a generator used when generating which rows/ranges within a partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder delete(SchemaSpec schema,
                                 Gen<Object[]> partitionKeys,
                                 Gen<Object[]> clusterings)
    {
        return delete(schema, () -> partitionKeys, () -> clusterings);
    }

    /**
     * Generate a delete within a partition. Deletes can be point or range deletes depending on further customization.
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator supplier used when generating which partition to delete
     * @param clusterings a generator used when generating which rows/ranges within a partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder delete(SchemaSpec schema,
                                 Supplier<Gen<Object[]>> partitionKeys,
                                 Gen<Object[]> clusterings)
    {
        return delete(schema, partitionKeys, () -> clusterings);
    }

    /**
     * Generate a delete within a partition. Deletes can be point or range deletes depending on further customization.
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator used when generating which partition to delete
     * @param clusterings a generator supplier used when generating which rows/ranges within a partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder delete(SchemaSpec schema,
                                 Gen<Object[]> partitionKeys,
                                 Supplier<Gen<Object[]>> clusterings)
    {
        return delete(schema, () -> partitionKeys, clusterings);
    }

    /**
     * Generate a delete within a partition. Deletes can be point or range deletes depending on further customization.
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator supplier used when generating which partition to delete
     * @param clusterings a generator supplier used when generating which rows/ranges within a partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder delete(SchemaSpec schema,
                                 Supplier<Gen<Object[]>> partitionKeys,
                                 Supplier<Gen<Object[]>> clusterings)
    {
        return new DeletesBuilder(schema, partitionKeys, clusterings);
    }


    public static class DeletesBuilder
    {
        private final SchemaSpec schema;
        private final Supplier<Gen<Object[]>> partitionKeys;
        private final Supplier<Gen<Object[]>> clusterings;

        private boolean rowPointDeletes = true;
        private boolean rowRangeDeletes = true;


        public DeletesBuilder(SchemaSpec schema, Supplier<Gen<Object[]>> partitionKeys, Supplier<Gen<Object[]>> clusterings)
        {
            this.schema = schema;
            this.partitionKeys = partitionKeys;
            this.clusterings = clusterings;
        }

        public DeletesBuilder partitionDeletesOnly()
        {
            rowRangeDeletes = false;
            rowPointDeletes = false;
            return this;
        }

        public DeletesBuilder pointDeletesOnly()
        {
            rowRangeDeletes = false;
            rowPointDeletes = true;
            return this;
        }

        public DeletesBuilder rangeDeletesOnly()
        {
            rowPointDeletes = false;
            rowRangeDeletes = true;
            return this;
        }

        public DeletesBuilder pointOrRangeDeletes()
        {
            rowPointDeletes = rowRangeDeletes = true;
            return this;
        }


        public Gen<Delete> withCurrentTimestamp()
        {
            return withTimestamp(FBUtilities.timestampMicros());
        }

        // TODO (jwest): add version that takes timestamp generator
        // TODO (jwest): borrowed heavily from QueryGen.deleteGen with a few minor changes
        public Gen<Delete> withTimestamp(long ts)
        {
            return in -> {
                Gen<Object[]> clusteringGen = clusterings.get();
                Object[] pk = partitionKeys.get().generate(in);
                Object[] ckEq = clusteringGen.generate(in);

                Delete delete = QueryBuilder.delete().from(schema.ksName, schema.tableName);

                Delete.Where where = delete.where();

                for (int i = 0; i < schema.partitionKeys.size(); i++)
                {
                    ColumnSpec<?> pkColumn = schema.partitionKeys.get(i);
                    where.and(eq(pkColumn.name, pk[i]));
                }

                // TODO (jwest): randomly generate point deletes too
                if (schema.clusteringKeys.size() == 0 || !rowRangeDeletes && !rowPointDeletes)
                {
                    delete.using(timestamp(ts));
                    return delete;
                }

                int eqs;
                if (rowRangeDeletes && !rowPointDeletes) // range only
                    eqs = integers().between(0, schema.clusteringKeys.size() - 1).generate(in);
                else if (rowPointDeletes && !rowRangeDeletes) // point only
                    eqs = schema.clusteringKeys.size();
                else // both
                    eqs = integers().between(0, schema.clusteringKeys.size()).generate(in);


                for (int i = 0; i < eqs; i++)
                {
                    ColumnSpec<?> ck = schema.clusteringKeys.get(i);
                    where.and(eq(ck.name, ckEq[i]));
                }

                // maybe include bound if range query
                int i = Math.max(eqs, 0);
                boolean includeBound = booleans().all().generate(in);
                if (includeBound && i < schema.clusteringKeys.size())
                {
                    Sign sign = arbitrary().pick(Sign.LT, Sign.GT, Sign.GTE, Sign.LTE).generate(in);
                    ColumnSpec<?> ck = schema.clusteringKeys.get(i);
                    where.and(sign.getClause(ck.name, ckEq[i]));

                    if (booleans().all().generate(in)) // maybe add opposite bound
                    {
                        Object[] ckBound = clusteringGen.generate(in);
                        where.and(sign.negate().getClause(ck.name, ckBound[i]));
                    }
                }

                delete.using(timestamp(ts));
                return delete;
            };
        }

    }

}
