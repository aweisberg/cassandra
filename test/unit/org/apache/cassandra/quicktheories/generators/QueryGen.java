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
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;
import org.quicktheories.generators.SourceDSL;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static org.apache.cassandra.quicktheories.generators.Extensions.combine;
import static org.apache.cassandra.quicktheories.generators.Extensions.subsetGenerator;

public class QueryGen
{
    public static Gen<Select> readGen(SchemaSpec schema, FullKey key)
    {
        return combine(subsetGenerator(schema.allColumns).map(columnDefinitions -> {
                           if (columnDefinitions.stream().allMatch((cd) -> {
                               return cd.kind == ColumnMetadata.Kind.STATIC || cd.kind == ColumnMetadata.Kind.PARTITION_KEY;
                           }))
                               return schema.allColumns;

                           return columnDefinitions;
                       }),
                       Generate.enumValues(Sign.class),
                       (prng, columnsGen, signGen) -> {
                           List<ColumnSpec<?>> columns = columnsGen.generate(prng);
                           String[] columnNames = new String[columns.size()];

                           int i = 0;
                           for (ColumnSpec column : columns)
                               columnNames[i++] = column.name;

                           Select select = QueryBuilder.select(columnNames).from(schema.ksName, schema.tableName);
                           Select.Where where = select.where();
                           i = 0;
                           for (ColumnSpec<?> partitionKey : schema.partitionKeys)
                           {
                               where.and(eq(partitionKey.name, key.partition[i++]));
                           }

                           i = 0;
                           for (ColumnSpec<?> clusteringKey : schema.clusteringKeys)
                           {
                               Sign sign = signGen.generate(prng);
                               where.and(sign.getClause(clusteringKey.name, key.clustering[i++]));
                               // continue only while we have EQ
                               if (sign != Sign.EQ)
                                   break;
                           }

                           return select;
                       });
    }

    public static Gen<Select> clusteringRangeReadGen(SchemaSpec schema, FullKey lKey, FullKey rKey)
    {
        return combine(subsetGenerator(schema.allColumns).map(columnDefinitions -> {
                           if (columnDefinitions.stream().allMatch((cd) -> {
                               return cd.kind == ColumnMetadata.Kind.STATIC || cd.kind == ColumnMetadata.Kind.PARTITION_KEY;
                           }))
                               return schema.allColumns;

                           return columnDefinitions;
                       }),
                       Generate.pick(Arrays.asList(Sign.LT, Sign.GT, Sign.GTE, Sign.LTE)),
                       (prng, columnsGen, signGen) -> {
                           assert Arrays.equals(lKey.partition, rKey.partition) : Arrays.asList(lKey.partition) + " " + Arrays.asList(rKey.partition);
                           List<ColumnSpec<?>> columns = columnsGen.generate(prng);
                           String[] columnNames = new String[columns.size()];

                           int i = 0;
                           for (ColumnSpec column : columns)
                               columnNames[i++] = column.name;

                           Select select = QueryBuilder.select(columnNames).from(schema.ksName, schema.tableName);
                           Select.Where where = select.where();
                           i = 0;
                           for (ColumnSpec<?> partitionKey : schema.partitionKeys)
                               where.and(eq(partitionKey.name, lKey.partition[i++]));

                           Sign sign = signGen.generate(prng);
                           List<String> names = new ArrayList<>();
                           for (ColumnSpec<?> clusteringKey : schema.clusteringKeys)
                               names.add(clusteringKey.name);

                           where.and(sign.getClause(names, Arrays.asList(lKey.clustering)));
                           where.and(sign.negate().getClause(names, Arrays.asList(rKey.clustering)));

                           return select;
                       });
    }

//    public static Gen<Pair<FullKey, Insert>> writeGen(SchemaSpec schemaSpec, Object[] partitionKey)
//    {
//        return combine(schemaSpec.clusteringKeyGenerator,
//                       (prng, rowGen) -> {
//                           Insert insert = QueryBuilder.insertInto(schemaSpec.ksName, schemaSpec.tableName);
//
//                           List<Pair<ColumnSpec<?>, Object>> row = rowGen.generate(prng);
//
//                           for (int i = 0; i < schemaSpec.partitionKeys.size(); i++)
//                           {
//                               insert.value(schemaSpec.partitionKeys.get(i).name,
//                                            partitionKey[i]);
//                           }
//
//                           Object[] clustering = new Object[schemaSpec.clusteringKeys.size()];
//                           for (int i = 0; i < row.size(); i++)
//                           {
//                               Pair<ColumnSpec<?>, Object> pair = row.get(i);
//                               if (pair.left.kind == ColumnMetadata.Kind.CLUSTERING)
//                                   clustering[i] = pair.right;
//                               insert.value(pair.left.name,
//                                            pair.right);
//                           }
//
//                           insert.using(timestamp(FBUtilities.timestampMicros()));
//                           return Pair.create(new FullKey(partitionKey, clustering), insert);
//                       });
//    }

    public static Gen<Delete> deleteGen(SchemaSpec schema, Object[] pk, Object[] ck1, Object[] ck2)
    {
        return combine(SourceDSL.arbitrary().pick(Sign.LT, Sign.GT, Sign.GTE, Sign.LTE),
                       SourceDSL.booleans().all(),
                       SourceDSL.integers().between(0, schema.clusteringKeys.size() - 1),
                       (prng, signGen, booleanGen, intGen) -> {
                           Delete delete = QueryBuilder.delete().from(schema.ksName, schema.tableName);

                           Delete.Where where = delete.where();

                           for (int i = 0; i < schema.partitionKeys.size(); i++)
                           {
                               ColumnSpec<?> pkColumn = schema.partitionKeys.get(i);
                               where.and(eq(pkColumn.name, pk[i]));
                           }

                           // how many EQ parts to have
                           int eqs = intGen.generate(prng);
                           for (int i = 0; i < eqs; i++)
                           {
                               ColumnSpec<?> ck = schema.clusteringKeys.get(i);
                               where.and(eq(ck.name, ck1[i]));
                           }

                           int i = Math.max(eqs, 0);
                           // whether or not to have a bound
                           if (i < schema.clusteringKeys.size() && booleanGen.generate(prng))
                           {
                               Sign sign = signGen.generate(prng);
                               ColumnSpec<?> ck = schema.clusteringKeys.get(i);
                               where.and(sign.getClause(ck.name, ck1[i]));

                               if (booleanGen.generate(prng))
                               {
                                   where.and(sign.negate().getClause(ck.name, ck2[i]));
                               }
                           }

                           delete.using(timestamp(FBUtilities.timestampMicros()));
                           return delete;
                       });
    }

}
