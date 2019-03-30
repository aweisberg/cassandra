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
import java.util.function.Function;

import com.datastax.driver.core.querybuilder.Clause;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

public class Relation
{
    private final String column;
    private final Sign sign;
    private final Object value;

    Relation(String column,
                    Sign sign,
                    Object value)
    {
        this.column = column;
        this.sign = sign;
        this.value = value;
    }

    public Object value()
    {
        return value;
    }

    public Clause toClause()
    {
        return sign.getClause(column, bindMarker());
    }

    private static Gen<Sign> signGen = Generate.enumValues(Sign.class);
    private static Gen<Sign> sliceSigns = Generate.pick(Arrays.asList(Sign.GT, Sign.GTE, Sign.LT, Sign.LTE));

    public static Gen<List<Relation>> relationsGen(SchemaSpec schemaSpec,
                                                   Gen<Object[]> pkGen,
                                                   Function<Object[], Gen<Object[]>> ckGenSupplier,
                                                   QueryKind queryKind)
    {
        return (prng) -> {
            List<Relation> relations = new ArrayList<>();
            switch (queryKind)
            {
                case SINGLE_PARTITION:
                {
                    addRelation(pkGen.generate(prng), schemaSpec.partitionKeys, relations);
                    break;
                }
                case SINGLE_ROW:
                {
                    Object[] pk = pkGen.generate(prng);
                    addRelation(pk, schemaSpec.partitionKeys, relations);
                    Gen<Object[]> ckGen = ckGenSupplier.apply(pk);
                    addRelation(ckGen.generate(prng), schemaSpec.clusteringKeys, relations);
                    break;
                }
                case CLUSTERING_SLICE:
                {
                    Object[] pk = pkGen.generate(prng);
                    addRelation(pk, schemaSpec.partitionKeys, relations);
                    Gen<Object[]> ckGen = ckGenSupplier.apply(pk);
                    Object[] ck = ckGen.generate(prng);
                    for (int i = 0; i < ck.length; i++)
                    {
                        ColumnSpec<?> spec = schemaSpec.clusteringKeys.get(i);
                        Sign sign = signGen.generate(prng);
                        relations.add(relation(spec.name, sign, ck[i]));

                        if (sign != Sign.EQ)
                            break;
                    }

                    break;
                }
                case CLUSTERING_RANGE:
                {
                    Object[] pk = pkGen.generate(prng);
                    addRelation(pk, schemaSpec.partitionKeys, relations);
                    Gen<Object[]> ckGen = ckGenSupplier.apply(pk);
                    Object[] ck1 = ckGen.generate(prng);
                    Object[] ck2 = ckGen.generate(prng);
                    assert ck1.length == ck2.length;
                    for (int i = 0; i < ck1.length; i++)
                    {
                        ColumnSpec<?> spec = schemaSpec.clusteringKeys.get(i);
                        Sign sign = signGen.generate(prng);
                        relations.add(relation(spec.name, sign, ck1[i]));
                        // TODO (alexp): we can also use a roll of dice to mark inclusion/exclusion
                        if (sign.isNegatable())
                            relations.add(relation(spec.name, sign.negate(), ck2[i]));

                        if (sign != Sign.EQ)
                            break;
                    }
                }
            }

            return relations;
        };
    }

    private static Relation relation(String column, Sign sign, Object value)
    {
        return new Relation(column, sign, value);
    }

    private static void addRelation(Object[] pk, List<ColumnSpec<?>> columnSpecs, List<Relation> relations)
    {
        for (int i = 0; i < pk.length; i++)
        {
            ColumnSpec<?> spec = columnSpecs.get(i);
            relations.add(relation(spec.name, Sign.EQ, pk[i]));
        }
    }

    enum QueryKind
    {
        SINGLE_PARTITION,
        // TODO (alexp): implement these
//        MULTI_PARTITION,
//        PARTITION_RANGE_OPEN,
//        PARTITION_RANGE_CLOSED,
//        MULTI_CLUSTERING_SLICE,
//        MULTI_CLUSTERING_RANGE,
        SINGLE_ROW,
        CLUSTERING_SLICE,
        CLUSTERING_RANGE
    }

}
