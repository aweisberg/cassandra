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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.driver.core.querybuilder.Ordering;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.apache.cassandra.quicktheories.generators.Extensions.subsetGenerator;
import static org.apache.cassandra.quicktheories.generators.Relation.*;
import static org.quicktheories.generators.SourceDSL.*;

public class ReadsDSL
{
    public Builder anyRead(SchemaSpec schemaSpec)
    {
        return new Builder(schemaSpec,
                           schemaSpec.partitionKeyGenerator,
                                pk -> schemaSpec.clusteringKeyGenerator,
                           Generate.enumValues(QueryKind.class));
    }

    public Builder anyRead(SchemaSpec schemaSpec,
                           Gen<Object[]> pkGen,
                           Function<Object[], Gen<Object[]>> ckGenSupplier)
    {
        return new Builder(schemaSpec,
                           pkGen,
                           ckGenSupplier,
                           Generate.enumValues(QueryKind.class));
    }

    public Builder partitionRead(SchemaSpec schemaSpec)
    {
        return new Builder(schemaSpec,
                           schemaSpec.partitionKeyGenerator,
                           null,
                           Generate.constant(QueryKind.SINGLE_PARTITION));
    }

    public Builder partitionRead(SchemaSpec schemaSpec, Object[] pk)
    {
        return new Builder(schemaSpec,
                           Generate.constant(pk),
                           null,
                           Generate.constant(QueryKind.SINGLE_PARTITION));
    }

    public Builder rowRead(SchemaSpec schemaSpec)
    {
        assert schemaSpec.clusteringKeys.size() > 0 : "Can't read a row from the table that has no clustering columns";

        return new Builder(schemaSpec,
                           schemaSpec.partitionKeyGenerator,
                           (pk) -> schemaSpec.clusteringKeyGenerator,
                           Generate.constant(QueryKind.SINGLE_ROW));
    }

    public Builder rowRead(SchemaSpec schemaSpec, Gen<Object[]> pkGen, Function<Object[], Gen<Object[]>> ckGen)
    {
        return new Builder(schemaSpec,
                           pkGen,
                           ckGen,
                           Generate.constant(QueryKind.SINGLE_ROW));
    }

    /**
     * Generates a row slice, e.g. (ck, ∞), (∞, ck), [ck, ∞), (∞, ck]
     */
    public Builder rowSlice(SchemaSpec schemaSpec)
    {
        assert schemaSpec.clusteringKeys.size() > 0 : "Can't read a row slice from the table that has no clustering columns";

        return new Builder(schemaSpec,
                           schemaSpec.partitionKeyGenerator,
                           (pk) -> schemaSpec.clusteringKeyGenerator,
                           Generate.constant(QueryKind.CLUSTERING_SLICE));
    }

    /**
     * Generates a row slice, e.g., inclusive or exclusive range between two clusterings
     */
    public Builder rowRange(SchemaSpec schemaSpec)
    {
        return new Builder(schemaSpec,
                           schemaSpec.partitionKeyGenerator,
                           (pk) -> schemaSpec.clusteringKeyGenerator,
                           Generate.constant(QueryKind.CLUSTERING_RANGE));
    }

    private static Gen<List<String>> columnSubsetGen(SchemaSpec schemaSpec)
    {
        if (schemaSpec.allColumns.size() == 0)
            throw new IllegalArgumentException("Can't generate a subset of an empty set");

        return subsetGenerator(schemaSpec.allColumns, 1, schemaSpec.allColumns.size() == 1 ? 1 : schemaSpec.allColumns.size() - 1)
               // We can only restrict clustering columns when selecting non-static columns
               // using `map` instead of `assuming` to avoid running out of variants
               .map(columnDefinitions -> {
                   if (columnDefinitions.stream().allMatch((cd) -> cd.kind == ColumnMetadata.Kind.STATIC ||
                                                                   cd.kind == ColumnMetadata.Kind.PARTITION_KEY))
                       return schemaSpec.allColumns;
                   return columnDefinitions;
               })
               .map(cds -> cds.stream().map(ColumnSpec::name).collect(Collectors.toList()));
    }

    private static Gen<List<Pair<String, Boolean>>> clusteringOrderGen(List<ColumnSpec<?>> columnSpecs)
    {
        return booleans().all().map(isReversed -> {
            List<Pair<String, Boolean>> orderings = new ArrayList<>();
            // TODO (alexp): try same with _not all_ columns in ordering
            for (ColumnSpec<?> columnSpec : columnSpecs)
            {
                orderings.add(Pair.create(columnSpec.name,
                                          isReversed ? columnSpec.isReversed() : !columnSpec.isReversed()));
            }

            return orderings;
        });
    }

    public static class Builder implements Gen<Select>
    {
        private final SchemaSpec schemaSpec;
        private final Gen<Object[]> pkGen;
        private final Function<Object[], Gen<Object[]>> ckGenSupplier;
        private final Gen<QueryKind> readTypeGen;

        private final boolean wildcard;
        private final boolean addLimit;
        private final boolean addOrder;

        private final Gen<Select> generator;

        Builder(SchemaSpec schemaSpec,
                Gen<Object[]> pkGen,
                Function<Object[], Gen<Object[]>> ckGenSupplier,
                Gen<QueryKind> readTypeGen)
        {
            this(schemaSpec, pkGen, ckGenSupplier, readTypeGen,
                 true, false, false);
        }

        Builder(SchemaSpec schemaSpec,
                Gen<Object[]> pkGen,
                Function<Object[], Gen<Object[]>> ckGenSupplier,
                Gen<QueryKind> readTypeGen,
                boolean wildcard,
                boolean addLimit,
                boolean addOrder)
        {
            this.schemaSpec = schemaSpec;
            this.pkGen = pkGen;
            this.ckGenSupplier = ckGenSupplier;
            this.readTypeGen = readTypeGen;

            this.wildcard = wildcard;
            this.addLimit = addLimit;
            this.addOrder = addOrder;

            this.generator = build(schemaSpec, pkGen, ckGenSupplier, readTypeGen, wildcard, addLimit, addOrder);
        }

        public Builder withColumnSelection()
        {
            if (!wildcard)
                return this;

            return new Builder(schemaSpec, pkGen, ckGenSupplier, readTypeGen, false, addLimit, addOrder);
        }

        public Builder withLimit()
        {
            if (addLimit)
                return this;

            return new Builder(schemaSpec, pkGen, ckGenSupplier, readTypeGen, wildcard, true, addOrder);
        }

        public Builder withOrder()
        {
            if (addOrder)
                return this;

            return new Builder(schemaSpec, pkGen, ckGenSupplier, readTypeGen, wildcard, addLimit, true);
        }

        private static QueryKind validate(Gen<Object[]> pkGen,
                                          Function<Object[], Gen<Object[]>> ckGenSupplier,
                                          QueryKind readType)
        {
            switch (readType)
            {
                case SINGLE_PARTITION:
                    if (pkGen == null)
                        throw new IllegalArgumentException("Need a partition key generator to generate partition reads");
                    break;
                case SINGLE_ROW:
                case CLUSTERING_SLICE:
                case CLUSTERING_RANGE:
                    if (pkGen == null || ckGenSupplier == null)
                        throw new IllegalArgumentException("Need a partition and a clustering key key generator to generate row and slice reads");
            }

            return readType;
        }

        // TODO (alexp): make it possible to provide columns
        private static Gen<Select> build(SchemaSpec schemaSpec,
                                         Gen<Object[]> pkGen,
                                         Function<Object[], Gen<Object[]>> ckGenSupplier,
                                         Gen<QueryKind> readTypeGen,
                                         boolean wildcard,
                                         boolean addLimit,
                                         boolean addOrder)
        {
            Gen<Optional<List<String>>> selectionGen = wildcard
                                                       ? Generate.constant(Optional.empty())
                                                       // TODO: validate partition-only selection in row-level query
                                                       : columnSubsetGen(schemaSpec).map(Optional::of);

            Gen<Optional<Integer>> limitGen = addLimit
                                              ? integers().between(1, 100).map(Optional::of) // TODO: to make limit configurable
                                              : Generate.constant(Optional.empty());

            Gen<List<Pair<String, Boolean>>> orderingGen = addOrder
                                                           ? clusteringOrderGen(schemaSpec.clusteringKeys)
                                                           : Generate.constant(Collections.EMPTY_LIST);

            return selectionGen.zip(readTypeGen.map(readType -> validate(pkGen, ckGenSupplier, readType))
                                               .flatMap(readType -> Relation.relationsGen(schemaSpec,
                                                                                          pkGen,
                                                                                          ckGenSupplier,
                                                                                          readType)),
                                    limitGen,
                                    orderingGen,
                                    (selection, relations, limit, order) -> {
                                        return new Select(schemaSpec,
                                                          selection,
                                                          relations,
                                                          limit,
                                                          order);
                                    });
        }

        public Select generate(RandomnessSource prng)
        {
            return generator.generate(prng);
        }
    }

    public static class Select
    {
        public final SchemaSpec schemaSpec;
        public final Optional<List<String>> selectedColumns;
        public final List<Relation> relations;
        public final Optional<Integer> limit;
        public final List<Pair<String, Boolean>> ordering;

        // TODO: filtering
        Select(SchemaSpec schemaSpec,
               // TODO (alexp): do we need optional here?
               Optional<List<String>> selectedColumns,
               List<Relation> relations,
               Optional<Integer> limit,
               List<Pair<String, Boolean>> ordering)
        {
            this.schemaSpec = schemaSpec;
            this.selectedColumns = selectedColumns;
            this.relations = relations;
            this.limit = limit;
            this.ordering = ordering;
        }


        public CompiledStatement compile()
        {
            Object[] bindings = new Object[relations.size()];
            int bindingCount = 0;
            com.datastax.driver.core.querybuilder.Select statement = (selectedColumns.isPresent() ?
                                                                      select(selectedColumns.get().toArray()) :
                                                                      select())
                                                                     .from(schemaSpec.ksName, schemaSpec.tableName);

            if (!relations.isEmpty())
            {
                com.datastax.driver.core.querybuilder.Select.Where where = statement.where();
                for (Relation relation : relations)
                {
                    where.and(relation.toClause());
                    bindings[bindingCount++] = relation.value();
                }
            }

            if (!ordering.isEmpty())
            {
                Ordering[] orderings = new Ordering[ordering.size()];
                for (int i = 0; i < orderings.length; i++)
                {
                    Pair<String, Boolean> p = ordering.get(i);
                    // TODO: this can be improved
                    orderings[i] = p.right ? asc(p.left) : desc(p.left);
                }
            }

            limit.ifPresent(statement::limit);

            // TODO (alexp): order
            assert bindingCount == bindings.length : bindingCount + " != " + bindings.length;
            return CompiledStatement.create(statement.toString(), bindings);
        }

        @Override
        public String toString()
        {
            // A little wasteful, but necessary
            return compile().toString();
        }
    }
}
