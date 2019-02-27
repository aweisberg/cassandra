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

package org.apache.cassandra.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.test.DistributedTestBase;
import org.apache.cassandra.quicktheories.generators.ColumnSpec;
import org.apache.cassandra.quicktheories.generators.FullKey;
import org.apache.cassandra.quicktheories.generators.QueryGen;
import org.apache.cassandra.quicktheories.generators.SchemaSpec;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.stateful.StatefulTheory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static org.apache.cassandra.distributed.test.DistributedTestBase.assertRows;
import static org.apache.cassandra.distributed.test.DistributedTestBase.rowsToString;
import static org.apache.cassandra.distributed.test.DistributedTestBase.toObjectArray;
import static org.apache.cassandra.quicktheories.generators.Extensions.combine;

abstract class QuorumReadModel extends StatefulTheory.StepBased
{
    final String ks;
    final AbstractCluster model;
    final AbstractCluster testCluster;
    final Executor executor;

    // All partition keys, both already written and still empty ones
    final List<Object[]> pks = new ArrayList<>();
    final List<Object[]> writtenPks = new ArrayList<>();
    final Map<Object[], List<FullKey>> rowKeys = new ConcurrentHashMap<>();
    final List<String> commands = new ArrayList<>();

    final int initialKeys;

    int deadNode = -1;

    SchemaSpec schema;

    public QuorumReadModel(String ks, AbstractCluster modelCluster, AbstractCluster testCluster)
    {
        this(ks, modelCluster, testCluster, 1);
    }

    public QuorumReadModel(String ks, AbstractCluster modelCluster, AbstractCluster testCluster, int initialKeys)
    {
        this.ks = ks;
        this.model = modelCluster;
        this.testCluster = testCluster;
        this.executor = Executors.newSingleThreadExecutor();
        this.initialKeys = initialKeys;
    }

    protected void savePk(Object[] pk)
    {
        this.pks.add(pk);
    }

    public void initSchema(SchemaSpec schema)
    {
        try
        {
            this.schema = schema;
            String ddl = schema.toCQL();
            System.out.println("CREATING SCHEMA: \n" + ddl);
            model.schemaChange(ddl);
            testCluster.schemaChange(ddl);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void setInitialKeys(Collection<Object[]> keys)
    {
        try
        {
            for (Object[] key : keys)
                savePk(key);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public Gen<FullKey> fullKeyPicker()
    {
        return new Gen<FullKey> (){

            private Gen<FullKey> gen;
            public FullKey generate(RandomnessSource prng)
            {
                if (gen == null)
                {
                    Object[] pk = SourceDSL.arbitrary().pick(writtenPks).generate(prng);
                    gen = SourceDSL.arbitrary().pick(rowKeys.get(pk));

                }
                return gen.generate(prng);
            }
        };
    }

    public Gen<FullKey> fullKeyPicker(Object[] pk)
    {
        return SourceDSL.arbitrary().pick(rowKeys.get(pk));
    }


    public Gen<Object[]> partitionKeyPicker()
    {
        return SourceDSL.arbitrary().pick(writtenPks);
    }

    protected Gen<Object[]> pkGen()
    {
        return schema.partitionGenerator
               .map(pairs -> {
                   Object[] pk = new Object[pairs.size()];
                   for (int i = 0; i < pairs.size(); i++)
                   {
                       pk[i] = pairs.get(i).right;
                   }
                   return pk;
               });
    }

    protected Gen<Delete> delGen(Object[] pk)
    {
        Gen<FullKey> fullKeyPicker = fullKeyPicker(pk);

        return combine(fullKeyPicker, (prng, keyPicker) -> {
            return QueryGen.deleteGen(schema, pk, fullKeyPicker.generate(prng).clustering, fullKeyPicker.generate(prng).clustering)
                           .generate(prng);
        });
    }

    protected Gen<Delete> delGen()
    {
        return combine(partitionKeyPicker(), (prng, partitionKeyPicker) -> {
            Object[] pk = partitionKeyPicker.generate(prng);
            Gen<FullKey> fullKeyPicker = fullKeyPicker(pk);

            return QueryGen.deleteGen(schema, pk, fullKeyPicker.generate(prng).clustering, fullKeyPicker.generate(prng).clustering)
                           .generate(prng);
        });
    }

    protected Gen<Pair<FullKey, Insert>> writeGen()
    {
        return writeGen(SourceDSL.arbitrary().pick(pks));
    }

    protected Gen<Pair<FullKey, Insert>> writeGen(Object[] fullKey)
    {
        return QueryGen.writeGen(schema, fullKey);
    }

    protected Gen<Pair<FullKey, Insert>> writeGen(Gen<Object[]> fullKeyPicker)
    {
        return fullKeyPicker.flatMap(key -> QueryGen.writeGen(schema, key));
    }

    protected Gen<Select> readGen()
    {
        return combine(fullKeyPicker(), (prng, keyGen) -> {
            return QueryGen.readGen(schema, keyGen.generate(prng))
                           .generate(prng);
        });
    }

    protected Gen<Select> clusteringRangeReadGen()
    {
        return combine(fullKeyPicker(), (prng, keyGen) -> {
            return QueryGen.clusteringRangeReadGen(schema, keyGen.generate(prng), keyGen.generate(prng))
                           .generate(prng);
        });
    }

    AtomicInteger failures = new AtomicInteger();
    protected boolean hasWritten()
    {
        System.out.println("failures.incrementAndGet() = " + failures.incrementAndGet());
        return rowKeys.size() > 0;
    }


    boolean isSetup()
    {
        return schema != null;
    }

//    void restoreNetwork()
//    {
//        deadNode = -1;
//        testCluster.filters().reset();
//        testCluster.markAllAlive();
//    }
//
//    void injectFailure(int node)
//    {
//        restoreNetwork();
//        deadNode = node;
//        testCluster.filters().allVerbs().to(node).drop();
//        testCluster.markAsDead(node);
//    }
//
//    void flush(int node)
//    {
//        commands.add(Pair.create("flush", node).toString());
//        executor.execute(() -> {
//            testCluster.get(node).flush(schema.ksName, schema.tableName);
//        });
//    }
//
//    void compact(int node)
//    {
//        testCluster.get(node).compact(schema.ksName, schema.tableName);
//    }

//    void writeTimeout(Pair<Integer, String> p)
//    {
//        restoreNetwork();
//        testCluster.filters().allVerbs().to(p.left).drop();
//        try
//        {
//            model.coordinator().execute(p.right,
//                                        ConsistencyLevel.QUORUM);
//
//            testCluster.coordinator().execute(p.right,
//                                              ConsistencyLevel.QUORUM);
//        }
//        catch (Throwable t)
//        {
//            // silent
//        }
//        finally
//        {
//            restoreNetwork();
//        }
//    }

    @Override
    public void teardown()
    {
        // didn't initialize
        if (schema == null)
            return;

//        restoreNetwork();
        String ddl = "DROP TABLE " + schema.ksName + "." + schema.tableName;
        model.schemaChange(ddl);
        testCluster.schemaChange(ddl);
    }

    public void run(Select query)
    {
        run(query, 1);
    }

    public void run(Select query, int node)
    {
        Object[][] modelRows;
        Object[][] sutRows;
        try
        {
            modelRows = model.coordinator(1).execute(query.toString(),
                                                    ConsistencyLevel.QUORUM);
            sutRows = testCluster.coordinator(node).execute(query.toString(),
                                                         ConsistencyLevel.QUORUM);
            assertRows(modelRows, sutRows);

            Object[][] reversed = new Object[modelRows.length][];
            for (int j = 0; j < sutRows.length; j++)
                reversed[modelRows.length - j - 1] = modelRows[j];

            ColumnSpec ck = schema.clusteringKeys.get(0);
            if (ck.type.isReversed())
                query.orderBy(asc(schema.clusteringKeys.get(0).name));
            else
                query.orderBy(desc(schema.clusteringKeys.get(0).name));

            assertRows(reversed,
                       testCluster.coordinator(node).execute(query.toString(),
                                                          ConsistencyLevel.QUORUM));
        }
        catch (AssertionError e)
        {
            System.out.println("query = " + query);
            reportAndExit(e, node);
        }
    }

    private void reportAndExit(Throwable t, int node)
    {
        System.out.println("Node: " + node);
        System.out.println(schema.toCQL());
        System.out.println("\n\n\nEXECUTED COMMANDS: " + commands.size());
        for (String command : commands)
        {
            System.out.println(command);
        }
        try
        {
            for (int j = 0; j < 10; j++)
            {

                String nextTry = rowsToString(testCluster.coordinator(node).execute("SELECT * FROM " + DistributedTestBase.KEYSPACE + "." + schema.tableName,
                                                                                    ConsistencyLevel.QUORUM));
                System.out.println("nextTry = " + nextTry);
            }
        }
        catch (Throwable t1)
        {
            // skip
        }

        System.err.println(t.getMessage());
        t.printStackTrace();
        System.exit(1);
    }

    public void run(Delete query)
    {
        run(query, 1);
    }

    public void run(Delete query, int i)
    {
        try
        {
            commands.add(Pair.create(query.toString(), i).toString());
            model.coordinator(1).execute(query.toString(),
                                         ConsistencyLevel.QUORUM);
            testCluster.coordinator(i).execute(query.toString(),
                                               ConsistencyLevel.QUORUM);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run(Pair<FullKey, Insert> write)
    {
        run(write, 1);
    }

    public void run(Pair<FullKey, Insert> write, int i)
    {
        try
        {
            String query = write.right.toString();
            System.out.println("query = " + query);
            commands.add(Pair.create(query, i).toString());
            rowKeys.computeIfAbsent(write.left.partition, (pk) -> {
                writtenPks.add(pk);
                return new ArrayList<>();
            })
                   .add(write.left);

            model.get(1).executeInternal(query);
            testCluster.coordinator(i).execute(query,
                                               ConsistencyLevel.QUORUM);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}

