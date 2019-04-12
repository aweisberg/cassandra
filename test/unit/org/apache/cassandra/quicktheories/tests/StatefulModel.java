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

package org.apache.cassandra.quicktheories.tests;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.quicktheories.generators.CompiledStatement;
import org.apache.cassandra.quicktheories.generators.DeletesDSL;
import org.apache.cassandra.quicktheories.generators.ReadsDSL;
import org.apache.cassandra.quicktheories.generators.SchemaSpec;
import org.apache.cassandra.quicktheories.generators.WritesDSL;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.stateful.StatefulTheory;

import static org.apache.cassandra.distributed.test.DistributedTestBase.assertRows;
import static org.apache.cassandra.distributed.test.DistributedTestBase.rowsToString;

public abstract class StatefulModel extends StatefulTheory.StepBased
{
    private static final Logger logger = LoggerFactory.getLogger(StatefulModel.class);

    private final AbstractCluster<IInstance> testCluster;
    private final AbstractCluster<IInstance> modelCluster;
    protected final ModelState modelState;
    protected final Gen<Integer> nodeSelector;
    protected SchemaSpec schemaSpec;
    protected Set<Integer> deadNodes = new HashSet<>();

    public StatefulModel(ModelState modelState,
                         AbstractCluster<IInstance> testCluster,
                         AbstractCluster<IInstance> modelCluster)
    {
        this.modelState = modelState;
        this.testCluster = testCluster;
        this.modelCluster = modelCluster;
        this.nodeSelector = SourceDSL.integers().between(1, testCluster.size())
                                     // Never try querying dead nodes
                                     .assuming(i -> !deadNodes.contains(i));
    }

    public void addSetupStep(StatefulTheory.StepBuilder builder)
    {
        addSetupStep(builder.build());
    }

    public void addStep(StatefulTheory.StepBuilder builder)
    {
        addStep(builder.build());
    }

    void injectFailure(int node)
    {
        testCluster.filters().allVerbs().to(node).drop();
        testCluster.filters().allVerbs().from(node).drop();

        for (int i = 1; i <= testCluster.size(); i++)
        {
            if (node != i)
                testCluster.get(i).markPeerDead(node);
        }
        deadNodes.add(node);
    }

    void restoreNetwork()
    {
        testCluster.filters().reset();
        for (int i = 1; i <= testCluster.size(); i++)
        {
            for (int j = 1; j <= testCluster.size(); j++)
            {
                testCluster.get(i).markPeerAlive(j);
            }
        }
        deadNodes.clear();
    }

    public void run(ReadsDSL.Select query, int node)
    {
        run(query, ConsistencyLevel.QUORUM, node);
    }

    public void run(ReadsDSL.Select query, ConsistencyLevel cl, int node)
    {
        Iterator<Object[]> modelRows;
        Iterator<Object[]> sutRows;
        CompiledStatement compiledStatement = query.compile();

        try
        {
            modelRows = Iterators.forArray(modelCluster.get(1).executeInternal(compiledStatement.cql(), compiledStatement.bindings()));
            sutRows = testCluster.coordinator(node).executeWithPaging(compiledStatement.cql(),
                                                                      cl,
                                                                      // TODO: randomsie page size
                                                                      20,
                                                                      compiledStatement.bindings());
            assertRows(sutRows, modelRows);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(String.format("Caught exception while executing: %s with %s CL on node %d", query, cl, node), t) ;
        }
    }

    protected void run(DeletesDSL.Delete delete, int node)
    {
        run(delete, ConsistencyLevel.QUORUM, node);
    }

    protected void run(DeletesDSL.Delete delete, ConsistencyLevel cl, int node)
    {
        assert !deadNodes.contains(node);
        CompiledStatement compiledStatement = delete.compile();

        try
        {
            modelCluster.get(1).executeInternal(compiledStatement.cql(), compiledStatement.bindings());
            testCluster.coordinator(node).execute(compiledStatement.cql(),
                                                  cl,
                                                  compiledStatement.bindings());
        }
        catch (Throwable t)
        {
            throw new RuntimeException(String.format("Caught exception while executing: %s", delete), t);
        }
    }

    protected void run(WritesDSL.Write write, int node)
    {
        run(write, ConsistencyLevel.QUORUM, node);
    }

    protected void run(WritesDSL.Write write, ConsistencyLevel cl, int node)
    {
        assert !deadNodes.contains(node);
        CompiledStatement compiledStatement = write.compile();
        try
        {
            modelState.addFullKey(write.key());

            modelCluster.get(1).executeInternal(compiledStatement.cql(), compiledStatement.bindings());
            testCluster.coordinator(node).execute(compiledStatement.cql(),
                                                  cl,
                                                  compiledStatement.bindings());
        }
        catch (Throwable t)
        {
            throw new RuntimeException(String.format("Caught exception while executing: %s", write), t);
        }
    }

    protected void insertRows(List<WritesDSL.Insert> rows, int node)
    {
        assert !deadNodes.contains(node);
        for (WritesDSL.Write row : rows)
            run(row, node);
    }

    public void initSchema(SchemaSpec schemaSpec)
    {
        assert this.schemaSpec == null : "Schema was already initialized";
        this.schemaSpec = schemaSpec;
        CompiledStatement ddl = schemaSpec.compile();
        logger.info("Creating schema: {}", ddl);
        modelCluster.schemaChange(ddl.cql());
        testCluster.schemaChange(ddl.cql());
    }

    @Override
    public void teardown()
    {
        restoreNetwork();
        // Make sure to drop the table to avoid OOMs
        if (schemaSpec != null)
        {
            String dropTable = String.format("DROP TABLE %s.%s", schemaSpec.ksName, schemaSpec.tableName);
            modelCluster.schemaChange(dropTable);
            testCluster.schemaChange(dropTable);
        }
    }
}
