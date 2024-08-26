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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.accord.AccordTestBase;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.util.QueryResultUtil.assertThat;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.apache.cassandra.net.Verb.READ_REPAIR_REQ;
import static org.junit.Assert.assertEquals;

public class AccordReadRepairTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordReadRepairTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    static AtomicLong interopApplyMessages = new AtomicLong();
    static AtomicLong regularApplyMessages = new AtomicLong();
    static AtomicLong interopRepairMessages = new AtomicLong();
    static AtomicLong interopRepairResponseMessages = new AtomicLong();

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder, 2);
        SHARED_CLUSTER.setMessageSink((to, message) ->
                               {
                                   Verb verb = Verb.fromId(message.verb());
                                   logger.info("verb {} to {} message {}", verb, to, message);
                                   if (message.verb() == Verb.ACCORD_INTEROP_APPLY_REQ.id)
                                       interopApplyMessages.incrementAndGet();
                                   if (message.verb() == Verb.ACCORD_APPLY_REQ.id)
                                       regularApplyMessages.incrementAndGet();
                                   if (message.verb() == Verb.ACCORD_INTEROP_READ_REPAIR_REQ.id)
                                       interopRepairMessages.incrementAndGet();
                                   if (message.verb() == Verb.ACCORD_INTEROP_READ_REPAIR_RSP.id)
                                       interopRepairResponseMessages.incrementAndGet();
                                   SHARED_CLUSTER.get(to).receiveMessage(message);
                               }
        );
    }

    @After
    public void tearDown()
    {
        interopApplyMessages.set(0);
        regularApplyMessages.set(0);
        interopRepairMessages.set(0);
        interopRepairResponseMessages.set(0);
    }

    /*
     * SERIAL read and CAS create Accord transactions which will then invoke Cassandra coordination to perform the read
     * and proxy any read repairs that are generated.
     */
    @Test
    public void testSerialReadRepair() throws Exception
    {
        testReadRepair(cluster -> cluster.coordinator(1).execute("SELECT * FROM " + qualifiedAccordTableName + " WHERE k = 1 AND c = 1;", ConsistencyLevel.SERIAL),
                       new Object[][] {{1, 1, 1, 1}},
                       TransactionalMode.unsafe_writes,
                       0, 2);
    }

    @Test
    public void testCASFailedConditionReadRepair() throws Exception
    {
        // Even if the condition fails to apply the data checked when applying the condition should be repaired
        testReadRepair(cluster -> cluster.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (k, c, v1) VALUES (1, 1, 99) IF NOT EXISTS;", ConsistencyLevel.SERIAL),
                       new Object[][] {{false, 1, 1, 1, 1}},
                       TransactionalMode.unsafe_writes,
                       2, 0);
    }

    @Test
    public void testCASReadRepair() throws Exception
    {
        // If the condition applies the read repair should preserve the existing timestamp
        testReadRepair(cluster -> cluster.coordinator(1).execute("UPDATE  " + qualifiedAccordTableName + " SET v2 = 99 WHERE k = 1 and c = 1 IF EXISTS;", ConsistencyLevel.SERIAL),
                       new Object[][] {{Boolean.TRUE}},
                       TransactionalMode.unsafe_writes,
                       2, 0);
    }

    /*
     * non-SERIAL consistency levels are coordinated by C* and then if a partition needs to be repaired an Accord transaction
     * is created for each partition repair to proxy the repair mutations safely.
     */
    @Test
    public void testNonSerialReadRepair() throws Exception
    {
        for (ConsistencyLevel cl : ImmutableList.of(ConsistencyLevel.QUORUM))
            testReadRepair(cluster -> cluster.coordinator(1).execute("SELECT * FROM " + qualifiedAccordTableName + " WHERE k = 1 AND c = 1;", cl),
                           new Object[][] {{1, 1, 1, 1}},
                           TransactionalMode.unsafe_writes,
                           0, 2);
    }

    @Test
    public void testNonSerialRangeReadRepair() throws Exception
    {
        for (ConsistencyLevel cl : ImmutableList.of(ConsistencyLevel.QUORUM))
            testReadRepair(cluster -> cluster.coordinator(1).execute("SELECT * FROM " + qualifiedAccordTableName + " WHERE TOKEN(k) > " + Long.MIN_VALUE + " AND TOKEN(k) < " + Long.MAX_VALUE, cl),
                           new Object[][] {{1, 1, 1, 1}},
                           TransactionalMode.unsafe_writes,
                           0, 2);
    }

    void testReadRepair(Function<Cluster, Object[][]> accordTxn, Object[][] expected, TransactionalMode transactionalMode, int expectedInteropApply, int expectedRegularApply) throws Exception
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v1 int, v2 int, PRIMARY KEY ((k), c)) WITH transactional_mode='" + transactionalMode + "';",
             cluster -> {
                 cluster.filters().verbs(READ_REPAIR_REQ.id, MUTATION_REQ.id, HINT_REQ.id).drop().on();
                 cluster.get(1).executeInternal("INSERT INTO " + qualifiedAccordTableName + " (k, c, v1, v2) VALUES (1, 1, 1, 1) USING TIMESTAMP 42;");
                 assertThat(cluster.get(2).executeInternalWithResult("SELECT * FROM " + qualifiedAccordTableName + " WHERE k = 1 AND c = 1;")).isEmpty();
                 // Should perform read repair
                 Object[][] result = accordTxn.apply(cluster);
                 assertRows(result, expected);
                 // Side effect of the read repair should be visible now
                 assertThat(cluster.get(2).executeInternalWithResult("SELECT k, c, v1, WRITETIME(v1) FROM " + qualifiedAccordTableName + " WHERE k = 1 AND c = 1;"))
                 .isEqualTo(1, 1, 1, 42L);
                 assertEquals(expectedInteropApply, interopApplyMessages.get());
                 assertEquals(expectedRegularApply, regularApplyMessages.get());
                 assertEquals(1, interopRepairMessages.get());
                 assertEquals(1, interopRepairResponseMessages.get());
             });
    }
}
