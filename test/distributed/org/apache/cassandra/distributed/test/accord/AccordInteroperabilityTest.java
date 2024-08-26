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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.net.Verb;

import static org.junit.Assert.assertEquals;

public class AccordInteroperabilityTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordInteroperabilityTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    private static final Map<Verb, AtomicLong> messageCounts = new ConcurrentHashMap<>();

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder, 3);
        SHARED_CLUSTER.setMessageSink((to, message) ->
                                      {
                                          Verb verb = Verb.fromId(message.verb());
                                          logger.info("verb {} to {} message {}", verb, to, message);
                                          messageCounts.computeIfAbsent(verb, ignored -> new AtomicLong()).incrementAndGet();
                                          SHARED_CLUSTER.get(to).receiveMessage(message);
                                      });
    }

    @After
    public void tearDown()
    {
        messageCounts.clear();
    }

    @Test
    public void testSerialReadDescending() throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c)) WITH transactional_mode='full'",
             cluster -> {
                 ICoordinator coordinator = cluster.coordinator(1);
                 for (int i = 1; i <= 10; i++)
                     coordinator.execute("INSERT INTO " + qualifiedAccordTableName + " (k, c, v) VALUES (0, ?, ?) USING TIMESTAMP 0;", ConsistencyLevel.ALL, i, i * 10);
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedAccordTableName + " WHERE k=0 ORDER BY c DESC LIMIT 1", AssertUtils.row(10, 100));
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedAccordTableName + " WHERE k=0 ORDER BY c DESC LIMIT 2", AssertUtils.row(10, 100), AssertUtils.row(9, 90));
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedAccordTableName + " WHERE k=0 ORDER BY c DESC LIMIT 3", AssertUtils.row(10, 100), AssertUtils.row(9, 90), AssertUtils.row(8, 80));
                 assertRowSerial(cluster, "SELECT c, v FROM " + qualifiedAccordTableName + " WHERE k=0 ORDER BY c DESC LIMIT 4", AssertUtils.row(10, 100), AssertUtils.row(9, 90), AssertUtils.row(8, 80), AssertUtils.row(7, 70));
             }
         );
    }

    @Test
    public void testApplyIsInteropApply() throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c)) WITH transactional_mode='mixed_reads'",
             cluster -> {
                cluster.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (k, c, v) VALUES (1, 2, 3)", ConsistencyLevel.QUORUM);
                assertEquals(3, messageCounts.get(Verb.ACCORD_INTEROP_APPLY_REQ).get());
             });
    }

    @Test
    public void testReadIsAtQuorum() throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY(k, c)) WITH transactional_mode='unsafe_writes'",
             cluster -> {
                 cluster.coordinator(1).execute("SELECT * FROM " + qualifiedAccordTableName + " WHERE k = 0", ConsistencyLevel.SERIAL);
                 assertEquals(2, messageCounts.get(Verb.ACCORD_INTEROP_COMMIT_REQ).get());
                 assertEquals(2, messageCounts.get(Verb.ACCORD_INTEROP_READ_RSP).get());
             });
    }
}
