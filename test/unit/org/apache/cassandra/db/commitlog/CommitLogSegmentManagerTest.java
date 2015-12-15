/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.commitlog;

import static junit.framework.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;

import org.junit.*;
import org.junit.runner.RunWith;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

@RunWith(BMUnitRunner.class)
public class CommitLogSegmentManagerTest
{
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String STANDARD1 = "Standard1";
    private static final String STANDARD2 = "Standard2";

    private final static byte[] entropy = new byte[1024 * 256];
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        new Random().nextBytes(entropy);
        DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", ImmutableMap.<String, String>of()));
        DatabaseDescriptor.setCommitLogSegmentSize(1);
        DatabaseDescriptor.setCommitLogSync(CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogSyncPeriod(10 * 1000);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD2));

        CompactionManager.instance.disableAutoCompaction();
    }

    public static boolean cl_blocked_on_backpressure = false;

    @Test
    @BMRules( rules = {
    @BMRule(name = "Greatest name in the world",
        targetClass="CompressedSegment",
        targetMethod="createBufferListener",
        targetLocation="AT INVOKE com.google.common.util.concurrent.SettableFuture.create",
        action="org.apache.cassandra.db.commitlog.CommitLogSegmentManagerTest.cl_blocked_on_backpressure = true;"),
    @BMRule(name="Second greatest name in the world",
        targetClass = "CompressedSegment",
        targetMethod="write",
        targetLocation="AT ENTRY",
        action="com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);")
    })
    public void testCompressedCommitLogBackpressure() throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);

        Mutation m = new Mutation(KEYSPACE1, ByteBufferUtil.bytes("k"));
        m.add(STANDARD1, Util.cellname("c1"), ByteBuffer.wrap(entropy), 0);

        for (int i = 0; i < 20; i++)
            CommitLog.instance.add(m);

        assertTrue(cl_blocked_on_backpressure);
    }
}

