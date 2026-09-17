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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Every case here runs twice, once per {@link Mode}: once with three full replicas of every range, and once with
 * one of those three turned into a witness. A witness journals a mutation so that it can take part in
 * reconciliation and never applies it to the table, so it has no data of its own to answer a read with, and a
 * tracked read takes data from exactly one replica per range - which therefore has to be a full replica of every
 * token in that range.
 * <p>
 * Witnessing changes where the data is, not what the answer is, so no assertion about an answer is conditioned on
 * the mode: the oracle a case is scored against stays fully replicated in both modes (see
 * {@link #createKeyspace}), and the cases that write down expected rows expect the same rows either way. What is
 * mode conditional is the unstressed case checks, which assert where the data sits before the read under test
 * runs, because that is the thing witnessing does change.
 */
@RunWith(Parameterized.class)
public class TrackedRangeReadTest extends TestBaseImpl
{
    private static final int REPLICAS = 3;

    /**
     * The replication the tracked keyspaces are created at. {@link #FULL} is what this suite has always run at.
     * {@link #WITNESSES} differs from it in exactly two things, both of which are what it takes to have witnesses
     * at all: the replication factor asks for one of the three replicas of every range to be transient, and the
     * yaml guard that gates transient replication is turned on.
     */
    public enum Mode
    {
        /** {@code replication_factor: 3} is written unquoted, exactly as this suite has always written it. */
        FULL("{'class': 'SimpleStrategy', 'replication_factor': 3}", false),
        WITNESSES("{'class': 'SimpleStrategy', 'replication_factor': '3/1'}", true);

        final String replication;
        final boolean transientReplicationEnabled;

        Mode(String replication, boolean transientReplicationEnabled)
        {
            this.replication = replication;
            this.transientReplicationEnabled = transientReplicationEnabled;
        }
    }

    @Parameterized.Parameter
    public Mode parameter;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> modes()
    {
        return Arrays.asList(new Object[]{ Mode.FULL }, new Object[]{ Mode.WITNESSES });
    }

    /**
     * The mode the cluster that is currently up was built for, so that the static helpers every case is written in
     * terms of can see it. Null when no cluster is up.
     */
    private static Mode mode;

    private static Cluster cluster;

    /**
     * One cluster per mode, built lazily and torn down when the mode changes, because
     * {@code transient_replication_enabled} is a yaml setting and two in-JVM clusters cannot be up at once - they
     * bind the same loopback addresses and ports. JUnit's Parameterized runner runs every case of one parameter
     * before moving to the next, so the mode changes exactly once and each cluster is built exactly once.
     */
    @Before
    public void beforeEach() throws IOException
    {
        if (mode == parameter)
            return;

        closeCluster();
        cluster = Cluster.build()
                         .withNodes(REPLICAS)
                         // background reconciliation converges the replicas within a few seconds, which would heal
                         // the divergence these cases are built on before the read under test ever sees it
                         .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                               .set("hinted_handoff_enabled", false)
                                               .set("transient_replication_enabled", parameter.transientReplicationEnabled)
                                               .set("mutation_tracking.background_reconciliation_enabled", false))
                         .start();
        mode = parameter;
    }

    @AfterClass
    public static void teardown()
    {
        closeCluster();
    }

    private static void closeCluster()
    {
        mode = null;
        if (cluster != null)
        {
            cluster.close();
            cluster = null;
        }
    }

    /** The tracked keyspace of a case that writes its own schema down rather than going through {@link #createKeyspace}. */
    private static void createTrackedKeyspace(String keyspace)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " + mode.replication + " AND replication_type='tracked'", keyspace));
    }

    @Test
    public void testPartialPartitionFilterWithPerPartitionLimit()
    {
        String keyspace = "partial_partition_filter_per_partition_limit";
        createTrackedKeyspace(keyspace);

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 bigint, pk1 text, ck0 bigint, s0 frozen<list<frozen<list<time>>>> static, " +
                                          "v0 'org.apache.cassandra.db.marshal.LexicalUUIDType', PRIMARY KEY ((pk0, pk1), ck0)) WITH CLUSTERING ORDER BY (ck0 DESC) AND read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 2 SET s0=[['03:28:16.047802044']] WHERE  pk0 = 7137864754153440313 AND  pk1 = '뢸镝蔥'", keyspace));
        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, v0) VALUES (7137864754153440313, '뢸镝蔥', 7732824726196172505, 0x0000000000004d00af00000000000000) USING TIMESTAMP 3", keyspace));

        cluster.get(2).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 5 " +
                                                    "SET s0=[['01:28:35.208066780', '05:25:43.184564123'], ['16:14:58.464860367', '13:59:53.463983006', '10:32:10.674489767']] " +
                                                    "WHERE  pk0 = 1699976006349660742 AND  pk1 = 'ጬ葲'", keyspace));

        cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, s0) VALUES (7137864754153440313, '뢸镝蔥', [['11:13:31.615781929', '02:03:35.298191424', '21:32:35.861361643']]) USING TIMESTAMP 6", keyspace));

        String select = withKeyspace("SELECT * FROM %s.tbl WHERE token(pk0, pk1) BETWEEN token(1699976006349660742, 'ጬ葲') AND token(7137864754153440313, '뢸镝蔥') PER PARTITION LIMIT 297 LIMIT 954", keyspace);
        cluster.coordinator(1).execute(select, ConsistencyLevel.ALL);

        select = withKeyspace("SELECT pk0, pk1, ck0 FROM %s.tbl WHERE pk0 = 7137864754153440313 PER PARTITION LIMIT 21 LIMIT 914 ALLOW FILTERING", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 1);

        assertRows(pagingResult, row(7137864754153440313L, "뢸镝蔥", 7732824726196172505L));
    }

    @Test
    public void testTokenRangeOnFullPartitionKeysWithPerPartitionLimitEmpty()
    {
        String keyspace = "token_range_per_partition_limit_empty";
        createTrackedKeyspace(keyspace);
        cluster.schemaChange(withKeyspace("CREATE TYPE IF NOT EXISTS %s.\"6iiPTW_Oe1eyqpNyLtoSbn\" (f0 smallint, f1 uuid)", keyspace));
        cluster.schemaChange(withKeyspace("CREATE TYPE IF NOT EXISTS %s.\"tjQi_gfccLmvemLRbkg\" (f0 uuid)", keyspace));

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 smallint, pk1 double, ck0 int, s0 text static, s1 map<frozen<map<time, double>>, bigint> static, " +
                                          "v0 frozen<map<timestamp, timeuuid>>, v1 frozen<set<uuid>>, v2 uuid, v3 frozen<tuple<vector<date, 1>, frozen<\"6iiPTW_Oe1eyqpNyLtoSbn\">, " +
                                          "frozen<\"tjQi_gfccLmvemLRbkg\">>>, v4 smallint, PRIMARY KEY ((pk0, pk1), ck0)) WITH CLUSTERING ORDER BY (ck0 ASC) AND read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(2).executeInternal(withKeyspace("DELETE s1 FROM %s.tbl USING TIMESTAMP 1 WHERE pk0 = 4217 AND  pk1 = -2.2644046491088394E265", keyspace));
        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, s1) VALUES (-16150, 1.0086497658456055E-263, {{'07:58:45.097000261': -2.1560404491129945E225}: 588520316827010420}) USING TIMESTAMP 2", keyspace));
        cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, s0, s1, v0) " +
                                                    "VALUES (4217, -2.2644046491088394E265, -2077196678, '᱔惔겎꣘', null, {'1972-11-15T21:50:31.510Z': 00000000-0000-1100-aa00-000000000000, '1973-10-01T03:02:11.345Z': 00000000-0000-1900-b500-000000000000, '2053-09-18T06:21:05.430Z': 00000000-0000-1900-a100-000000000000}) USING TIMESTAMP 3", keyspace));

        String select = withKeyspace("SELECT * FROM %s.tbl WHERE token(pk0, pk1) >= -9223372036854775808 AND token(pk0, pk1) < -3253266623840194343 PER PARTITION LIMIT 995 LIMIT 950", keyspace);
        cluster.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 5000);

        // TODO: This seems to fail only sporadically. It may not add value, and we could remove it after CASSANDRA-20954 if we feel there is enough coverage otherwise...
        select = withKeyspace("SELECT * FROM %s.tbl WHERE token(pk0, pk1) > token(4217, -2.2644046491088394E265) AND token(pk0, pk1) < token(-16150, 1.0086497658456055E-263) PER PARTITION LIMIT 89 LIMIT 832", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 10);
        assertRows(pagingResult);
    }

    /*
    INFO  [node2_isolatedExecutor:1] 2025-10-21T17:32:27,840 SubstituteLogger.java:222 - ERROR [node2_isolatedExecutor:1] node2 2025-10-21T17:32:27,832 JVMStabilityInspector.java:72 - Exception in thread Thread[node2_isolatedExecutor:1,5,isolatedExecutor]
    java.lang.IllegalStateException: Multiple partitions received for DecoratedKey(2680073734780247800, 000253ed0000100000000000004100ba0000000000000000)
        at org.apache.cassandra.db.partitions.PartitionIterators$1.reduce(PartitionIterators.java:126)
        at org.apache.cassandra.db.partitions.PartitionIterators$1.reduce(PartitionIterators.java:112)
        at org.apache.cassandra.utils.MergeIterator$Candidate.consume(MergeIterator.java:439)
        at org.apache.cassandra.utils.MergeIterator$ManyToOne.consume(MergeIterator.java:242)
        at org.apache.cassandra.utils.MergeIterator$ManyToOne.computeNext(MergeIterator.java:186)
        at org.apache.cassandra.utils.AbstractIterator.hasNext(AbstractIterator.java:47)
        at org.apache.cassandra.db.partitions.PartitionIterators$2.computeNext(PartitionIterators.java:145)
        at org.apache.cassandra.db.partitions.PartitionIterators$2.computeNext(PartitionIterators.java:141)
     */
    @Test
    public void testTokenRangeOnFullPartitionKeysWithPerPartitionLimitNonEmpty()
    {
        String keyspace = "token_range_per_partition_limit_non_empty";
        createTrackedKeyspace(keyspace);
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 smallint, pk1 uuid, ck0 'org.apache.cassandra.db.marshal.LexicalUUIDType', ck1 timeuuid, v0 int, PRIMARY KEY ((pk0, pk1), ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 DESC) AND read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());
        
        cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl", keyspace), ConsistencyLevel.ALL);
        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v0) VALUES (24199, 00000000-0000-4900-9c00-000000000000, 0x0000000000001800b700000000000000, 00000000-0000-1000-8f00-000000000000, 1) USING TIMESTAMP 1", keyspace));

        cluster.get(3).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 2 WHERE pk0 = -16322 AND pk1 = 00000000-0000-4400-ba00-000000000000", keyspace));
        cluster.get(3).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 3 SET v0=2 WHERE  pk0 = 24199 AND pk1 = 00000000-0000-4900-9c00-000000000000 AND  ck0 IN (0x00000000000015008100000000000000) AND ck1 = 00000000-0000-1b00-bd00-000000000000", keyspace));

        cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v0) VALUES (21485, 00000000-0000-4100-ba00-000000000000, 0x0000000000004c00a900000000000000, 00000000-0000-1200-b700-000000000000, 3) USING TIMESTAMP 4", keyspace));

        String select = withKeyspace("SELECT pk0 FROM %s.tbl WHERE token(pk0, pk1) >= token(24199, 00000000-0000-4900-9c00-000000000000) AND token(pk0, pk1) <= token(21485, 00000000-0000-4100-ba00-000000000000) PER PARTITION LIMIT 139 LIMIT 587", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(2).executeWithPaging(select, ConsistencyLevel.ALL, 100);
        assertRows(pagingResult, row((short) 24199), row((short) 24199), row((short) 21485));
    }

    @Test
    public void testTextRangeFilterWithHighLimit()
    {
        String keyspace = "text_range_filter_with_high_limit";
        createTrackedKeyspace(keyspace);
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 bigint, pk1 smallint, ck0 inet, ck1 double, v3 text, PRIMARY KEY ((pk0, pk1), ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 ASC) AND read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 1 WHERE pk0 = -3279716623783136579 AND  pk1 = -25927", keyspace));
        cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v3) VALUES (3754566280912306098, -28139, '9c05:10e3:8a10:dd12:b357:6f0b:736b:c3d', 6.248336852153311E-201 * -1.711074442164963E-123, '⩭爭ᣪ흟赃') USING TIMESTAMP 3", keyspace));

        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v3) " +
                                                    "VALUES (-3279716623783136579, -25927, '9191:f315:92eb:f9b8:ebbe:6456:10f4:ca6c', -1.8918823041672677E168 - -3.900839250480109E-214, '吮植' + '䛆') USING TIMESTAMP 4", keyspace));

        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v3) VALUES (5882007412747503201, 3756, '4a4b:7deb:98f4:a0ab:f5d0:43f:ab2b:2628', 6.334562923798137E276 * -4.6068109424772055E-29, '㺍ັୁ' + '䝱\u000E݂ụ') USING TIMESTAMP 6", keyspace));

        String select = withKeyspace("SELECT * FROM %s.tbl WHERE pk0 > 5882007412747503201 LIMIT 764 ALLOW FILTERING", keyspace);
        cluster.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1);

        select = withKeyspace("SELECT pk0, pk1 FROM %s.tbl WHERE v3 > '브ﭶ熒讘ꯄ謏??䎸锭商Ử豫羀펛葕䝆㛔' LIMIT 785 ALLOW FILTERING", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(2).executeWithPaging(select, ConsistencyLevel.ALL, 1);
        assertRows(pagingResult, row(3754566280912306098L, (short) -28139));
    }

    /*
    INFO  [node2_ReadStage-2] 2025-10-21T16:33:02,997 SubstituteLogger.java:222 - ERROR 11:33:02,996 Error while processing read
    java.lang.NullPointerException: null
        at org.apache.cassandra.service.reads.tracked.FilteredFollowupRead.lambda$start$1(FilteredFollowupRead.java:155)
        at org.apache.cassandra.utils.concurrent.ListenerList$CallbackBiConsumerListener.run(ListenerList.java:267)
        at org.apache.cassandra.concurrent.ImmediateExecutor.execute(ImmediateExecutor.java:140)
        at org.apache.cassandra.utils.concurrent.ListenerList.safeExecute(ListenerList.java:190)
        at org.apache.cassandra.utils.concurrent.ListenerList.notifyListener(ListenerList.java:181)
        at org.apache.cassandra.utils.concurrent.ListenerList$CallbackBiConsumerListener.notifySelf(ListenerList.java:274)
        at org.apache.cassandra.utils.concurrent.ListenerList.lambda$notifyExclusive$0(ListenerList.java:148)
        at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:242)
        at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:235)
        at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:225)
        at org.apache.cassandra.utils.concurrent.ListenerList.notifyExclusive(ListenerList.java:148)
        at org.apache.cassandra.utils.concurrent.ListenerList.notify(ListenerList.java:113)
        at org.apache.cassandra.utils.concurrent.AsyncFuture.trySet(AsyncFuture.java:102)
        at org.apache.cassandra.utils.concurrent.AbstractFuture.trySuccess(AbstractFuture.java:143)
        at org.apache.cassandra.utils.concurrent.FutureCombiner.trySuccess(FutureCombiner.java:189)
        at org.apache.cassandra.utils.concurrent.FutureCombiner$Listener.onCompletion(FutureCombiner.java:81)
        at org.apache.cassandra.utils.concurrent.FutureCombiner$Listener.operationComplete(FutureCombiner.java:76)
        at org.apache.cassandra.utils.concurrent.FutureCombiner$FailFastListener.operationComplete(FutureCombiner.java:107)
        at org.apache.cassandra.utils.concurrent.ListenerList.notifyListener(ListenerList.java:158)
        at org.apache.cassandra.utils.concurrent.ListenerList.notifyListener(ListenerList.java:172)
        at org.apache.cassandra.utils.concurrent.ListenerList$GenericFutureListenerList.notifySelf(ListenerList.java:214)
        at org.apache.cassandra.utils.concurrent.ListenerList.lambda$notifyExclusive$0(ListenerList.java:148)
        at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:242)
        at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:235)
        at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:225)
        at org.apache.cassandra.utils.concurrent.ListenerList.notifyExclusive(ListenerList.java:148)
        at org.apache.cassandra.utils.concurrent.ListenerList.notify(ListenerList.java:113)
        at org.apache.cassandra.utils.concurrent.AsyncFuture.trySet(AsyncFuture.java:102)
        at org.apache.cassandra.utils.concurrent.AbstractFuture.trySuccess(AbstractFuture.java:143)
        at org.apache.cassandra.utils.concurrent.AsyncPromise.trySuccess(AsyncPromise.java:117)
        at org.apache.cassandra.service.reads.tracked.TrackedRead.onResponse(TrackedRead.java:339)
        at org.apache.cassandra.service.reads.tracked.TrackedRead.lambda$start$2(TrackedRead.java:291)
        at org.apache.cassandra.utils.concurrent.ListenerList$CallbackBiConsumerListener.run(ListenerList.java:267)
        at org.apache.cassandra.concurrent.ImmediateExecutor.execute(ImmediateExecutor.java:140)
        at org.apache.cassandra.utils.concurrent.ListenerList.safeExecute(ListenerList.java:190)
        at org.apache.cassandra.utils.concurrent.ListenerList.notifyListener(ListenerList.java:181)
        at org.apache.cassandra.utils.concurrent.ListenerList$CallbackBiConsumerListener.notifySelf(ListenerList.java:274)
        at org.apache.cassandra.utils.concurrent.ListenerList.lambda$notifyExclusive$0(ListenerList.java:148)
        at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:242)
        at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:235)
        at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:225)
        at org.apache.cassandra.utils.concurrent.ListenerList.notifyExclusive(ListenerList.java:148)
        at org.apache.cassandra.utils.concurrent.ListenerList.notify(ListenerList.java:113)
        at org.apache.cassandra.utils.concurrent.AsyncFuture.trySet(AsyncFuture.java:102)
        at org.apache.cassandra.utils.concurrent.AbstractFuture.trySuccess(AbstractFuture.java:143)
        at org.apache.cassandra.utils.concurrent.AsyncPromise.trySuccess(AsyncPromise.java:117)
        at org.apache.cassandra.service.reads.tracked.TrackedLocalReads$Coordinator.complete(TrackedLocalReads.java:252)
     */
    @Test
    public void testRangeFilterOnFrozenSetNoLimit()
    {
        String keyspace = "range_filter_on_frozen_set_no_limit";
        createTrackedKeyspace(keyspace);

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 int, pk1 boolean, ck0 inet, v1 int, v4 frozen<set<bigint>>, PRIMARY KEY ((pk0, pk1), ck0)) WITH CLUSTERING ORDER BY (ck0 DESC) AND read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 3 SET v4={-4237118076428244729, -1815831816430314156} " +
                                                    "WHERE pk0 = -1256431887 AND pk1 = true AND ck0 IN ('c50:5c4d:35cb:1739:f958:8f83:5d95:963d', '7bf6:c19e:d3f2:8679:b3b3:377f:1ac8:1416', 'd035:5ffc:960c:1b8c:f4ed:a2cf:73f6:af9c')", keyspace));
        cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, v4) VALUES (-639885536, false, '238.234.202.249', {8383242616920701144}) USING TIMESTAMP 4", keyspace));

        String select = withKeyspace("SELECT * FROM %s.tbl WHERE v1 = 3 ALLOW FILTERING", keyspace);
        cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 100);

        select = withKeyspace("SELECT * FROM %s.tbl WHERE v1 <= 3 LIMIT 175 ALLOW FILTERING", keyspace);
        cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 1);

        cluster.get(3).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 7 SET v4={7721973864222015806} WHERE  pk0 = -1256431887 AND  pk1 = true AND  ck0 = 'b318:85d4:d6a0:907:ff1e:9262:9635:ccfa'", keyspace));
        cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 8 WHERE  pk0 = -639885536 AND  pk1 = false", keyspace));

        select = withKeyspace("SELECT pk0, pk1 FROM %s.tbl WHERE v4 > {-4237118076428244729, -1815831816430314156} ALLOW FILTERING", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(2).executeWithPaging(select, ConsistencyLevel.ALL, 5000);
        assertRows(pagingResult, row(-1256431887, true));
    }

    /** Enough partitions that each of the three primary ranges holds a few dozen of them. */
    private static final int PARTITIONS = 100;

    /**
     * A full table scan from every coordinator, over a hundred partitions written at ALL. The answer is the whole
     * table in both modes, and the reason it is interesting differs between them: at {@code replication_factor: 3}
     * every node is a full replica of the whole ring and every coordinator holds every partition, so a short answer
     * is rows lost on the read path. Under {@code '3/1'} each node is the witness of one of the three primary
     * ranges, so the scan covers a range that no one replica is full for, and a tracked read takes data from
     * exactly one replica per range - whichever node coordinates, and whichever replica each of its range plans
     * picks, the answer still has to be the whole table.
     * <p>
     * ReplicaPlans.maybeMerge merged adjacent range plans by matching their replicas on endpoint alone, so a node
     * that is full for one range and a witness of the next was described as full for the merged range and the read
     * asked a witness for data it does not keep. Coordinated on node 1 the scan came back with 68 of the 100
     * partitions, which is exactly the number node 1 is a full replica of.
     */
    @Test
    public void testFullTableScanFromEveryCoordinator()
    {
        String keyspace = "full_table_scan_every_coordinator";
        createTrackedKeyspace(keyspace);
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int) WITH read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        Map<Integer, Integer> expected = new TreeMap<>();
        for (int pk = 0; pk < PARTITIONS; pk++)
        {
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)", keyspace), ConsistencyLevel.ALL, pk, pk);
            expected.put(pk, pk);
        }

        assertEveryNodeHoldsWhatTheModeSays(keyspace);

        for (int node = 1; node <= REPLICAS; node++)
        {
            Object[][] rows = cluster.coordinator(node).execute(withKeyspace("SELECT pk, v FROM %s.tbl", keyspace), ConsistencyLevel.ALL);
            Map<Integer, Integer> actual = new TreeMap<>();
            for (Object[] row : rows)
                Assert.assertNull("partition " + row[0] + " returned twice", actual.put((Integer) row[0], (Integer) row[1]));
            Assert.assertEquals("full table scan coordinated on node " + node, expected, actual);
        }
    }

    /**
     * The unstressed case check for {@link #testFullTableScanFromEveryCoordinator}, made with
     * {@code executeInternal} so that it cannot reconcile away the state it is measuring. The two modes want
     * opposite things of it, and each is the claim that makes a short coordinated answer in that mode a defect:
     * at {@code replication_factor: 3} every node has to hold every partition, so nothing is missing anywhere and
     * losing a row is the read path's doing; under {@code '3/1'} no node may hold all of them, because a node that
     * held the whole table would be witnessing none of it and a scan reading data from one replica would be right
     * however the range was split. Holding none of them would be just as wrong, and means the node is a full
     * replica of nothing.
     */
    private static void assertEveryNodeHoldsWhatTheModeSays(String keyspace)
    {
        for (int node = 1; node <= REPLICAS; node++)
        {
            int local = nodeLocal(keyspace, node, "SELECT pk FROM %s.tbl").length;
            if (mode == Mode.FULL)
            {
                Assert.assertEquals("node " + node + " does not hold all " + PARTITIONS + " partitions", PARTITIONS, local);
            }
            else
            {
                Assert.assertTrue("Not stressed: node " + node + " holds all " + PARTITIONS + " partitions, so it witnesses none of them",
                                  local < PARTITIONS);
                Assert.assertTrue("node " + node + " holds no data at all, so it is not a full replica of anything",
                                  local > 0);
            }
        }
    }

    /*
     * Everything below shares one harness. Each case runs its query twice, against two keyspaces that differ
     * only in replication_type, over identical data written the identical way, and asserts the tracked answer
     * equals the untracked one. No expected result is written down anywhere, so a mismatch is attributable to
     * mutation tracking and nothing else, and no case can be scored wrong because the expectation was guessed.
     *
     * Each case also has to earn its place, which is what the probe argument is for. It runs on the tracked
     * keyspace after the writes and before the read under test, and every assertion in it is a node local
     * executeInternal that never enters StorageProxy and so cannot reconcile away the state it is measuring.
     * A case built on divergent replicas proves there that the divergence is still present and that the data
     * replica could not have answered on its own; a case built on convergent ones proves that every replica
     * already holds everything the answer needs, which is what separates a wrong answer from absent data.
     */

    /** {@code (pk0, pk1)} is the partition key and {@code v} the filtered non primary key column. */
    private static final String TABLE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

    private static final String TABLE_WITH_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

    private static final String TABLE_WITH_FROZEN_SET =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, fs frozen<set<int>>, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

    private static final String TABLE_WITH_INDEXED_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_pk0 ON %s.tbl(pk0) USING 'SAI';" +
        "CREATE INDEX tbl_s ON %s.tbl(s) USING 'SAI'";

    /** {@code v} is indexed and {@code w} is not, so a filter on {@code w} is left for the read to apply itself. */
    private static final String TABLE_WITH_INDEXED_VALUE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, w int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_v ON %s.tbl(v) USING 'SAI'";

    /**
     * A clustering column index over a table that also has a static column, so an update setting both carries a static
     * row that an expression on a clustering column can not be evaluated against. Legacy 2i because SAI indexes a
     * static column and a clustering column with separate index terms and never asks one about the other.
     */
    private static final String TABLE_WITH_INDEXED_CLUSTERING_AND_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_ck ON %s.tbl(ck) USING 'legacy_local_table'";

    /**
     * A partition key column index over a table that also has a static column. Legacy 2i indexes the static row for
     * such an index deliberately, so that a partition holding nothing but static data is still discoverable.
     */
    private static final String TABLE_WITH_INDEXED_PARTITION_KEY_AND_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_pk0 ON %s.tbl(pk0) USING 'legacy_local_table'";

    private static final String FILTER = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 ALLOW FILTERING";

    /** Passed as a page size to read the whole range in one request. */
    private static final int UNPAGED = 0;

    /**
     * Node 1 holds (1,'a') with the value the filter rejects and node 2 the newer value it accepts; node 3 holds
     * neither. Node 1 coordinates, so it is the data replica for its own stale view, and reconciliation has to
     * deliver a mutation for a key that replica has already materialized and thrown away.
     */
    private static final String[] SOLE_PARTITION_STALE_ON_NODE_1 =
    {
        "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 1) USING TIMESTAMP 10",
        "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
    };

    /**
     * As {@link #SOLE_PARTITION_STALE_ON_NODE_1}, plus a partition (2,'b') that node 1 already holds a matching
     * row for, so the data replica's materialized data is not empty and the reconciled result set is two rows.
     */
    private static final String[] TWO_PARTITIONS_ONE_STALE_ON_NODE_1 =
    {
        "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 1) USING TIMESTAMP 10",
        "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (2, 'b', 1, 900) USING TIMESTAMP 11",
        "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
    };

    /**
     * Three partitions written through the coordinator at ALL, so every replica holds identical data and
     * reconciliation has nothing to do, plus a partition level tombstone on (2,'b') older than the row that
     * partition contains. With the default Murmur3 partitioner a range scan visits (3,'c'), then (2,'b'), then
     * (1,'a'), so the tombstoned partition is reached before the only one satisfying {@code v > 100}.
     */
    private static final String[] TOMBSTONED_PARTITION_BEFORE_THE_MATCH =
    {
        "*:DELETE FROM %s.tbl USING TIMESTAMP 5 WHERE pk0 = 2 AND pk1 = 'b'",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 500) USING TIMESTAMP 10",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (2, 'b', 1, 1) USING TIMESTAMP 11",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 1, 2) USING TIMESTAMP 12"
    };

    /** {@link #TOMBSTONED_PARTITION_BEFORE_THE_MATCH} with the tombstone left out and nothing else changed. */
    private static final String[] SAME_PARTITIONS_WITHOUT_THE_TOMBSTONE =
        Arrays.copyOfRange(TOMBSTONED_PARTITION_BEFORE_THE_MATCH, 1, TOMBSTONED_PARTITION_BEFORE_THE_MATCH.length);

    /**
     * Node 1 holds a match in (1,'a') and a row in (2,'b') that the filter rejects; node 2 holds the newer value that
     * makes (2,'b') match. With the default Murmur3 partitioner (2,'b') sorts before (1,'a'), so the key
     * reconciliation flags sorts ahead of the one partition the read kept, and the row it contributes belongs in
     * front of the row already counted rather than after it.
     */
    private static final String[] INTERLEAVING_STALE_PARTITION_ON_NODE_1 =
    {
        "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 900) USING TIMESTAMP 10",
        "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (2, 'b', 1, 1) USING TIMESTAMP 11",
        "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 500 WHERE pk0 = 2 AND pk1 = 'b' AND ck = 1"
    };

    /**
     * Writes the same data to a tracked keyspace and to an otherwise identical untracked one, reads the untracked
     * one for the expected answer, runs {@code probe} against the tracked one, and asserts the tracked read
     * returns what the untracked read did.
     *
     * @param pageSize the page size to read at, or {@link #UNPAGED}
     * @param probe    given the tracked keyspace and the oracle's answer, run after the writes and before the read
     * @return the tracked keyspace, for any assertion a case wants to make about what the read left behind
     */
    private static String assertTrackedMatchesOracle(String name, String table, String[] writes, String select,
                                                     int pageSize, BiConsumer<String, Object[][]> probe)
    {
        String untracked = createKeyspace(name + "_oracle", table, false);
        write(untracked, writes);
        Object[][] expected = read(untracked, select, pageSize);

        String tracked = createKeyspace(name, table, true);
        write(tracked, writes);
        probe.accept(tracked, expected);

        assertRows(read(tracked, select, pageSize), expected);
        return tracked;
    }

    private static String createKeyspace(String keyspace, String table, boolean tracked)
    {
        // the oracle is fully replicated in both modes. Witnessing decides where a mutation is applied, not what the
        // answer to a query over it is, so the keyspace that defines the correct answer is deliberately left at
        // replication_factor 3 under witnesses too: comparing a witnessed tracked read against a fully replicated
        // untracked one is the strongest form of the assertion available, not a relaxed one.
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = "
                                          + (tracked ? mode.replication + " AND replication_type='tracked'"
                                                     : Mode.FULL.replication), keyspace));
        // a table definition may carry index DDL after the CREATE TABLE, one statement per semicolon
        for (String statement : table.split(";"))
            cluster.schemaChange(withKeyspace(statement, keyspace));
        // an index is not queryable until every replica has finished building it, and a read that reaches one that
        // has not fails with INDEX_BUILD_IN_PROGRESS rather than waiting for it; for a keyspace with no index at all
        // this finds nothing to wait for and returns
        SAIUtil.waitForIndexQueryable(cluster, keyspace);
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());
        return keyspace;
    }

    /**
     * A statement prefixed with a node number is applied with {@code executeInternal}, which lands it on that node
     * alone and leaves the replicas divergent. One prefixed with {@code *} goes through the coordinator at ALL and
     * leaves them identical.
     * <p>
     * Neither prefix escapes witnessing. {@code Keyspace.applyInternalTracked} sits below the coordinator, and when
     * the strategy has transient replicas and the update's token is in none of the node's full local ranges it
     * journals the mutation and skips the write to the table. So under {@link Mode#WITNESSES} a write aimed at a node
     * that merely witnesses its token is invisible to a node local read there and is still available to
     * reconciliation, and a {@code *} write is materialized only on the two replicas that are full for its token.
     * That is why the unstressed case checks consult placement rather than assume a write landed where it was sent.
     */
    private static void write(String keyspace, String[] writes)
    {
        for (String write : writes)
        {
            int colon = write.indexOf(':');
            String target = write.substring(0, colon);
            String cql = withKeyspace(write.substring(colon + 1), keyspace);
            if (target.equals("*"))
                cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL);
            else
                cluster.get(Integer.parseInt(target)).executeInternal(cql);
        }
    }

    private static Object[][] read(String keyspace, String select, int pageSize)
    {
        String cql = withKeyspace(select, keyspace);
        if (pageSize == UNPAGED)
            return cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL);

        List<Object[]> rows = new ArrayList<>();
        Iterator<Object[]> paged = cluster.coordinator(1).executeWithPaging(cql, ConsistencyLevel.ALL, pageSize);
        while (paged.hasNext())
            rows.add(paged.next());
        return rows.toArray(new Object[0][]);
    }

    /** What one node answers on its own, off its own memtables and sstables, reconciling nothing. */
    private static Object[][] nodeLocal(String keyspace, int node, String select)
    {
        return cluster.get(node).executeInternal(withKeyspace(select, keyspace));
    }

    /**
     * The nodes that are full replicas of the partition {@code (pk0, pk1)}, decided by the same question
     * {@code Keyspace.applyInternalTracked} asks before it writes: is the token in one of this node's full local
     * ranges. Every table the harness uses has that partition key, and reading the answer out of the replication
     * strategy rather than off a hardcoded ring means the model cannot drift from the placement the writes actually
     * got. Under {@link Mode#FULL} the strategy has no transient replicas, every node's full local ranges cover the
     * ring, and this is always all three nodes.
     */
    private static Set<Integer> fullReplicasFor(String keyspace, int pk0, String pk1)
    {
        Set<Integer> full = new TreeSet<>();
        for (int node = 1; node <= REPLICAS; node++)
        {
            boolean isFull = cluster.get(node).callOnInstance(() -> {
                AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
                Token token = DatabaseDescriptor.getPartitioner()
                                                .getToken(CompositeType.build(ByteBufferAccessor.instance,
                                                                              Int32Type.instance.decompose(pk0),
                                                                              UTF8Type.instance.decompose(pk1)));
                for (Range<Token> range : strategy.getLocalRanges(ClusterMetadata.current()).onlyFull().ranges())
                    if (range.contains(token))
                        return true;
                return false;
            });
            if (isFull)
                full.add(node);
        }
        return full;
    }

    /**
     * The rows of {@code rows} that node {@code node} can possibly hold in its own memtables and sstables: the ones
     * whose partition it is a full replica of. Under {@link Mode#FULL} that is every row, so a per node expectation
     * built with this is the whole set and every assertion made against one says exactly what it said before this
     * suite had a second mode.
     * <p>
     * The first two columns of every row are {@code pk0} and {@code pk1}, which every select the harness hands to a
     * placement aware check begins with.
     */
    private static Object[][] materializedOn(String keyspace, int node, Object[][] rows)
    {
        List<Object[]> held = new ArrayList<>();
        for (Object[] row : rows)
            if (fullReplicasFor(keyspace, (Integer) row[0], (String) row[1]).contains(node))
                held.add(row);
        return held.toArray(new Object[0][]);
    }

    /**
     * The unstressed case check, for the cases built on divergent replicas. If what the data replica holds on its own
     * already answers the query then the read never has to reconcile anything, and the case would pass with the read
     * path completely broken.
     * <p>
     * Node 1 is the data replica of every one of these reads, and deterministically so: {@link #read} coordinates on
     * node 1, TrackedRead.start prefers the local replica whenever it is a full one, and at RF=3 on three nodes every
     * node is a full replica of every range. So the replica whose materialized data the answer is built from is the
     * one these fixtures leave stale, and it is the same one every run.
     * <p>
     * Under {@link Mode#WITNESSES} node 1 is a full replica of two of the three primary ranges rather than all of
     * them, so it is still the data replica of those two and the third is read from whichever full replica that
     * range's plan picks. The assertion below is unchanged and still holds - the coordinator's own materialized data
     * is not the answer either way - but it is no longer the whole story, because node 1 could also be short simply
     * by witnessing part of the range. {@link #assertWitnessesMaterializedNothing} is what says which of the two it
     * is, per partition and against placement.
     */
    private static void assertDataReplicaCannotAnswerAlone(String keyspace, String select, Object[][] oracle)
    {
        Assert.assertFalse("Not stressed: the data replica answers this query correctly on its own",
                           Arrays.deepEquals(nodeLocal(keyspace, 1, select), oracle));

        if (mode == Mode.WITNESSES)
            assertWitnessesMaterializedNothing(keyspace);
    }

    /**
     * The witness half of {@link #assertDataReplicaCannotAnswerAlone}: every partition any node holds is one that
     * node is a full replica of, and at least one partition is held somewhere, so at least one node is missing a
     * partition for no reason other than witnessing its token. That is the layout the mode exists to test, asserted
     * where it can still be seen: a witness journals a mutation and skips the table, so a partition it witnesses can
     * only reach the read through reconciliation, whoever the plan asks for data.
     * <p>
     * Asserting it here rather than trusting it is what keeps the mode conditional above honest. If witnesses ever
     * started materializing what they journal, these fixtures would quietly stop being divergent under {@code '3/1'}
     * and every case would pass for the wrong reason.
     */
    private static void assertWitnessesMaterializedNothing(String keyspace)
    {
        Set<List<Object>> anywhere = new HashSet<>();
        for (int node = 1; node <= REPLICAS; node++)
        {
            for (Object[] partition : nodeLocal(keyspace, node, "SELECT DISTINCT pk0, pk1 FROM %s.tbl"))
            {
                Assert.assertTrue("node " + node + " materialized (" + partition[0] + ",'" + partition[1] + "') and is only a witness of it",
                                  fullReplicasFor(keyspace, (Integer) partition[0], (String) partition[1]).contains(node));
                anywhere.add(Arrays.asList(partition));
            }
        }
        // every token has exactly one witness among three nodes under '3/1', so one materialized partition anywhere
        // is one partition that some node is missing because it witnesses it
        Assert.assertFalse("Not stressed: no node materialized any partition, so nothing is being witnessed",
                           anywhere.isEmpty());
    }

    /**
     * The converse, for the cases that write through the coordinator and so have no divergence in them: every
     * replica already holds everything the answer needs, which is what makes a short coordinator answer a defect
     * in the read path rather than data that was never there.
     * <p>
     * Under {@link Mode#WITNESSES} "everything the answer needs" is per node rather than global, because a write at
     * ALL is applied only on the two replicas that are full for its token. So each node is held to the part of the
     * answer it is a full replica of, which is the same assertion with the mode's placement substituted into it
     * rather than a weaker one: no node may be missing a row it is responsible for, and no node may return a row it
     * has no business holding. Their union is still the whole answer, since every token has two full replicas.
     * <p>
     * The last assertion is the unstressed case check, and it is the same claim read in each mode's direction. At
     * {@code replication_factor: 3} no node may be short, or the fixture did not converge. Under {@code '3/1'} some
     * node must be short, or nothing is being witnessed and the case is the other mode over again.
     */
    private static void assertEveryReplicaCanAnswerAlone(String keyspace, String select, Object[][] oracle)
    {
        boolean someNodeShort = false;
        for (int node = 1; node <= REPLICAS; node++)
        {
            Object[][] expected = materializedOn(keyspace, node, oracle);
            assertRows(nodeLocal(keyspace, node, select), expected);
            someNodeShort |= expected.length < oracle.length;
        }
        Assert.assertEquals(mode == Mode.FULL ? "A replica is missing part of the answer, so the fixture did not converge"
                                              : "Not stressed: every replica holds the whole answer, so nothing is being witnessed",
                            mode == Mode.WITNESSES, someNodeShort);
    }

    /**
     * A row filtered range read where the data replica filters out every partition it can see locally, and
     * reconciliation then hands it a mutation for one of those filtered partitions. The mutation does not satisfy
     * the row filter either, so no follow up read is needed, but the read was still augmented and therefore still
     * takes the extending path.
     * <p>
     * PartialTrackedRangeRead.Filtered.FilteredCompleted.extendRead read the last matching key out of the empty
     * branch of its ternary, so it called TreeMap.lastKey() on the map it had just tested for emptiness:
     * <pre>
     * java.util.NoSuchElementException: null
     *     at java.base/java.util.TreeMap.key(TreeMap.java:1324)
     *     at java.base/java.util.TreeMap.lastKey(TreeMap.java:296)
     *     at org.apache.cassandra.service.reads.tracked.PartialTrackedRangeRead$Filtered$FilteredCompleted.extendRead(PartialTrackedRangeRead.java:561)
     *     at org.apache.cassandra.service.reads.tracked.PartialTrackedRangeRead$RangeCompleted.createResult(PartialTrackedRangeRead.java:290)
     * </pre>
     * The oracle's answer is empty, which the data replica also answers on its own, so the unstressed case check
     * cannot tell this case apart from a vacuous one and the probe asserts the divergence directly instead. That
     * makes it the one probe that names the rows a node holds, so it is also the one that has to ask where a row
     * is allowed to be: node 2 is a witness of (1,'a') under {@link Mode#WITNESSES}, and journals the newer value
     * without applying it, so its copy of the row is present in one mode and absent in the other while the
     * divergence the case needs - node 1 stale, the newer value only reachable by reconciling - is identical.
     */
    @Test
    public void testFilteredRangeReadWhereEveryLocalPartitionIsFilteredOut()
    {
        String everything = "SELECT pk0, pk1, ck, v FROM %s.tbl";
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 1) USING TIMESTAMP 10",
            // reconciliation delivers this to node 1, which augments the read even though the update is filtered out too
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 2 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
        };

        String tracked = assertTrackedMatchesOracle("b_all_locals_filtered", TABLE, writes, FILTER, UNPAGED, (keyspace, oracle) -> {
            // the defect is in the branch taken when the filter empties a non empty local map, so the data replica
            // has to have materialized the partition in the first place
            Assert.assertTrue("(1,'a') is only witnessed by the data replica, so its local map is empty for another reason",
                              fullReplicasFor(keyspace, 1, "a").contains(1));
            assertRows(nodeLocal(keyspace, 1, everything), row(1, "a", 1, 1));
            assertRows(nodeLocal(keyspace, 2, everything), materializedOn(keyspace, 2, new Object[][]{ row(1, "a", 1, 2) }));
            assertRows(nodeLocal(keyspace, 3, everything));
        });

        // and the read did reconcile: node 1 now holds the value node 2 had, so the empty answer is not vacuous
        assertRows(nodeLocal(tracked, 1, everything), row(1, "a", 1, 2));
    }

    /**
     * A static value on the diverged partition makes the otherwise identical failing case pass, because the
     * partition is no longer dropped from the materialized map. Worth keeping because it means a schema with
     * static columns hides the defect above, so a fix must not be validated only against tables without one.
     */
    @Test
    public void testFilteredRangeReadWithAStaticColumn()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 1) USING TIMESTAMP 10",
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE v > 100 ALLOW FILTERING";
        assertTrackedMatchesOracle("b_static_column", TABLE_WITH_STATIC, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The direction of a frozen collection predicate decides whether the same shape is broken. Here node 1's local
     * row satisfies {@code fs > {1, 2}} and the row reconciliation delivers does not, so the data replica
     * materializes a matching partition and reconciliation takes it away again — the opposite of the case above,
     * and the direction that has always worked. A fix for one direction must not break the other.
     */
    @Test
    public void testFilteredRangeReadOnAFrozenSetInTheDirectionThatWorks()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, fs) VALUES (1, 'a', 1, {3}) USING TIMESTAMP 10",
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET fs = {1} WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
        };
        String select = "SELECT pk0, pk1, ck, fs FROM %s.tbl WHERE fs > {1, 2} ALLOW FILTERING";
        assertTrackedMatchesOracle("b_frozen_set_other_direction", TABLE_WITH_FROZEN_SET, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The same shape as {@link #testFilteredRangeReadWhereEveryLocalPartitionIsFilteredOut}, except that the
     * mutation reconciliation hands the data replica does satisfy the row filter. That records a follow up key, so
     * the read takes the FilteredFollowupRead path instead of finishing where it stands.
     * <p>
     * FilteredFollowupRead.start asks the nested range read it just started where to resume, through a consumer
     * that TrackedRead.start never invokes, so the reference is always null:
     * <pre>
     * java.lang.NullPointerException: null
     *     at org.apache.cassandra.service.reads.tracked.FilteredFollowupRead.lambda$start$1(FilteredFollowupRead.java:155)
     * </pre>
     * The replica logs it and never answers, so the coordinator fails with a ReadTimeoutException reporting 0
     * responses. The LIMIT matters: without one the command's limits are unlimited and
     * ExtendingCompletedRead.followUpReadRequired stops before reaching the nested read at all.
     */
    @Test
    public void testFilteredRangeReadWhereReconciliationRestoresTheOnlyMatch()
    {
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 LIMIT 10 ALLOW FILTERING";
        assertTrackedMatchesOracle("c_reconciled_only_match", TABLE, SOLE_PARTITION_STALE_ON_NODE_1, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The same follow up path reached with a limit that merely exceeds the reconciled result set rather than
     * dwarfing it: two rows come back and the limit is three. Reaching the path at all also needs the fix
     * {@link #testUnlimitedFilteredRangeReadWhereAFlaggedKeySortsLast} covers, because the key reconciliation
     * flagged here sorts after a partition the read kept.
     */
    @Test
    public void testFilteredRangeReadWithALimitLargerThanTheReconciledResult()
    {
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 LIMIT 3 ALLOW FILTERING";
        assertTrackedMatchesOracle("c_limit_exceeds_result", TABLE, TWO_PARTITIONS_ONE_STALE_ON_NODE_1, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The control for the two methods above, and the axis that separates them from it: reconciliation contributes to
     * a partition the data replica kept rather than to one it discarded, so nothing is recorded as a follow up key
     * and the limit is filled by the merge itself. No follow up read is requested and FilteredFollowupRead is never
     * constructed. It passed before the fixes and has to keep passing after them.
     * <p>
     * Node 2's row is the lowest clustering in the partition, so the two rows the limit admits are not the two the
     * data replica holds, which is what the probe checks.
     */
    @Test
    public void testFilteredRangeReadWithALimitTheReconciledResultSatisfies()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 2, 500) USING TIMESTAMP 10",
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 3, 600) USING TIMESTAMP 11",
            "2:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 700) USING TIMESTAMP 12"
        };
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 LIMIT 2 ALLOW FILTERING";
        assertTrackedMatchesOracle("c_limit_satisfied", TABLE, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * A filtered range read with no limit at all over two partitions, one of which the data replica
     * discards and reconciliation then shows a match in. The discarded partition is simply dropped and the query
     * returns the other one on its own.
     * <p>
     * A key the row filter dropped is inside the range the read already scanned, so a follow up read that resumes
     * the scan past the last key it saw can never revisit it: PartialTrackedRangeRead.Filtered.FilteredCompletedRead
     * is the only thing that will ever ask for it. It asked only when the key interleaved with the partitions the
     * read kept, or when short read protection independently wanted another round - and short read protection has no
     * reason to want one here, because this read did not stop early, it threw a partition away. So a flagged key
     * that sorts after everything the read kept was flagged and then forgotten.
     * <p>
     * With the default Murmur3 partitioner (2,'b') sorts before (1,'a'), so the kept partition is (2,'b') and the
     * discarded one is (1,'a'): not interleaved, and the read reached the end of its range.
     * {@link #testFilteredRangeReadWithALimitLargerThanTheReconciledResult} is the same defect reached with a LIMIT;
     * this one shows it does not need one, which is what separates it from the follow up read defect that one also
     * covers.
     */
    @Test
    public void testUnlimitedFilteredRangeReadWhereAFlaggedKeySortsLast()
    {
        assertTrackedMatchesOracle("f_flagged_key_sorts_last", TABLE, TWO_PARTITIONS_ONE_STALE_ON_NODE_1, FILTER, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * The control for reading a discarded partition back: what a follow up read fetches still has to survive the row
     * filter. Reconciliation decides which discarded keys to chase by asking the row filter how many matches an update
     * could contain, and that count is deliberately optimistic - it stops at the first expression the update satisfies,
     * so with two partition level expressions any update satisfying either one is chased. Here (1,'b') satisfies
     * {@code pk0 = 1} and not {@code s = 7} and is fetched in full, and only the filter standing between the follow up
     * read and the answer keeps it out of the result.
     * <p>
     * That matters more once a flagged key that sorts last is chased at all, because chasing them is no longer the
     * rare case.
     */
    @Test
    public void testFilteredRangeReadWhereAFollowUpKeyDoesNotMatchTheFilter()
    {
        String[] writes =
        {
            // (1,'a') matches only once reconciliation has delivered node 2's static value
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 1, 10) USING TIMESTAMP 10",
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET s = 7 WHERE pk0 = 1 AND pk1 = 'a'",
            // (1,'b') matches neither before nor after, but its update does satisfy the partition key expression
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'b', 1, 2, 20) USING TIMESTAMP 11",
            "2:UPDATE %s.tbl USING TIMESTAMP 21 SET s = 3 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7 LIMIT 10 ALLOW FILTERING";
        assertTrackedMatchesOracle("f_followup_key_not_matching", TABLE_WITH_STATIC, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * A flagged key that sorts ahead of the partitions the read kept, with a limit the initial result already fills.
     * The row it contributes belongs in front of a row that was counted, so it displaces that row rather than
     * extending the result past the limit, and the correct answer is the row the tracked read cannot see.
     * <p>
     * FilteredFollowupRead asks each flagged key for {@code command.limits().forShortReadRetry(toQuery)} rows, where
     * {@code toQuery} is what is left of the limit. Here nothing is left of it, so every flagged key was read with a
     * limit of zero: reached before its first row, so the partition came back empty and the answer reconciliation had
     * just contradicted stood. The key was queried - it interleaves, which is the one reason this path queries a key
     * with no budget left - and then asked for nothing.
     */
    @Test
    public void testFilteredRangeReadWhereAnInterleavingKeyDisplacesTheRowTheLimitAdmits()
    {
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 LIMIT 1 ALLOW FILTERING";
        assertTrackedMatchesOracle("l_interleaving_key_unpaged", TABLE, INTERLEAVING_STALE_PARTITION_ON_NODE_1, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The same defect without a LIMIT in the query at all: a page is a limit, and a page the initial result fills
     * leaves the flagged key nothing to be read with. The row is not merely returned on the wrong page, it is lost -
     * the next page resumes past the partition the first page returned, which sorts after the flagged key.
     */
    @Test
    public void testPagedFilteredRangeReadWhereAnInterleavingKeyDisplacesTheRowOnThePage()
    {
        assertTrackedMatchesOracle("l_interleaving_key_paged", TABLE, INTERLEAVING_STALE_PARTITION_ON_NODE_1, FILTER, 1,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * The same shape under GROUP BY, which is the case that also needs the limits' continuation state dropped. A
     * grouping state names the clustering the range read left off at in some other partition, and the flagged key's
     * read would resume that group against a partition it has nothing to do with.
     * <p>
     * Before this commit the query does not merely answer wrongly, it does not answer: the flagged key is asked for
     * zero rows, comes back empty, and the follow up round that produced it asks for another one on the same
     * unchanged bounds, which recurses until the request times out.
     */
    @Test
    public void testPagedGroupByRangeReadWhereAKeyIsFlagged()
    {
        String select = "SELECT pk0, pk1, count(*) FROM %s.tbl WHERE v > 100 GROUP BY pk0, pk1 ALLOW FILTERING";
        assertTrackedMatchesOracle("m_group_by_flagged_key", TABLE, INTERLEAVING_STALE_PARTITION_ON_NODE_1, select, 1,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The range scan reaches the tombstoned partition (2,'b') before the matching partition (1,'a'), the first page
     * comes back empty, and the coordinator treats an empty page as the end of the result set, so (1,'a') is never
     * looked at. No divergence, no reconciliation, no exception, just a successful wrong answer.
     * <p>
     * PartialTrackedRangeRead.Filtered.FilteredMaterializer.filter asked whether the row filter had left anything
     * behind with UnfilteredRowIterator.isEmpty(), which is false whenever a partition carries a partition level
     * deletion, however little of the partition can satisfy the filter. So (2,'b') was materialized, its one row
     * spent the page's whole row budget, and (1,'a') was never reached. The read was not augmented, so it completed
     * as CompletedRead.simple, which issues no short read follow up, and the coordinator filtered the one partition
     * it was handed down to nothing.
     * <p>
     * The three methods after this one are this one with a single axis changed, and each axis is necessary.
     */
    @Test
    public void testPagedFilteredRangeReadOverATombstonedPartition()
    {
        assertTrackedMatchesOracle("d_paged_tombstone", TABLE, TOMBSTONED_PARTITION_BEFORE_THE_MATCH, FILTER, 1,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * {@link #testPagedFilteredRangeReadOverATombstonedPartition} unpaged. The whole range is read in one go, so the
     * materialization never stops short and nothing has to survive a page boundary.
     */
    @Test
    public void testUnpagedFilteredRangeReadOverATombstonedPartition()
    {
        assertTrackedMatchesOracle("d_unpaged_tombstone", TABLE, TOMBSTONED_PARTITION_BEFORE_THE_MATCH, FILTER, UNPAGED,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * {@link #testPagedFilteredRangeReadOverATombstonedPartition} without the partition level tombstone. The same
     * three partitions in the same order at the same page size, so neither paging nor the filter nor the presence of
     * non matching partitions is enough on its own. The tombstone is what keeps a partition the filter rejects in the
     * replica's materialized data, where it spends the page's row budget.
     */
    @Test
    public void testPagedFilteredRangeReadWithoutTheTombstone()
    {
        assertTrackedMatchesOracle("d_paged_no_tombstone", TABLE, SAME_PARTITIONS_WITHOUT_THE_TOMBSTONE, FILTER, 1,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * {@link #testPagedFilteredRangeReadOverATombstonedPartition} without the row filter. The same data, tombstone and
     * page size return every row, so the page only comes back empty once a filter can reject everything on it.
     */
    @Test
    public void testPagedUnfilteredRangeReadOverATombstonedPartition()
    {
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl";
        assertTrackedMatchesOracle("d_paged_unfiltered_tombstone", TABLE, TOMBSTONED_PARTITION_BEFORE_THE_MATCH, select, 1,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The other half of the same fix, and the reason the two halves cannot be separated. Filtering on part of the
     * partition key produces a filter made up entirely of partition level expressions, and a row filter only
     * evaluates those when it is applied to a partition rather than to a row iterator, so applied to a row iterator
     * it matches everything and (3,'c') is carried rather than discarded.
     * <p>
     * That went unnoticed while the emptiness check was consuming the iterator it was probing: the first surviving
     * row of every kept partition was swallowed before the limit counter saw it, so (3,'c') was carried but counted
     * as nothing and (1,'a') was reached anyway. Probing a fresh iterator makes the count honest, at which point
     * (3,'c') spends the page's whole row budget on a partition the coordinator discards, unless the filter is also
     * applied where its partition level expressions are evaluated.
     */
    @Test
    public void testPagedRangeReadFilteredOnAPartitionKeyColumn()
    {
        // (3,'c') sorts ahead of (1,'a'), and pk0 = 1 is the only thing that rules it out
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 1, 9) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 500) USING TIMESTAMP 11"
        };
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE pk0 = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle("d_paged_partition_key_filter", TABLE, writes, select, 1,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, select, oracle));
    }

    /**
     * A page's worth of rows is counted on the replica before the row filter has had a say, so rows that cannot be
     * returned still spend the page. (3,'c') holds one row the filter keeps and one it rejects, which is a full page
     * of two by the replica's count and one row by the coordinator's, and a page shorter than the page size is how
     * AbstractQueryPager recognizes the end of a result set, so (1,'a') is never read.
     */
    @Test
    public void testPagedFilteredRangeReadWhereARejectedRowSpendsThePage()
    {
        // (3,'c') sorts ahead of (1,'a'), and only one of its two rows can satisfy the filter
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 1, 500) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 2, 1) USING TIMESTAMP 11",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 500) USING TIMESTAMP 12"
        };
        assertTrackedMatchesOracle("e_rejected_row_spends_page", TABLE, writes, FILTER, 2,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * {@link #testPagedFilteredRangeReadWhereARejectedRowSpendsThePage} with a partition level deletion in front of
     * the rows, which is what makes the same query fail before this commit rather than only guard it afterwards.
     * <p>
     * UnfilteredRowIterator.isEmpty() short circuits on a partition level deletion, so the emptiness probe never
     * reached hasNext() and never swallowed the partition's first row. With nothing hiding the count, (3,'c') was
     * handed to the limit counter whole: its two rows filled a page of two, the coordinator filtered them down to the
     * one that satisfies the filter, and a page shorter than the page size ended the result set before (1,'a') was
     * ever read.
     */
    @Test
    public void testPagedFilteredRangeReadWhereARejectedRowSpendsThePageBehindATombstone()
    {
        // the deletion predates every row it covers, so it removes nothing and only stops the emptiness probe short
        String[] writes =
        {
            "*:DELETE FROM %s.tbl USING TIMESTAMP 5 WHERE pk0 = 3 AND pk1 = 'c'",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 1, 500) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 2, 1) USING TIMESTAMP 11",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 500) USING TIMESTAMP 12"
        };
        assertTrackedMatchesOracle("e_rejected_row_behind_a_tombstone", TABLE, writes, FILTER, 2,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * An indexed range read whose predicate is a partition key column and a static column. A tracked index read is the
     * only caller of {@link org.apache.cassandra.index.Index.MultiStepSearcher#filterCompletedRead}, and because every
     * expression the index claims is stripped from the post index query filter, that method is the only thing between
     * an index false positive and the answer. It filtered rows, so a partition holding a static row and no clustering
     * rows had nothing to filter and survived whole, even though its static value is not the one asked for.
     * <p>
     * (1,'b') is that partition: it satisfies the expression on the partition key column and not the one on the static
     * column, and it has no rows for a row level filter to reject.
     */
    @Test
    public void testIndexedRangeReadWhereAStaticOnlyPartitionDoesNotMatch()
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
            // static only: no clustering row is ever written for this partition
            "*:UPDATE %s.tbl USING TIMESTAMP 11 SET s = 3 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7";
        assertTrackedMatchesOracle("g_indexed_static_only", TABLE_WITH_INDEXED_STATIC, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, select, oracle));
    }

    /** Enough static only partitions ahead of the match that dropping them has to survive a limit being reached. */
    private static final String[] STATIC_ONLY_PARTITIONS =
    {
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
        "*:UPDATE %s.tbl USING TIMESTAMP 11 SET s = 3 WHERE pk0 = 1 AND pk1 = 'b'",
        "*:UPDATE %s.tbl USING TIMESTAMP 12 SET s = 3 WHERE pk0 = 1 AND pk1 = 'c'",
        "*:UPDATE %s.tbl USING TIMESTAMP 13 SET s = 3 WHERE pk0 = 1 AND pk1 = 'd'"
    };

    /**
     * {@link #testIndexedRangeReadWhereAStaticOnlyPartitionDoesNotMatch} paged one row at a time, with three dropped
     * partitions rather than one. Dropping a partition returns an empty page to a pager that reads a short page as the
     * end of the result set, so the drop has to be invisible to the limit counter as well as to the answer.
     */
    @Test
    public void testPagedIndexedRangeReadWhereStaticOnlyPartitionsAreDropped()
    {
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7";
        assertTrackedMatchesOracle("g_indexed_static_only_paged", TABLE_WITH_INDEXED_STATIC, STATIC_ONLY_PARTITIONS, select, 1,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, select, oracle));
    }

    /** {@link #testPagedIndexedRangeReadWhereStaticOnlyPartitionsAreDropped} under a LIMIT instead of a page size. */
    @Test
    public void testLimitedIndexedRangeReadWhereStaticOnlyPartitionsAreDropped()
    {
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7 LIMIT 1";
        assertTrackedMatchesOracle("g_indexed_static_only_limited", TABLE_WITH_INDEXED_STATIC, STATIC_ONLY_PARTITIONS, select, UNPAGED,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, select, oracle));
    }

    /**
     * A tracked index read decides which of the mutations reconciliation delivers the index expression matches by
     * indexing them itself, and an update that sets a static column carries a static row. Only an index on a static
     * column, or on a partition key column, indexes that row; asking an index on a clustering column about it read a
     * clustering value out of a clustering that has none and threw ArrayIndexOutOfBoundsException. It threw inside the
     * read's completion, where no failure response is sent, so the query hung until the coordinator gave up rather
     * than returning a wrong answer.
     */
    @Test
    public void testIndexedRangeReadWhereAReconciledUpdateCarriesAStaticRow()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
            // node 1 coordinates, so this is the update reconciliation delivers, static row and all
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET s = 8, v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 2"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE ck = 2 ALLOW FILTERING";
        assertTrackedMatchesOracle("g_indexed_clustering_with_static", TABLE_WITH_INDEXED_CLUSTERING_AND_STATIC,
                                   writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * As {@link #testIndexedRangeReadWhereAReconciledUpdateCarriesAStaticRow}, for an index on a partition key column,
     * which does index the static row: legacy 2i keys that entry on nothing but the base partition key, so that a
     * partition holding only static data is still discoverable. The tracked matcher hands the searcher the row's own
     * clustering, so the entry arrived as {@code Clustering.STATIC_CLUSTERING} and tripped the assertion in
     * ClusteringIndexNamesFilter, again inside the read's completion where no failure response is sent.
     * <p>
     * (1,'a') reaches the searcher with a static entry alongside a row entry, and (1,'b') with a static entry alone,
     * which is the case that leaves the names filter with no row to name at all.
     */
    @Test
    public void testIndexedPartitionKeyRangeReadWhereAReconciledUpdateCarriesAStaticRow()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
            // node 1 coordinates, so these are the updates reconciliation delivers, static rows and all
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET s = 8, v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 2",
            "2:UPDATE %s.tbl USING TIMESTAMP 30 SET s = 9 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle("g_indexed_pk_with_static", TABLE_WITH_INDEXED_PARTITION_KEY_AND_STATIC,
                                   writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * As {@link #testIndexedPartitionKeyRangeReadWhereAReconciledUpdateCarriesAStaticRow}, where the partition that
     * ends up holding nothing but static data also carries tombstones the static write has to be reconciled against:
     * a delete of the static column and of a row that was never written, aimed at a different replica than the static
     * write, so the two reach the read from different places.
     */
    @Test
    public void testIndexedPartitionKeyRangeReadWhereAStaticOnlyPartitionAlsoHasTombstones()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
            // deletes the static column of (1,'b') along with a row of it that was never written
            "2:DELETE v, s FROM %s.tbl USING TIMESTAMP 13 WHERE pk0 = 1 AND pk1 = 'b' AND ck = 1",
            // and then writes the static column back from another replica, leaving only static data behind
            "3:UPDATE %s.tbl USING TIMESTAMP 15 SET s = 9 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle("g_indexed_pk_static_only_with_tombstones", TABLE_WITH_INDEXED_PARTITION_KEY_AND_STATIC,
                                   writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * A reconciled range read whose per partition limit is reached on a row covered by a range tombstone that has not
     * closed yet. ReadCommand.completeRead pairs the counter enforcing the limit with an RTBoundCloser, because a
     * counter that stops inside an open range tombstone drops its closing bound; the closer appends that bound lazily,
     * on the pull after the counter has stopped.
     * <p>
     * The extending read's own counter stopped at the same row one level higher up, so that pull never happened and
     * the bound was never appended. It sat above the PROCESSED RTBoundValidator, which then saw the partition close
     * with a range tombstone still open:
     * <pre>
     * java.lang.IllegalStateException: PROCESSED UnfilteredRowIterator for ... has an illegal RT bounds sequence:
     * expected all RTs to be closed, but the last one is open
     *     at org.apache.cassandra.db.transform.RTBoundValidator$RowsTransformation.onPartitionClose(RTBoundValidator.java:112)
     *     at org.apache.cassandra.db.partitions.PartitionIterators$Serializer.serialize(PartitionIterators.java:253)
     *     at org.apache.cassandra.service.reads.tracked.ExtendingCompletedRead$RangeRead.response(ExtendingCompletedRead.java:206)
     * </pre>
     * The replica throws instead of responding, so the read never completes and the client sees a timeout.
     * <p>
     * ck = 3 is the row reconciliation delivers and the row the limit stops on, and the range tombstone covering
     * [2, 6] is still open there because ck = 5 is behind it.
     */
    @Test
    public void testRangeReadWhosePerPartitionLimitFallsInsideARangeTombstone()
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 10) USING TIMESTAMP 10",
            "*:DELETE FROM %s.tbl USING TIMESTAMP 20 WHERE pk0 = 1 AND pk1 = 'a' AND ck >= 2 AND ck <= 6",
            // node 2 only: reconciliation has to deliver this, which is what makes the completed read an extending one
            "2:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 3, 30) USING TIMESTAMP 30",
            // still inside the tombstone, so the tombstone is open when the limit stops on ck = 3
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 5, 50) USING TIMESTAMP 50"
        };
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl PER PARTITION LIMIT 2";
        assertTrackedMatchesOracle("h_per_partition_limit_inside_rt", TABLE, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * An indexed range read that scans part of its range, because a page's limit is reached before the end of it,
     * and is then handed a key past everything it scanned by reconciliation.
     * <p>
     * The read remembers the last key it scanned so that the matches it did not get to can be recognised as a short
     * read, and so that the follow up read knows where to resume. Reconciliation used to move that key up to the one
     * it delivered, which claimed the whole span up to it even though only that one partition was read. The matches
     * left over from the index scan then fell inside the claimed span, so instead of being treated as a short read
     * they were emitted with nothing behind them:
     * <pre>
     * java.lang.IllegalStateException: Received match for key without initial or followup read: 000400000003...
     *     at org.apache.cassandra.service.reads.tracked.PartialTrackedIndexRead$FilteringCompletedIndexRead$UnfilteredResultIterator.computeNext(PartialTrackedIndexRead.java:807)
     *     at org.apache.cassandra.db.partitions.PartitionIterators$Serializer.serialize(PartitionIterators.java:247)
     *     at org.apache.cassandra.service.reads.tracked.PartialTrackedIndexRead$FilteringCompletedIndexRead.response(PartialTrackedIndexRead.java:838)
     * </pre>
     * The replica throws instead of responding, so the read never completes and the client sees a timeout.
     * <p>
     * With the default Murmur3 partitioner a range scan visits (3,'c'), then (2,'b'), then (1,'a'). A page size of
     * one stops the scan after (3,'c') and leaves (2,'b') among the matches it did not get to, (1,'a') is the key
     * reconciliation delivers, and (3,'c') is written with a value the row filter rejects so that the page's limit
     * is still unspent when the read moves past it and reaches (2,'b').
     */
    @Test
    public void testIndexedRangeReadHandedAKeyPastTheScannedRange()
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (3, 'c', 1, 100, 0) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (2, 'b', 1, 100, 1) USING TIMESTAMP 11",
            // node 2 only, so reconciliation has to deliver it, and last in token order, so it is past the scan
            "2:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'a', 1, 100, 1) USING TIMESTAMP 12"
        };
        String select = "SELECT pk0, pk1, ck, v, w FROM %s.tbl WHERE v = 100 AND w = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle("j_indexed_key_past_the_scan", TABLE_WITH_INDEXED_VALUE, writes, select, 1,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    public static String withKeyspace(String replaceIn, String keyspace)
    {
        return String.format(replaceIn, keyspace);
    }
}
