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
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.sai.SAIUtil;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class MutationTrackingRangeReadTest extends TestBaseImpl
{
    private static final int REPLICAS = 3;

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = Cluster.build()
                         .withNodes(REPLICAS)
                         // background reconciliation converges the replicas within a few seconds, which would heal
                         // the divergence these cases are built on before the read under test ever sees it
                         .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                               .set("hinted_handoff_enabled", false)
                                               .set("mutation_tracking.background_reconciliation_enabled", false))
                         .start();
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testPartialPartitionFilterWithPerPartitionLimit()
    {
        String keyspace = "partial_partition_filter_per_partition_limit";
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));

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
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));
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
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));
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
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));
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
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));

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
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"
                                          + (tracked ? " AND replication_type='tracked'" : ""), keyspace));
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
     * The unstressed case check, for the cases built on divergent replicas. If what the data replica holds on its own
     * already answers the query then the read never has to reconcile anything, and the case would pass with the read
     * path completely broken.
     * <p>
     * Node 1 is the data replica of every one of these reads, and deterministically so: {@link #read} coordinates on
     * node 1, TrackedRead.start prefers the local replica whenever it is a full one, and at RF=3 on three nodes every
     * node is a full replica of every range. So the replica whose materialized data the answer is built from is the
     * one these fixtures leave stale, and it is the same one every run.
     */
    private static void assertDataReplicaCannotAnswerAlone(String keyspace, String select, Object[][] oracle)
    {
        Assert.assertFalse("Not stressed: the data replica answers this query correctly on its own",
                           Arrays.deepEquals(nodeLocal(keyspace, 1, select), oracle));
    }

    /**
     * The converse, for the cases that write through the coordinator and so have no divergence in them: every
     * replica already holds everything the answer needs, which is what makes a short coordinator answer a defect
     * in the read path rather than data that was never there.
     */
    private static void assertEveryReplicaCanAnswerAlone(String keyspace, String select, Object[][] oracle)
    {
        for (int node = 1; node <= REPLICAS; node++)
            assertRows(nodeLocal(keyspace, node, select), oracle);
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
     * cannot tell this case apart from a vacuous one and the probe asserts the divergence directly instead.
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
            assertRows(nodeLocal(keyspace, 1, everything), row(1, "a", 1, 1));
            assertRows(nodeLocal(keyspace, 2, everything), row(1, "a", 1, 2));
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

    public static String withKeyspace(String replaceIn, String keyspace)
    {
        return String.format(replaceIn, keyspace);
    }
}
