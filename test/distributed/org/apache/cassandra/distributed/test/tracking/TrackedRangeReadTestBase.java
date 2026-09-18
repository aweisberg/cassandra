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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
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

/**
 * The cluster, the two modes and the oracle harness the tracked range read cases are written in terms of. The cases
 * themselves are split across sibling classes - {@link TrackedRangeReadTest} and
 * {@link TrackedLegacyIndexedRangeReadTest} - because every case leaves behind the two keyspaces it created, and a
 * keyspace's tables keep a memtable region and a set of table metrics for as long as the cluster is up, so how many
 * cases one class holds is bounded by the heap of the one JVM that runs it.
 * <p>
 * Every case runs twice, once per {@link Mode}: once with three full replicas of every range, and once with
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
public abstract class TrackedRangeReadTestBase extends TestBaseImpl
{
    protected static final int REPLICAS = 3;

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
    protected static Mode mode;

    protected static Cluster cluster;

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
    protected static void createTrackedKeyspace(String keyspace)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " + mode.replication + " AND replication_type='tracked'", keyspace));
    }

    /*
     * Everything below is one harness. A case that uses it runs its query twice, against two keyspaces that differ
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

    /** Passed as a page size to read the whole range in one request. */
    protected static final int UNPAGED = 0;

    /**
     * Writes the same data to a tracked keyspace and to an otherwise identical untracked one, reads the untracked
     * one for the expected answer, runs {@code probe} against the tracked one, and asserts the tracked read
     * returns what the untracked read did.
     *
     * @param pageSize the page size to read at, or {@link #UNPAGED}
     * @param probe    given the tracked keyspace and the oracle's answer, run after the writes and before the read
     * @return the tracked keyspace, for any assertion a case wants to make about what the read left behind
     */
    protected static String assertTrackedMatchesOracle(String name, String table, String[] writes, String select,
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
    protected static Object[][] nodeLocal(String keyspace, int node, String select)
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
    protected static Set<Integer> fullReplicasFor(String keyspace, int pk0, String pk1)
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
    protected static Object[][] materializedOn(String keyspace, int node, Object[][] rows)
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
    protected static void assertDataReplicaCannotAnswerAlone(String keyspace, String select, Object[][] oracle)
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
    protected static void assertEveryReplicaCanAnswerAlone(String keyspace, String select, Object[][] oracle)
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

    /*
     * The index shapes below are each asserted against two indexes - SAI in TrackedRangeReadTest and legacy 2i in
     * TrackedLegacyIndexedRangeReadTest - so the fixture lives here and the table and the query each sibling hands
     * it name the implementation.
     */

    /**
     * A partition holding a static row and no clustering rows, which satisfies one expression of a two expression
     * predicate and not the other. A tracked index read is the only caller of
     * {@link org.apache.cassandra.index.Index.MultiStepSearcher#filterCompletedRead}, and because every expression
     * the index claims is stripped from the post index query filter, that method is the only thing between an index
     * false positive and the answer. It filtered rows, so a partition with no clustering rows had nothing to filter
     * and survived whole, even though its static value is not the one asked for.
     * <p>
     * (1,'b') is that partition.
     */
    protected static void staticOnlyPartitionDoesNotMatch(String name, String table, String select)
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
            // static only: no clustering row is ever written for this partition
            "*:UPDATE %s.tbl USING TIMESTAMP 11 SET s = 3 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        assertTrackedMatchesOracle(name, table, writes, select, UNPAGED,
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
     * {@link #staticOnlyPartitionDoesNotMatch} with three dropped partitions rather than one, and only as much of the
     * answer asked for as one of them would fill. Dropping a partition returns an empty page to a pager that reads a
     * short page as the end of the result set, so the drop has to be invisible to the limit counter as well as to the
     * answer, whether the limit is a page size or a LIMIT.
     */
    protected static void staticOnlyPartitionsAreDropped(String name, String table, String select, int pageSize)
    {
        assertTrackedMatchesOracle(name, table, STATIC_ONLY_PARTITIONS, select, pageSize,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, select, oracle));
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
    protected static void indexedRangeReadHandedAKeyPastTheScannedRange(String name, String table)
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (3, 'c', 1, 100, 0) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (2, 'b', 1, 100, 1) USING TIMESTAMP 11",
            // node 2 only, so reconciliation has to deliver it, and last in token order, so it is past the scan
            "2:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'a', 1, 100, 1) USING TIMESTAMP 12"
        };
        String select = "SELECT pk0, pk1, ck, v, w FROM %s.tbl WHERE v = 100 AND w = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle(name, table, writes, select, 1,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    public static String withKeyspace(String replaceIn, String keyspace)
    {
        return String.format(replaceIn, keyspace);
    }
}
