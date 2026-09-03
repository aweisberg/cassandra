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

package org.apache.cassandra.service.reads;

import java.util.List;

import com.google.common.collect.Iterators;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.tracked.TrackedDataResponse;

import static org.apache.cassandra.Util.assertClustering;
import static org.apache.cassandra.Util.assertColumn;
import static org.apache.cassandra.Util.assertColumns;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A tracked read answers a single command from several sub-reads - the data replica's own result, single partition
 * follow up reads for keys reconciliation delivered mutations for, and short read protection follow ups - and folds
 * each of them into one {@link TrackedDataResponse} as another serialized chunk. Nothing in that path keeps two
 * chunks from carrying rows for the same partition, so reading the response back has to reconcile them.
 * <p>
 * {@link TrackedDataResponse#makeIteratorUnlimited} used to combine the chunks with
 * {@code PartitionIterators.mergeNonOverlapping}, which threw {@code IllegalStateException("Multiple partitions
 * received")} on the second chunk to carry a key, failing the whole query on the coordinator.
 */
public class TrackedDataResponseTest extends AbstractReadResponseTest
{
    private static final String KEYSPACE_STATIC = "TrackedDataResponseTest";
    private static final String CF_STATIC = "WithStatic";

    private static ColumnFamilyStore cfsStatic;
    private static TableMetadata cfmStatic;

    /**
     * A table with static columns, which {@link AbstractReadResponseTest} does not define. Marks the cluster
     * metadata again afterwards so that the reset every test does keeps this keyspace.
     */
    @BeforeClass
    public static void setupStaticTable()
    {
        TableMetadata.Builder builder =
            TableMetadata.builder(KEYSPACE_STATIC, CF_STATIC)
                         .addPartitionKeyColumn("key", BytesType.instance)
                         .addClusteringColumn("col1", AsciiType.instance)
                         .addStaticColumn("s1", AsciiType.instance)
                         .addStaticColumn("s2", AsciiType.instance)
                         .addRegularColumn("c1", AsciiType.instance);

        SchemaLoader.createKeyspace(KEYSPACE_STATIC, KeyspaceParams.simple(2), builder);
        cfsStatic = Keyspace.open(KEYSPACE_STATIC).getColumnFamilyStore(CF_STATIC);
        cfmStatic = cfsStatic.metadata();
        ServerTestUtils.markCMS();
    }

    /** One serialized chunk of a response, made the way a completed tracked read makes one. */
    private TrackedDataResponse chunk(ReadCommand command, UnfilteredPartitionIterator data)
    {
        try (PartitionIterator filtered = filter(data))
        {
            return TrackedDataResponse.create(filtered, command.columnFilter());
        }
    }

    /** A chunk holding one row of one partition. */
    private TrackedDataResponse row(ReadCommand command, TableMetadata table, DecoratedKey key,
                                    long timestamp, String clustering, String column, String value)
    {
        return chunk(command, iter(new RowUpdateBuilder(table, nowInSec, timestamp, key).clustering(clustering)
                                                                                       .add(column, value)
                                                                                       .buildUpdate()));
    }

    /** A chunk holding one partition that has nothing in it but a static row. */
    private TrackedDataResponse staticOnly(ReadCommand command, TableMetadata table, DecoratedKey key,
                                           long timestamp, String column, String value)
    {
        return chunk(command, iter(new RowUpdateBuilder(table, nowInSec, timestamp, key).clustering()
                                                                                       .add(column, value)
                                                                                       .buildUpdate()));
    }

    /**
     * Two chunks carrying the same partition with different rows in it: what a range read whose per partition limit
     * cut a partition short and whose follow up then read more of that same partition comes back with.
     */
    @Test
    public void testChunksCarryingTheSamePartitionAreMerged()
    {
        ReadCommand command = Util.cmd(cfs).withNowInSeconds(nowInSec).build();
        TrackedDataResponse first = row(command, cfm, dk, 0L, "1", "c1", "v1");
        TrackedDataResponse second = row(command, cfm, dk, 1L, "2", "c1", "v2");

        try (PartitionIterator merged = first.merge(second).makeIteratorUnlimited(command))
        {
            // the rows of a partition have to be consumed before the partition iterator is advanced past it
            try (RowIterator rows = merged.next())
            {
                assertEquals(dk, rows.partitionKey());

                Row row = rows.next();
                assertClustering(cfm, row, "1");
                assertColumn(cfm, row, "c1", "v1", 0);

                row = rows.next();
                assertClustering(cfm, row, "2");
                assertColumn(cfm, row, "c1", "v2", 1);

                assertFalse(rows.hasNext());
            }
            assertFalse(merged.hasNext());
        }
    }

    /**
     * Chunks whose rows share a clustering are reconciled cell by cell rather than one of them winning: the newest
     * value of a column that several chunks hold, and the union of the columns they hold between them. Two chunks
     * can hold the same row because the sub-reads a tracked read is assembled from are independent reads, each
     * seeing whatever its own replica had.
     */
    @Test
    public void testRowsSharingAClusteringAreReconciled()
    {
        ReadCommand command = Util.cmd(cfs).withNowInSeconds(nowInSec).build();
        List<TrackedDataResponse> chunks = List.of(row(command, cfm, dk, 0L, "1", "c1", "stale"),
                                                   row(command, cfm, dk, 2L, "1", "c1", "fresh"),
                                                   row(command, cfm, dk, 1L, "1", "c2", "other"));

        try (PartitionIterator merged = TrackedDataResponse.merge(chunks).makeIteratorUnlimited(command))
        {
            try (RowIterator rows = merged.next())
            {
                Row row = Iterators.getOnlyElement(rows);
                assertClustering(cfm, row, "1");
                assertColumns(row, "c1", "c2");
                assertColumn(cfm, row, "c1", "fresh", 2);
                assertColumn(cfm, row, "c2", "other", 1);
            }
            assertFalse(merged.hasNext());
        }
    }

    /**
     * Static rows are unioned across the chunks the same way, including from a chunk that has one when the first
     * chunk does not. A chunk carrying nothing but a static row is a partition in its own right, so it has to
     * contribute to the merged one rather than be passed over.
     */
    @Test
    public void testStaticRowsAreUnionedAcrossChunks()
    {
        ReadCommand command = Util.cmd(cfsStatic).withNowInSeconds(nowInSec).build();
        List<TrackedDataResponse> chunks = List.of(row(command, cfmStatic, dk, 0L, "1", "c1", "v1"),
                                                   staticOnly(command, cfmStatic, dk, 1L, "s1", "stale"),
                                                   staticOnly(command, cfmStatic, dk, 2L, "s2", "other"),
                                                   staticOnly(command, cfmStatic, dk, 3L, "s1", "fresh"));

        try (PartitionIterator merged = TrackedDataResponse.merge(chunks).makeIteratorUnlimited(command))
        {
            try (RowIterator rows = merged.next())
            {
                Row statics = rows.staticRow();
                assertColumns(statics, "s1", "s2");
                assertColumn(cfmStatic, statics, "s1", "fresh", 3);
                assertColumn(cfmStatic, statics, "s2", "other", 2);

                Row row = Iterators.getOnlyElement(rows);
                assertClustering(cfmStatic, row, "1");
                assertColumn(cfmStatic, row, "c1", "v1", 0);
            }
            assertFalse(merged.hasNext());
        }
    }

    /**
     * The case the merge always handled, kept as a control: chunks with no key in common are still returned whole,
     * in token order, whichever order they arrive in.
     */
    @Test
    public void testChunksWithDisjointKeysAreReturnedInTokenOrder()
    {
        ReadCommand command = Util.cmd(cfs).withNowInSeconds(nowInSec).build();
        DecoratedKey one = dk("key1");
        DecoratedKey two = dk("key2");
        DecoratedKey lower = one.compareTo(two) < 0 ? one : two;
        DecoratedKey higher = lower == one ? two : one;

        TrackedDataResponse higherFirst = row(command, cfm, higher, 1L, "1", "c1", "higher");
        TrackedDataResponse lowerSecond = row(command, cfm, lower, 0L, "1", "c1", "lower");

        try (PartitionIterator merged = higherFirst.merge(lowerSecond).makeIteratorUnlimited(command))
        {
            try (RowIterator rows = merged.next())
            {
                assertEquals(lower, rows.partitionKey());
                assertColumn(cfm, Iterators.getOnlyElement(rows), "c1", "lower", 0);
            }
            assertTrue(merged.hasNext());
            try (RowIterator rows = merged.next())
            {
                assertEquals(higher, rows.partitionKey());
                assertColumn(cfm, Iterators.getOnlyElement(rows), "c1", "higher", 1);
            }
            assertFalse(merged.hasNext());
        }
    }
}
