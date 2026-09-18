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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * The index shapes {@link TrackedRangeReadTest} asserts against SAI, asserted against legacy 2i. The two
 * implementations reach the same answer by entirely different means - SAI keeps a static term per partition where a
 * legacy index keeps one index row whose clustering is the base partition key, and a tracked read indexes the
 * mutations reconciliation delivers with whichever implementation the table has - so a tracked index read over them
 * is two different reads and has to be asserted twice.
 * <p>
 * A query uses one legacy index only, so a predicate of two expressions leaves whichever one the chosen index does
 * not serve to filtering, and CQL will not run it without {@code ALLOW FILTERING}. Which of the two indexes is chosen
 * is the planner's call, so both an index on a partition key column and one on a static column are covered by the
 * static column cases below.
 */
@RunWith(Parameterized.class)
public class TrackedLegacyIndexedRangeReadTest extends TrackedRangeReadTestBase
{
    private static final String TABLE_WITH_INDEXED_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_pk0 ON %s.tbl(pk0) USING 'legacy_local_table';" +
        "CREATE INDEX tbl_s ON %s.tbl(s) USING 'legacy_local_table'";

    /** {@code v} is indexed and {@code w} is not, so a filter on {@code w} is left for the read to apply itself. */
    private static final String TABLE_WITH_INDEXED_VALUE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, w int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_v ON %s.tbl(v) USING 'legacy_local_table'";

    private static final String STATIC_SELECT =
        "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7 ALLOW FILTERING";

    /** {@link TrackedRangeReadTest#testIndexedRangeReadWhereAStaticOnlyPartitionDoesNotMatch} with legacy 2i. */
    @Test
    public void testIndexedRangeReadWhereAStaticOnlyPartitionDoesNotMatch()
    {
        staticOnlyPartitionDoesNotMatch("g_legacy_indexed_static_only", TABLE_WITH_INDEXED_STATIC, STATIC_SELECT);
    }

    /** {@link TrackedRangeReadTest#testPagedIndexedRangeReadWhereStaticOnlyPartitionsAreDropped} with legacy 2i. */
    @Test
    public void testPagedIndexedRangeReadWhereStaticOnlyPartitionsAreDropped()
    {
        staticOnlyPartitionsAreDropped("g_legacy_indexed_static_only_paged", TABLE_WITH_INDEXED_STATIC, STATIC_SELECT, 1);
    }

    /** {@link TrackedRangeReadTest#testLimitedIndexedRangeReadWhereStaticOnlyPartitionsAreDropped} with legacy 2i. */
    @Test
    public void testLimitedIndexedRangeReadWhereStaticOnlyPartitionsAreDropped()
    {
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7 LIMIT 1 ALLOW FILTERING";
        staticOnlyPartitionsAreDropped("g_legacy_indexed_static_only_limited", TABLE_WITH_INDEXED_STATIC, select, UNPAGED);
    }

    /** {@link TrackedRangeReadTest#testIndexedRangeReadHandedAKeyPastTheScannedRange} with legacy 2i. */
    @Test
    public void testIndexedRangeReadHandedAKeyPastTheScannedRange()
    {
        indexedRangeReadHandedAKeyPastTheScannedRange("j_legacy_indexed_key_past_the_scan", TABLE_WITH_INDEXED_VALUE);
    }
}
