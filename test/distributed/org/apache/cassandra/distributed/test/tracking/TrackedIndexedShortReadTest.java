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
 * An index read whose short read follows a key that reconciliation handed it, with SAI and with a legacy index.
 */
@RunWith(Parameterized.class)
public class TrackedIndexedShortReadTest extends TrackedRangeReadTestBase
{
    private static final String SAI_TABLE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, w int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_v ON %s.tbl(v) USING 'SAI'";

    private static final String LEGACY_TABLE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, w int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_v ON %s.tbl(v) USING 'legacy_local_table'";

    @Test
    public void testIndexedRangeReadHandedAKeyBeforeTheNextUnreadMatch()
    {
        indexedRangeReadHandedAKeyBeforeTheNextUnreadMatch("k_indexed_key_before_unread", SAI_TABLE);
    }

    @Test
    public void testLegacyIndexedRangeReadHandedAKeyBeforeTheNextUnreadMatch()
    {
        indexedRangeReadHandedAKeyBeforeTheNextUnreadMatch("k_legacy_indexed_key_before_unread", LEGACY_TABLE);
    }

    @Test
    public void testIndexedRangeReadWithStaleEntriesHandedAKeyBeforeTheNextUnreadMatch()
    {
        indexedRangeReadWithStaleEntriesHandedAKeyBeforeTheNextUnreadMatch("k_indexed_stale_before_unread", SAI_TABLE);
    }

    @Test
    public void testLegacyIndexedRangeReadWithStaleEntriesHandedAKeyBeforeTheNextUnreadMatch()
    {
        indexedRangeReadWithStaleEntriesHandedAKeyBeforeTheNextUnreadMatch("k_legacy_stale_before_unread", LEGACY_TABLE);
    }
}
