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
package org.apache.cassandra.io.sstable;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;

@RunWith(OrderedJUnit4ClassRunner.class)
public class IndexSummaryManagerCompactionRaceTest extends IndexSummaryManagerTest
{
    @Override
    public void testChangeMinIndexInterval() {}

    @Override
    public void testChangeMaxIndexInterval() {}

    @Override
    public void testRedistributeSummaries() {}

    @Override
    public void testRebuildAtSamplingLevel() {}

    @Override
    public void testJMXFunctions() {}

    //This test runs last, since cleaning up compactions and tp is a pain
    @Test
    public void testCompactionRace() throws InterruptedException, ExecutionException
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARDRACE; // index interval of 8, no key caching
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        int numSSTables = 50;
        int numRows = 1 << 10;
        createSSTables(ksname, cfname, numSSTables, numRows);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());

        ExecutorService tp = Executors.newFixedThreadPool(2);

        final AtomicBoolean failed = new AtomicBoolean(false);

        for (int i = 0; i < 2; i++)
        {
            tp.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    while(!failed.get())
                    {
                        try
                        {
                            IndexSummaryManager.instance.redistributeSummaries();
                        }
                        catch (Throwable e)
                        {
                            failed.set(true);
                        }
                    }
                }
            });
        }

        while ( cfs.getSSTables().size() != 1 )
            cfs.forceMajorCompaction();

        try
        {
            Assert.assertFalse(failed.getAndSet(true));

            for (SSTableReader sstable : sstables)
            {
                Assert.assertEquals(true, sstable.isMarkedCompacted());
            }

            Assert.assertEquals(numSSTables, sstables.size());

            try
            {
                totalOffHeapSize(sstables);
                Assert.fail("This should have failed");
            } catch (AssertionError e)
            {

            }
        }
        finally
        {
            tp.shutdownNow();
            CompactionManager.instance.finishCompactionsAndShutdown(10, TimeUnit.SECONDS);
        }

        cfs.truncateBlocking();
    }
}
