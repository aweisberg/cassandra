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
package org.apache.cassandra.net;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.net.OutboundTcpConnection.FixedCoalescingStrategy;
import org.apache.cassandra.net.OutboundTcpConnection.MovingAverageCoalescingStrategy;
import org.junit.Test;

public class OutboundTcpConnectionTest
{

    @Test
    public void testFixedCoalescingStrategy() throws Exception
    {
        FixedCoalescingStrategy fcs = new FixedCoalescingStrategy(200);

        long now = System.nanoTime();
        assertTrue(fcs.notifyAndSleep(42));
        assertTrue(System.nanoTime() >= now + TimeUnit.MICROSECONDS.toNanos(200));

        //Shouldn't sleep
        fcs = new FixedCoalescingStrategy((int)TimeUnit.NANOSECONDS.toMicros(Integer.MAX_VALUE));
        now = System.nanoTime();
        fcs.notifyOfSample(42);
        assertTrue(System.nanoTime() < now + TimeUnit.SECONDS.toNanos(1));
    }

    public static long toNanos(long micros) {
        return TimeUnit.MICROSECONDS.toNanos(micros);
    }


    @Test
    public void testMovingAverageCoalescingStrategy() throws Exception {
        MovingAverageCoalescingStrategy macs = new MovingAverageCoalescingStrategy(200);

        /*
         * There should be no coalescing until all samples have been provided
         */
        for (long ii = 0; ii < 15; ii++) {
            assertFalse(macs.notifyAndSleep(toNanos(199 * ii)));
        }
        //Average should be too high so still no coalescing
        assertFalse(macs.notifyAndSleep(toNanos(Integer.MAX_VALUE)));

        for (long ii = 1; ii < 16; ii++) {
            assertFalse(macs.notifyAndSleep(toNanos(Integer.MAX_VALUE + (199 * ii))));
        }

        long now = System.nanoTime();
        assertTrue(macs.notifyAndSleep(toNanos(Integer.MAX_VALUE + (199L * 16L))));

        //Should coalesce for at least 200 microseconds
        long newNow = System.nanoTime();
        assertTrue(toNanos(200) <= newNow - now);

        /*
         * Test that out of order samples are treated as being close together
         */
        macs = new MovingAverageCoalescingStrategy(200);

        for (long ii = 0; ii < 15; ii++) {
            assertFalse(macs.notifyAndSleep(toNanos(1000 - 1)));
        }

        /*
         * Average should be 62438, it should coalesce at least that long
         */
        now = System.nanoTime();
        assertTrue(macs.notifyAndSleep(toNanos(1000 - 1)));
        assertTrue(62438 <= System.nanoTime() - now);
    }

    @Test
    public void testTimestampOrdering() throws Exception
    {
        long nowNanos = System.nanoTime();
        long now = System.currentTimeMillis();
        long lastConverted = 0;
        for (long ii = 0; ii < 10000000; ii++)
        {
            now = Math.max(now, System.currentTimeMillis());
            if (ii % 10000 == 0) {
                synchronized (OutboundTcpConnection.TIMESTAMP_UPDATE) {
                    OutboundTcpConnection.TIMESTAMP_UPDATE.notify();
                }
                Thread.sleep(1);
            }
            nowNanos = Math.max(now, System.nanoTime());
            long convertedNow = OutboundTcpConnection.convertNanoTimeToCurrentTimeMillis(nowNanos);
            assertTrue(convertedNow >= lastConverted);
            lastConverted = convertedNow;
            //Seems to be off by as much as two milliseconds sadly
            assertTrue((now - 2) <= convertedNow);
        }
    }

}
