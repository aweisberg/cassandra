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
package org.apache.cassandra.utils;

import static org.junit.Assert.*;

import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.cassandra.utils.BackpressureMonitor;
import org.apache.cassandra.utils.BackpressureMonitor.BackpressureListener;
import org.apache.cassandra.utils.BackpressureMonitor.WeightHolder;
import org.junit.Before;
import org.junit.Test;

public class BackpressureMonitorTest
{
    Queue<Boolean> states = new ArrayDeque<Boolean>();
    BackpressureListener mock = new BackpressureListener()
    {
        @Override
        public void backpressureStateChange(boolean onBackpressure)
        {
            states.offer(onBackpressure);
        }

    };

    BackpressureMonitor bm;

    @Before
    public void setUp()
    {
        states.clear();
        bm = new BackpressureMonitor(5, 3);
        bm.addListener(mock);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadOnOffParam()
    {
        new BackpressureMonitor(1, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParamOffGT0()
    {
        new BackpressureMonitor(1, -1);
    }

    @Test(expected = IllegalStateException.class)
    public void testDoubleListener()
    {
        bm.addListener(mock);
    }

    @Test
    public void doNothingCases()
    {
        bm.backpressure(0);
        bm = new BackpressureMonitor(Long.MAX_VALUE, 100);
        bm.backpressure(200);
        bm.backpressure(-120);
        assertTrue(states.isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void negativeTotalWeight()
    {
        bm.backpressure(-1);
    }

    @Test
    public void testBackpressureOnOff()
    {
        WeightHolder wh1 = bm.new WeightHolderImpl(1);
        wh1.addWeight(3);
        wh1.close();
        assertTrue(states.isEmpty());

        WeightHolder wh2 = bm.new WeightHolderImpl(0);
        wh2.addWeight(1);
        wh2.close();
        assertTrue(!states.isEmpty() && states.poll() == true);

        wh2.decRef();
        assertTrue(states.isEmpty());

        wh1.decRef();
        assertTrue(!states.isEmpty() && states.poll() == false);

        assertTrue(states.isEmpty());
    }

    @Test
    public void discardWH()
    {
        WeightHolder wh = bm.new WeightHolderImpl(100);
        wh.close();
        assertTrue(states.isEmpty());
    }

    @Test
    public void testReset()
    {
        WeightHolder wh = bm.new WeightHolderImpl(100);
        wh.addWeight(1);
        wh.close();
        assertEquals(bm.listeners.get(0), mock);
        assertEquals(101, bm.weight.get());
        bm.reset();
        assertEquals(0, bm.weight.get());
        assertTrue(bm.listeners.isEmpty());
    }

    @Test
    public void testThreaded() throws Throwable
    {
        BackpressureMonitor bm = new BackpressureMonitor(3, 2);
        bm.backpressure(1);
        Runnable r = () -> {
            for (int i = 0; i < 1000000; i++)
            {
                bm.backpressure(i % 2 == 0 ? 1 : -1);
            }
        };
        Thread t = new Thread(r);
        Thread t2 = new Thread(r);

        t.start();
        t2.start();
        t.join();
        t2.join();

        int changes = states.size();
        for (int i = 0; i < changes; i++)
        {
            assertEquals(i % 2 == 0 ? true : false, states.poll());
        }
    }
}
