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

package org.apache.cassandra.service;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.locator.ReplicaMultimap;
import org.apache.cassandra.locator.ReplicaSet;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This is also fairly effectively testing source retrieval for bootstrap as well since RangeStreamer
 * is used to calculate the endpoints to fetch from and check they are alive for both RangeRelocator (move) and
 * bootstrap (RangeRelocator).
 */
public class MoveTransientTest
{
    static InetAddressAndPort aAddress;
    static InetAddressAndPort bAddress;
    static InetAddressAndPort cAddress;
    static InetAddressAndPort dAddress;
    static InetAddressAndPort eAddress;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        aAddress = InetAddressAndPort.getByName("127.0.0.1");
        bAddress = InetAddressAndPort.getByName("127.0.0.2");
        cAddress = InetAddressAndPort.getByName("127.0.0.3");
        dAddress = InetAddressAndPort.getByName("127.0.0.4");
        eAddress = InetAddressAndPort.getByName("127.0.0.5");
    }

    private final List<InetAddressAndPort> downNodes = new ArrayList();
    Predicate<Replica> alivePredicate = replica -> !downNodes.contains(replica.getEndpoint());

    private final List<InetAddressAndPort> sourceFilterDownNodes = new ArrayList<>();
    private final Collection<Predicate<Replica>> sourceFilters = Collections.singleton(replica -> !sourceFilterDownNodes.contains(replica.getEndpoint()));

    @After
    public void clearDownNode()
    {
        downNodes.clear();
        sourceFilterDownNodes.clear();
    }

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    Token oneToken = new RandomPartitioner.BigIntegerToken("1");
    Token twoToken = new RandomPartitioner.BigIntegerToken("2");
    Token threeToken = new RandomPartitioner.BigIntegerToken("3");
    Token fourToken = new RandomPartitioner.BigIntegerToken("4");
    Token sixToken = new RandomPartitioner.BigIntegerToken("6");
    Token sevenToken = new RandomPartitioner.BigIntegerToken("7");
    Token nineToken = new RandomPartitioner.BigIntegerToken("9");
    Token elevenToken = new RandomPartitioner.BigIntegerToken("11");
    Token fourteenToken = new RandomPartitioner.BigIntegerToken("14");

    Range aRange = new Range(oneToken, threeToken);
    Range bRange = new Range(threeToken, sixToken);
    Range cRange = new Range(sixToken, nineToken);
    Range dRange = new Range(nineToken, elevenToken);
    Range eRange = new Range(elevenToken, oneToken);


    ReplicaSet current = ReplicaSet.of(new Replica(aAddress, aRange, true),
                                       new Replica(aAddress, eRange, true),
                                       new Replica(aAddress, dRange, false));


    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-1
     * A's token moves from 3 to 4.
     * <p>
     * Result is A gains some range
     *
     * @throws Exception
     */
    @Test
    public void testCalculateStreamAndFetchRangesMoveForward() throws Exception
    {
        calculateStreamAndFetchRangesMoveForward();
    }

    private Pair<ReplicaSet, ReplicaSet> calculateStreamAndFetchRangesMoveForward() throws Exception
    {
        Range aPrimeRange = new Range(oneToken, fourToken);

        ReplicaSet updated = new ReplicaSet();
        updated.add(new Replica(aAddress, aPrimeRange, true));
        updated.add(new Replica(aAddress, eRange, true));
        updated.add(new Replica(aAddress, dRange, false));


        Pair<ReplicaSet, ReplicaSet> result = StorageService.instance.calculateStreamAndFetchRanges(current, updated);
        System.out.println("To stream");
        System.out.println(result.left);
        System.out.println("To fetch");
        System.out.println(result.right);

        assertEquals(ReplicaSet.empty(), result.left);
        assertEquals(ReplicaSet.of(new Replica(aAddress, new Range(threeToken, fourToken), true)), result.right);
        return result;
    }

    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-11 E 11-1
     * A's token moves from 3 to 14
     * <p>
     * Result is A loses range and it must be streamed
     *
     * @throws Exception
     */
    @Test
    public void testCalculateStreamAndFetchRangesMoveBackwardBetween() throws Exception
    {
        calculateStreamAndFetchRangesMoveBackwardBetween();
    }

    public Pair<ReplicaSet, ReplicaSet> calculateStreamAndFetchRangesMoveBackwardBetween() throws Exception
    {
        Range aPrimeRange = new Range(elevenToken, fourteenToken);

        ReplicaSet updated = new ReplicaSet();
        updated.add(new Replica(aAddress, aPrimeRange, true));
        updated.add(new Replica(aAddress, dRange, true));
        updated.add(new Replica(aAddress, cRange, false));


        Pair<ReplicaSet, ReplicaSet> result = StorageService.instance.calculateStreamAndFetchRanges(current, updated);
        System.out.println("To stream");
        System.out.println(result.left);
        System.out.println("To fetch");
        System.out.println(result.right);

        assertEquals(ReplicaSet.of(new Replica(aAddress, new Range(fourteenToken, oneToken), true), new Replica(aAddress, new Range(oneToken, threeToken), true)), result.left);
        assertEquals(ReplicaSet.of(new Replica(aAddress, new Range(sixToken, nineToken), false), new Replica(aAddress, new Range(nineToken, elevenToken), true)), result.right);
        return result;
    }

    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-11 E 11-1
     * A's token moves from 3 to 2
     *
     * Result is A loses range and it must be streamed
     * @throws Exception
     */
    @Test
    public void testCalculateStreamAndFetchRangesMoveBackward() throws Exception
    {
        calculateStreamAndFetchRangesMoveBackward();
    }

    private Pair<ReplicaSet, ReplicaSet> calculateStreamAndFetchRangesMoveBackward() throws Exception
    {
        Range aPrimeRange = new Range(oneToken, twoToken);

        ReplicaSet updated = new ReplicaSet();
        updated.add(new Replica(aAddress, aPrimeRange, true));
        updated.add(new Replica(aAddress, eRange, true));
        updated.add(new Replica(aAddress, dRange, false));


        Pair<ReplicaSet, ReplicaSet> result = StorageService.instance.calculateStreamAndFetchRanges(current, updated);
        System.out.println("To stream");
        System.out.println(result.left);
        System.out.println("To fetch");
        System.out.println(result.right);

        //Moving backwards has no impact on any replica. We already fully replicate counter clockwise
        //The transient replica does transiently replicate slightly more, but that is addressed by cleanup
        assertEquals(ReplicaSet.of(new Replica(aAddress, new Range(twoToken, threeToken), true)), result.left);
        assertEquals(ReplicaSet.of(), result.right);

        return result;
    }

    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-11 E 11-1
     * A's moves from 3 to 7
     *
     * @throws Exception
     */
    private Pair<ReplicaSet, ReplicaSet> calculateStreamAndFetchRangesMoveForwardBetween() throws Exception
    {
        Range aPrimeRange = new Range(sixToken, sevenToken);
        Range bPrimeRange = new Range(oneToken, sixToken);


        ReplicaSet updated = new ReplicaSet();
        updated.add(new Replica(aAddress, aPrimeRange, true));
        updated.add(new Replica(aAddress, bPrimeRange, true));
        updated.add(new Replica(aAddress, eRange, false));


        Pair<ReplicaSet, ReplicaSet> result = StorageService.instance.calculateStreamAndFetchRanges(current, updated);
        System.out.println("To stream");
        System.out.println(result.left);
        System.out.println("To fetch");
        System.out.println(result.right);

        assertEquals(ReplicaSet.of(new Replica(aAddress, new Range(nineToken, elevenToken), false), new Replica(aAddress, new Range(elevenToken, oneToken), true)), result.left);
        assertEquals(ReplicaSet.of(new Replica(aAddress, new Range(threeToken, sixToken), true), new Replica(aAddress, new Range(sixToken, sevenToken), true)), result.right);
        return result;
    }

    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-11 E 11-1
     * A's token moves from 3 to 7
     *
     * @throws Exception
     */
    @Test
    public void testCalculateStreamAndFetchRangesMoveForwardBetween() throws Exception
    {
        calculateStreamAndFetchRangesMoveForwardBetween();
    }

    /**
     * Construct the ring state for calculateStreamAndFetchRangesMoveBackwardBetween
     * Where are A moves from 3 to 14
     * @return
     */
    private Pair<ReplicaMultimap<Range<Token>, ReplicaSet>, ReplicaMultimap<Range<Token>, ReplicaList>> constructRangeAddressesMoveBackwardBetween()
    {
        ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses = ReplicaMultimap.set();
        rangeAddresses.put(aRange, new Replica(aAddress, aRange, true));
        rangeAddresses.put(eRange, new Replica(aAddress, eRange, true));
        rangeAddresses.put(dRange, new Replica(aAddress, dRange, false));

        rangeAddresses.put(bRange, new Replica(bAddress, bRange, true));
        rangeAddresses.put(aRange, new Replica(bAddress, aRange, true));
        rangeAddresses.put(eRange, new Replica(bAddress, eRange, false));

        rangeAddresses.put(cRange, new Replica(cAddress, cRange, true));
        rangeAddresses.put(bRange, new Replica(cAddress, bRange, true));
        rangeAddresses.put(aRange, new Replica(cAddress, aRange, false));

        rangeAddresses.put(dRange, new Replica(dAddress, dRange, true));
        rangeAddresses.put(cRange, new Replica(dAddress, cRange, true));
        rangeAddresses.put(bRange, new Replica(dAddress, bRange, false));

        rangeAddresses.put(eRange, new Replica(eAddress, eRange, true));
        rangeAddresses.put(dRange, new Replica(eAddress, dRange, true));
        rangeAddresses.put(cRange, new Replica(eAddress, cRange, false));

        Range aPrimeRange = new Range(elevenToken, fourteenToken);
        Range ePrimeRange = new Range(fourteenToken, oneToken);
        Range bPrimeRange = new Range(oneToken, sixToken);

        ReplicaMultimap<Range<Token>, ReplicaList> rangeAddressesSettled = ReplicaMultimap.list();
        rangeAddressesSettled.put(aPrimeRange, new Replica(aAddress, aPrimeRange, true));
        rangeAddressesSettled.put(dRange, new Replica(aAddress, dRange, true));
        rangeAddressesSettled.put(cRange, new Replica(aAddress, cRange, false));

        rangeAddressesSettled.put(bPrimeRange, new Replica(bAddress, bPrimeRange, true));
        rangeAddressesSettled.put(ePrimeRange, new Replica(bAddress, ePrimeRange, true));
        rangeAddressesSettled.put(aPrimeRange, new Replica(bAddress, aPrimeRange, false));

        rangeAddressesSettled.put(cRange, new Replica(cAddress, cRange, true));
        rangeAddressesSettled.put(bPrimeRange, new Replica(cAddress, bPrimeRange, true));
        rangeAddressesSettled.put(ePrimeRange, new Replica(cAddress, ePrimeRange, false));

        rangeAddressesSettled.put(dRange, new Replica(dAddress, dRange, true));
        rangeAddressesSettled.put(cRange, new Replica(dAddress, cRange, true));
        rangeAddressesSettled.put(bPrimeRange, new Replica(dAddress, bPrimeRange, false));

        rangeAddressesSettled.put(ePrimeRange, new Replica(eAddress, ePrimeRange, true));
        rangeAddressesSettled.put(aPrimeRange, new Replica(eAddress, aPrimeRange, true));
        rangeAddressesSettled.put(dRange, new Replica(eAddress, dRange, false));

        return Pair.create(rangeAddresses, rangeAddressesSettled);
    }


    /**
     * Construct the ring state for calculateStreamAndFetchRangesMoveForwardBetween
     * Where are A moves from 3 to 7
     * @return
     */
    private Pair<ReplicaMultimap<Range<Token>, ReplicaSet>, ReplicaMultimap<Range<Token>, ReplicaList>> constructRangeAddressesMoveForwardBetween()
    {
        ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses = ReplicaMultimap.set();
        rangeAddresses.put(aRange, new Replica(aAddress, aRange, true));
        rangeAddresses.put(eRange, new Replica(aAddress, eRange, true));
        rangeAddresses.put(dRange, new Replica(aAddress, dRange, false));

        rangeAddresses.put(bRange, new Replica(bAddress, bRange, true));
        rangeAddresses.put(aRange, new Replica(bAddress, aRange, true));
        rangeAddresses.put(eRange, new Replica(bAddress, eRange, false));

        rangeAddresses.put(cRange, new Replica(cAddress, cRange, true));
        rangeAddresses.put(bRange, new Replica(cAddress, bRange, true));
        rangeAddresses.put(aRange, new Replica(cAddress, aRange, false));

        rangeAddresses.put(dRange, new Replica(dAddress, dRange, true));
        rangeAddresses.put(cRange, new Replica(dAddress, cRange, true));
        rangeAddresses.put(bRange, new Replica(dAddress, bRange, false));

        rangeAddresses.put(eRange, new Replica(eAddress, eRange, true));
        rangeAddresses.put(dRange, new Replica(eAddress, dRange, true));
        rangeAddresses.put(cRange, new Replica(eAddress, cRange, false));

        Range aPrimeRange = new Range(sixToken, sevenToken);
        Range bPrimeRange = new Range(oneToken, sixToken);
        Range cPrimeRange = new Range(sevenToken, nineToken);

        ReplicaMultimap<Range<Token>, ReplicaList> rangeAddressesSettled = ReplicaMultimap.list();
        rangeAddressesSettled.put(aPrimeRange, new Replica(aAddress, aPrimeRange, true));
        rangeAddressesSettled.put(bPrimeRange, new Replica(aAddress, bPrimeRange, true));
        rangeAddressesSettled.put(eRange, new Replica(aAddress, eRange, false));

        rangeAddressesSettled.put(bPrimeRange, new Replica(bAddress, bPrimeRange, true));
        rangeAddressesSettled.put(eRange, new Replica(bAddress, eRange, true));
        rangeAddressesSettled.put(dRange, new Replica(bAddress, dRange, false));

        rangeAddressesSettled.put(cPrimeRange, new Replica(cAddress, cPrimeRange, true));
        rangeAddressesSettled.put(aPrimeRange, new Replica(cAddress, aPrimeRange, true));
        rangeAddressesSettled.put(bPrimeRange, new Replica(cAddress, bPrimeRange, false));

        rangeAddressesSettled.put(dRange, new Replica(dAddress, dRange, true));
        rangeAddressesSettled.put(cPrimeRange, new Replica(dAddress, cPrimeRange, true));
        rangeAddressesSettled.put(aPrimeRange, new Replica(dAddress, aPrimeRange, false));

        rangeAddressesSettled.put(eRange, new Replica(eAddress, eRange, true));
        rangeAddressesSettled.put(dRange, new Replica(eAddress, dRange, true));
        rangeAddressesSettled.put(cPrimeRange, new Replica(eAddress, cPrimeRange, false));

        return Pair.create(rangeAddresses, rangeAddressesSettled);
    }

    private Pair<ReplicaMultimap<Range<Token>, ReplicaSet>, ReplicaMultimap<Range<Token>, ReplicaList>> constructRangeAddressesMoveBackward()
    {
        ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses = ReplicaMultimap.set();
        rangeAddresses.put(aRange, new Replica(aAddress, aRange, true));
        rangeAddresses.put(eRange, new Replica(aAddress, eRange, true));
        rangeAddresses.put(dRange, new Replica(aAddress, dRange, false));

        rangeAddresses.put(bRange, new Replica(bAddress, bRange, true));
        rangeAddresses.put(aRange, new Replica(bAddress, aRange, true));
        rangeAddresses.put(eRange, new Replica(bAddress, eRange, false));

        rangeAddresses.put(cRange, new Replica(cAddress, cRange, true));
        rangeAddresses.put(bRange, new Replica(cAddress, bRange, true));
        rangeAddresses.put(aRange, new Replica(cAddress, aRange, false));

        rangeAddresses.put(dRange, new Replica(dAddress, dRange, true));
        rangeAddresses.put(cRange, new Replica(dAddress, cRange, true));
        rangeAddresses.put(bRange, new Replica(dAddress, bRange, false));

        rangeAddresses.put(eRange, new Replica(eAddress, eRange, true));
        rangeAddresses.put(dRange, new Replica(eAddress, dRange, true));
        rangeAddresses.put(cRange, new Replica(eAddress, cRange, false));

        Range aPrimeRange = new Range(oneToken, twoToken);
        Range bPrimeRange = new Range(twoToken, sixToken);

        ReplicaMultimap<Range<Token>, ReplicaList> rangeAddressesSettled = ReplicaMultimap.list();
        rangeAddressesSettled.put(aPrimeRange, new Replica(aAddress, aPrimeRange, true));
        rangeAddressesSettled.put(dRange, new Replica(aAddress, dRange, true));
        rangeAddressesSettled.put(cRange, new Replica(aAddress, cRange, false));

        rangeAddressesSettled.put(bPrimeRange, new Replica(bAddress, bPrimeRange, true));
        rangeAddressesSettled.put(aPrimeRange, new Replica(bAddress, aPrimeRange, true));
        rangeAddressesSettled.put(eRange, new Replica(bAddress, eRange, false));

        rangeAddressesSettled.put(cRange, new Replica(cAddress, cRange, true));
        rangeAddressesSettled.put(bPrimeRange, new Replica(cAddress, bPrimeRange, true));
        rangeAddressesSettled.put(aPrimeRange, new Replica(cAddress, aPrimeRange, false));

        rangeAddressesSettled.put(dRange, new Replica(dAddress, dRange, true));
        rangeAddressesSettled.put(cRange, new Replica(dAddress, cRange, true));
        rangeAddressesSettled.put(bPrimeRange, new Replica(dAddress, bPrimeRange, false));

        rangeAddressesSettled.put(eRange, new Replica(eAddress, eRange, true));
        rangeAddressesSettled.put(dRange, new Replica(eAddress, dRange, true));
        rangeAddressesSettled.put(cRange, new Replica(eAddress, cRange, false));

        return Pair.create(rangeAddresses, rangeAddressesSettled);
    }

    private Pair<ReplicaMultimap<Range<Token>, ReplicaSet>, ReplicaMultimap<Range<Token>, ReplicaList>> constructRangeAddressesMoveForward()
    {
        ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses = ReplicaMultimap.set();
        rangeAddresses.put(aRange, new Replica(aAddress, aRange, true));
        rangeAddresses.put(eRange, new Replica(aAddress, eRange, true));
        rangeAddresses.put(dRange, new Replica(aAddress, dRange, false));

        rangeAddresses.put(bRange, new Replica(bAddress, bRange, true));
        rangeAddresses.put(aRange, new Replica(bAddress, aRange, true));
        rangeAddresses.put(eRange, new Replica(bAddress, eRange, false));

        rangeAddresses.put(cRange, new Replica(cAddress, cRange, true));
        rangeAddresses.put(bRange, new Replica(cAddress, bRange, true));
        rangeAddresses.put(aRange, new Replica(cAddress, aRange, false));

        rangeAddresses.put(dRange, new Replica(dAddress, dRange, true));
        rangeAddresses.put(cRange, new Replica(dAddress, cRange, true));
        rangeAddresses.put(bRange, new Replica(dAddress, bRange, false));

        rangeAddresses.put(eRange, new Replica(eAddress, eRange, true));
        rangeAddresses.put(dRange, new Replica(eAddress, dRange, true));
        rangeAddresses.put(cRange, new Replica(eAddress, cRange, false));

        Range aPrimeRange = new Range(oneToken, fourToken);
        Range bPrimeRange = new Range(fourToken, sixToken);

        ReplicaMultimap<Range<Token>, ReplicaList> rangeAddressesSettled = ReplicaMultimap.list();
        rangeAddressesSettled.put(aPrimeRange, new Replica(aAddress, aPrimeRange, true));
        rangeAddressesSettled.put(eRange, new Replica(aAddress, eRange, true));
        rangeAddressesSettled.put(cRange, new Replica(aAddress, cRange, false));

        rangeAddressesSettled.put(bPrimeRange, new Replica(bAddress, bPrimeRange, true));
        rangeAddressesSettled.put(aPrimeRange, new Replica(bAddress, aPrimeRange, true));
        rangeAddressesSettled.put(eRange, new Replica(bAddress, eRange, false));

        rangeAddressesSettled.put(cRange, new Replica(cAddress, cRange, true));
        rangeAddressesSettled.put(bPrimeRange, new Replica(cAddress, bPrimeRange, true));
        rangeAddressesSettled.put(aPrimeRange, new Replica(cAddress, aPrimeRange, false));

        rangeAddressesSettled.put(dRange, new Replica(dAddress, dRange, true));
        rangeAddressesSettled.put(cRange, new Replica(dAddress, cRange, true));
        rangeAddressesSettled.put(bPrimeRange, new Replica(dAddress, bPrimeRange, false));

        rangeAddressesSettled.put(eRange, new Replica(eAddress, eRange, true));
        rangeAddressesSettled.put(dRange, new Replica(eAddress, dRange, true));
        rangeAddressesSettled.put(cRange, new Replica(eAddress, cRange, false));

        return Pair.create(rangeAddresses, rangeAddressesSettled);
    }


    @Test
    public void testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        ReplicaMultimap<Replica, ReplicaList> expectedResult = ReplicaMultimap.list();

        InetAddressAndPort cOrB = (downNodes.contains(cAddress) || sourceFilterDownNodes.contains(cAddress)) ? bAddress : cAddress;

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(new Replica(aAddress, new Range(sixToken, sevenToken), true),  new Replica(dAddress, new Range(sixToken, nineToken), true));
        expectedResult.put(new Replica(aAddress, new Range(sixToken, sevenToken), true), new Replica(eAddress, new Range(sixToken, nineToken), false));

        //Same need both here as well
        expectedResult.put(new Replica(aAddress, new Range(threeToken, sixToken), true), new Replica(cOrB, new Range(threeToken, sixToken), true));
        expectedResult.put(new Replica(aAddress, new Range(threeToken, sixToken), true), new Replica(dAddress, new Range(threeToken, sixToken), false));

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForwardBetween().right,
                                                           constructRangeAddressesMoveForwardBetween(),
                                                           expectedResult);
    }

    @Test
    public void testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodes() throws Exception
    {
        for (InetAddressAndPort downNode : new InetAddressAndPort[] {dAddress, eAddress})
        {
            downNodes.clear();
            downNodes.add(downNode);
            boolean threw = false;
            try
            {
                testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
            }
            catch (IllegalStateException ise)
            {
                ise.printStackTrace();
                assertTrue(downNode.toString(),
                           ise.getMessage().startsWith("A node required to move the data consistently is down:")
                                    && ise.getMessage().contains(downNode.toString()));
                threw = true;
            }
            if (!threw)
            {
                System.out.println("Didn't throw for " + downNode);
            }
            assertTrue(threw);
        }

        //Shouldn't throw because another full replica is available
        downNodes.clear();
        downNodes.add(cAddress);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test
    public void testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodesSourceFilter() throws Exception
    {
        for (InetAddressAndPort downNode : new InetAddressAndPort[] {dAddress, eAddress})
        {
            sourceFilterDownNodes.clear();
            sourceFilterDownNodes.add(downNode);
            boolean threw = false;
            try
            {
                testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
            }
            catch (IllegalStateException ise)
            {
                ise.printStackTrace();
                assertTrue(downNode.toString(),
                           ise.getMessage().startsWith("Necessary replicas for strict consistency were removed by source filters:")
                           && ise.getMessage().contains(downNode.toString()));
                threw = true;
            }
            if (!threw)
            {
                System.out.println("Didn't throw for " + downNode);
            }
            assertTrue(threw);
        }

        //Shouldn't throw because another full replica is available
        sourceFilterDownNodes.clear();
        sourceFilterDownNodes.add(cAddress);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }


    @Test
    public void testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        ReplicaMultimap<Replica, ReplicaList> expectedResult = ReplicaMultimap.list();

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(new Replica(aAddress, new Range(nineToken, elevenToken), true), new Replica(eAddress, new Range(nineToken, elevenToken), true));
        expectedResult.put(new Replica(aAddress, new Range(sixToken, nineToken), false), new Replica(eAddress, new Range(sixToken, nineToken), false));

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackwardBetween().right,
                                                           constructRangeAddressesMoveBackwardBetween(),
                                                           expectedResult);

    }

    @Test(expected = IllegalStateException.class)
    public void testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodes() throws Exception
    {
        //Any replica can be the full replica so this will always fail on the transient range
        downNodes.add(eAddress);
        testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test(expected = IllegalStateException.class)
    public void testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodesSourceFilter() throws Exception
    {
        //Any replica can be the full replica so this will always fail on the transient range
        sourceFilterDownNodes.add(eAddress);
        testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }


    //There is no down node version of this test because nothing needs to be fetched
    @Test
    public void testMoveBackwardCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        //Moving backwards should fetch nothing and fetch ranges is emptys so this doesn't test a ton
        ReplicaMultimap<Replica, ReplicaList> expectedResult = ReplicaMultimap.list();

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackward().right,
                                                           constructRangeAddressesMoveBackward(),
                                                           expectedResult);

    }

    @Test
    public void testMoveForwardCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        ReplicaMultimap<Replica, ReplicaList> expectedResult = ReplicaMultimap.list();

        InetAddressAndPort cOrBAddress = (downNodes.contains(cAddress) || sourceFilterDownNodes.contains(cAddress)) ? bAddress : cAddress;

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(new Replica(aAddress, new Range(threeToken, fourToken), true), new Replica(cOrBAddress, new Range(threeToken, sixToken), true));
        expectedResult.put(new Replica(aAddress, new Range(threeToken, fourToken), true), new Replica(dAddress, new Range(threeToken, sixToken), false));

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForward().right,
                                                           constructRangeAddressesMoveForward(),
                                                           expectedResult);

    }

    @Test
    public void testMoveForwardCalculateRangesToFetchWithPreferredEndpointsDownNodes() throws Exception
    {
        downNodes.add(dAddress);
        boolean threw = false;
        try
        {
            testMoveForwardCalculateRangesToFetchWithPreferredEndpoints();
        }
        catch (IllegalStateException ise)
        {
            ise.printStackTrace();
            assertTrue(dAddress.toString(),
                       ise.getMessage().startsWith("A node required to move the data consistently is down:")
                       && ise.getMessage().contains(dAddress.toString()));
            threw = true;
        }
        if (!threw)
        {
            System.out.println("Didn't throw for " + dAddress);
        }
        assertTrue(threw);

        //Shouldn't throw because another full replica is available
        downNodes.clear();
        downNodes.add(cAddress);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test
    public void testMoveForwardCalculateRangesToFetchWithPreferredEndpointsDownNodesSourceFilter() throws Exception
    {
        sourceFilterDownNodes.add(dAddress);
        boolean threw = false;
        try
        {
            testMoveForwardCalculateRangesToFetchWithPreferredEndpoints();
        }
        catch (IllegalStateException ise)
        {
            ise.printStackTrace();
            assertTrue(dAddress.toString(),
                       ise.getMessage().startsWith("Necessary replicas for strict consistency were removed by source filters:")
                       && ise.getMessage().contains(dAddress.toString()));
            threw = true;
        }
        if (!threw)
        {
            System.out.println("Didn't throw for " + dAddress);
        }
        assertTrue(threw);

        //Shouldn't throw because another full replica is available
        sourceFilterDownNodes.clear();
        sourceFilterDownNodes.add(cAddress);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    private void invokeCalculateRangesToFetchWithPreferredEndpoints(ReplicaSet toFetch,
                                                                    Pair<ReplicaMultimap<Range<Token>, ReplicaSet>, ReplicaMultimap<Range<Token>, ReplicaList>> rangeAddresses,
                                                                    ReplicaMultimap<Replica, ReplicaList> expectedResult)
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);

        ReplicaMultimap<Replica, ReplicaList>  result = RangeStreamer.calculateRangesToFetchWithPreferredEndpoints((address, replicas) -> new ReplicaList(replicas),
                                                                                                                   rangeAddresses.left,
                                                                                                                   toFetch,
                                                                                                                   true,
                                                                                                                   token -> {
                                                                                                                       for (Range<Token> range : rangeAddresses.right.keySet())
                                                                                                                       {
                                                                                                                           if (range.contains(token))
                                                                                                                               return new ReplicaList(rangeAddresses.right.get(range));
                                                                                                                       }
                                                                                                                       return null;
                                                                                                                   },
                                                                                                                   new ReplicationFactor(3, 1),
                                                                                                                   alivePredicate,
                                                                                                                   "OldNetworkTopologyStrategyTest",
                                                                                                                   sourceFilters);
        System.out.println(result);
        assertMultimapEqualsIgnoreOrder(expectedResult, result);

    }

    @Test
    public void testMoveForwardBetweenCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        ReplicaSet toStream = calculateStreamAndFetchRangesMoveForwardBetween().left;
        StorageService.RangeRelocator relocator = new StorageService.RangeRelocator();

        ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses = constructRangeAddressesMoveForwardBetween().left;
        ReplicaMultimap<Range<Token>, ReplicaList> rangeAddressesSettled = constructRangeAddressesMoveForwardBetween().right;

        ReplicaMultimap<InetAddressAndPort, ReplicaList> result = relocator.calculateRangesToStreamWithPreferredEndpoints(toStream,
                                                                                                                          token ->
                                                                                                                          {
                                                                                                                              for (Range<Token> range : rangeAddresses.keySet())
                                                                                                                              {
                                                                                                                                  if (range.contains(token))
                                                                                                                                      return new ReplicaList(rangeAddresses.get(range));
                                                                                                                              }
                                                                                                                              return null;
                                                                                                                          },
                                                                                                                          token ->
                                                                                                                          {
                                                                                                                              for (Range<Token> range : rangeAddressesSettled.keySet())
                                                                                                                              {
                                                                                                                                  if (range.contains(token))
                                                                                                                                      return rangeAddressesSettled.get(range);
                                                                                                                              }
                                                                                                                              return null;
                                                                                                                          });
        System.out.println(result);
        ReplicaMultimap<InetAddressAndPort, ReplicaList> expectedResult = ReplicaMultimap.list();

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(bAddress, new Replica(bAddress, new Range(nineToken, elevenToken), false));
        expectedResult.put(bAddress, new Replica(bAddress, new Range(elevenToken, oneToken), true));

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForwardBetween().left,
                                                            constructRangeAddressesMoveForwardBetween(),
                                                            expectedResult);
    }

    @Test
    public void testMoveBackwardBetweenCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        ReplicaMultimap<InetAddressAndPort, ReplicaList> expectedResult = ReplicaMultimap.list();

        expectedResult.put(bAddress, new Replica(bAddress, new Range(fourteenToken, oneToken), true));

        expectedResult.put(dAddress, new Replica(dAddress, new Range(oneToken, threeToken), false));

        expectedResult.put(cAddress, new Replica(cAddress, new Range(oneToken, threeToken), true));
        expectedResult.put(cAddress, new Replica(cAddress, new Range(fourteenToken, oneToken), false));

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackwardBetween().left,
                                                            constructRangeAddressesMoveBackwardBetween(),
                                                            expectedResult);
    }

    @Test
    public void testMoveBackwardCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        ReplicaMultimap<InetAddressAndPort, ReplicaList> expectedResult = ReplicaMultimap.list();

        expectedResult.put(cAddress, new Replica(cAddress, new Range(twoToken, threeToken), true));

        expectedResult.put(dAddress, new Replica(dAddress, new Range(twoToken, threeToken), false));

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackward().left,
                                                            constructRangeAddressesMoveBackward(),
                                                            expectedResult);
    }

    @Test
    public void testMoveForwardCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        //Nothing to stream moving forward because we are acquiring more range not losing range
        ReplicaMultimap<InetAddressAndPort, ReplicaList> expectedResult = ReplicaMultimap.list();

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForward().left,
                                                            constructRangeAddressesMoveForward(),
                                                            expectedResult);
    }



    private void invokeCalculateRangesToStreamWithPreferredEndpoints(ReplicaSet toStream,
                                                                     Pair<ReplicaMultimap<Range<Token>, ReplicaSet>, ReplicaMultimap<Range<Token>, ReplicaList>> rangeAddresses,
                                                                     ReplicaMultimap<InetAddressAndPort, ReplicaList> expectedResult)
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        StorageService.RangeRelocator relocator = new StorageService.RangeRelocator();
        ReplicaMultimap<InetAddressAndPort, ReplicaList> result = relocator.calculateRangesToStreamWithPreferredEndpoints(toStream,
                                                                                                                          token ->
                                                                                                                          {
                                                                                                                              for (Range<Token> range : rangeAddresses.left.keySet())
                                                                                                                              {
                                                                                                                                  if (range.contains(token))
                                                                                                                                      return new ReplicaList(rangeAddresses.left.get(range));
                                                                                                                              }
                                                                                                                              return null;
                                                                                                                          },
                                                                                                                          token ->
                                                                                                                          {
                                                                                                                              for (Range<Token> range : rangeAddresses.right.keySet())
                                                                                                                              {
                                                                                                                                  if (range.contains(token))
                                                                                                                                      return rangeAddresses.right.get(range);
                                                                                                                              }
                                                                                                                              return null;
                                                                                                                          });
        System.out.println(result);
        assertMultimapEqualsIgnoreOrder(expectedResult, result);
    }

    public static <K>  void assertMultimapEqualsIgnoreOrder(ReplicaMultimap<K, ReplicaList> a, ReplicaMultimap<K, ReplicaList> b)
    {
        if (!a.keySet().equals(b.keySet()))
            assertEquals(a, b);
        for (K key : a.keySet())
        {
            ReplicaList aList = a.get(key);
            ReplicaList bList = b.get(key);
            if (a == null && b == null)
                return;
            if (aList.size() != bList.size())
                assertEquals(a, b);
            for (Replica r : aList)
            {
                if (!bList.anyMatch(replica -> r.equals(replica)))
                    assertEquals(a, b);
            }
        }
    }

}
