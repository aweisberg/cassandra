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
package org.apache.cassandra.locator;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OldNetworkTopologyStrategyTest
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

    private List<Token> keyTokens;
    private TokenMetadata tmd;
    private Map<String, ArrayList<InetAddressAndPort>> expectedResults;


    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void init() throws Exception
    {
        keyTokens = new ArrayList<Token>();
        tmd = new TokenMetadata();
        expectedResults = new HashMap<String, ArrayList<InetAddressAndPort>>();
    }

    /**
     * 4 same rack endpoints
     *
     * @throws java.net.UnknownHostException
     */
    @Test
    public void testBigIntegerEndpointsA() throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tmd, endpointSnitch, optsWithRF(1));
        addEndpoint("0", "5", "254.0.0.1");
        addEndpoint("10", "15", "254.0.0.2");
        addEndpoint("20", "25", "254.0.0.3");
        addEndpoint("30", "35", "254.0.0.4");

        expectedResults.put("5", buildResult("254.0.0.2", "254.0.0.3", "254.0.0.4"));
        expectedResults.put("15", buildResult("254.0.0.3", "254.0.0.4", "254.0.0.1"));
        expectedResults.put("25", buildResult("254.0.0.4", "254.0.0.1", "254.0.0.2"));
        expectedResults.put("35", buildResult("254.0.0.1", "254.0.0.2", "254.0.0.3"));

        testGetEndpoints(strategy, keyTokens.toArray(new Token[0]));
    }

    /**
     * 3 same rack endpoints
     * 1 external datacenter
     *
     * @throws java.net.UnknownHostException
     */
    @Test
    public void testBigIntegerEndpointsB() throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tmd, endpointSnitch, optsWithRF(1));
        addEndpoint("0", "5", "254.0.0.1");
        addEndpoint("10", "15", "254.0.0.2");
        addEndpoint("20", "25", "254.1.0.3");
        addEndpoint("30", "35", "254.0.0.4");

        expectedResults.put("5", buildResult("254.0.0.2", "254.1.0.3", "254.0.0.4"));
        expectedResults.put("15", buildResult("254.1.0.3", "254.0.0.4", "254.0.0.1"));
        expectedResults.put("25", buildResult("254.0.0.4", "254.1.0.3", "254.0.0.1"));
        expectedResults.put("35", buildResult("254.0.0.1", "254.1.0.3", "254.0.0.2"));

        testGetEndpoints(strategy, keyTokens.toArray(new Token[0]));
    }

    /**
     * 2 same rack endpoints
     * 1 same datacenter, different rack endpoints
     * 1 external datacenter
     *
     * @throws java.net.UnknownHostException
     */
    @Test
    public void testBigIntegerEndpointsC() throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tmd, endpointSnitch, optsWithRF(1));
        addEndpoint("0", "5", "254.0.0.1");
        addEndpoint("10", "15", "254.0.0.2");
        addEndpoint("20", "25", "254.0.1.3");
        addEndpoint("30", "35", "254.1.0.4");

        expectedResults.put("5", buildResult("254.0.0.2", "254.0.1.3", "254.1.0.4"));
        expectedResults.put("15", buildResult("254.0.1.3", "254.1.0.4", "254.0.0.1"));
        expectedResults.put("25", buildResult("254.1.0.4", "254.0.0.1", "254.0.0.2"));
        expectedResults.put("35", buildResult("254.0.0.1", "254.0.1.3", "254.1.0.4"));

        testGetEndpoints(strategy, keyTokens.toArray(new Token[0]));
    }

    private ArrayList<InetAddressAndPort> buildResult(String... addresses) throws UnknownHostException
    {
        ArrayList<InetAddressAndPort> result = new ArrayList<>();
        for (String address : addresses)
        {
            result.add(InetAddressAndPort.getByName(address));
        }
        return result;
    }

    private void addEndpoint(String endpointTokenID, String keyTokenID, String endpointAddress) throws UnknownHostException
    {
        BigIntegerToken endpointToken = new BigIntegerToken(endpointTokenID);

        BigIntegerToken keyToken = new BigIntegerToken(keyTokenID);
        keyTokens.add(keyToken);

        InetAddressAndPort ep = InetAddressAndPort.getByName(endpointAddress);
        tmd.updateNormalToken(endpointToken, ep);
    }

    private void testGetEndpoints(AbstractReplicationStrategy strategy, Token[] keyTokens)
    {
        for (Token keyToken : keyTokens)
        {
            List<InetAddressAndPort> endpoints = strategy.getNaturalReplicas(keyToken).asEndpointList();
            for (int j = 0; j < endpoints.size(); j++)
            {
                ArrayList<InetAddressAndPort> hostsExpected = expectedResults.get(keyToken.toString());
                assertEquals(endpoints.get(j), hostsExpected.get(j));
            }
        }
    }

    /**
     * test basic methods to move a node. For sure, it's not the best place, but it's easy to test
     *
     * @throws java.net.UnknownHostException
     */
    @Test
    public void testMoveLeft() throws UnknownHostException
    {
        // Moves to the left : nothing to fetch, last part to stream

        int movingNodeIdx = 1;
        BigIntegerToken newToken = new BigIntegerToken("21267647932558653966460912964485513216");
        BigIntegerToken[] tokens = initTokens();
        BigIntegerToken[] tokensAfterMove = initTokensAfterMove(tokens, movingNodeIdx, newToken);
        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = calculateStreamAndFetchRanges(tokens, tokensAfterMove, movingNodeIdx);

        assertEquals(ranges.left.iterator().next().left, tokensAfterMove[movingNodeIdx]);
        assertEquals(ranges.left.iterator().next().right, tokens[movingNodeIdx]);
        assertEquals("No data should be fetched", ranges.right.size(), 0);
    }

    @Test
    public void testMoveRight() throws UnknownHostException
    {
        // Moves to the right : last part to fetch, nothing to stream

        int movingNodeIdx = 1;
        BigIntegerToken newToken = new BigIntegerToken("35267647932558653966460912964485513216");
        BigIntegerToken[] tokens = initTokens();
        BigIntegerToken[] tokensAfterMove = initTokensAfterMove(tokens, movingNodeIdx, newToken);
        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = calculateStreamAndFetchRanges(tokens, tokensAfterMove, movingNodeIdx);

        assertEquals("No data should be streamed", ranges.left.size(), 0);
        assertEquals(ranges.right.iterator().next().left, tokens[movingNodeIdx]);
        assertEquals(ranges.right.iterator().next().right, tokensAfterMove[movingNodeIdx]);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMoveMiddleOfRing() throws UnknownHostException
    {
        // moves to another position in the middle of the ring : should stream all its data, and fetch all its new data

        int movingNodeIdx = 1;
        int movingNodeIdxAfterMove = 4;
        BigIntegerToken newToken = new BigIntegerToken("90070591730234615865843651857942052864");
        BigIntegerToken[] tokens = initTokens();
        BigIntegerToken[] tokensAfterMove = initTokensAfterMove(tokens, movingNodeIdx, newToken);
        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = calculateStreamAndFetchRanges(tokens, tokensAfterMove, movingNodeIdx);

        // sort the results, so they can be compared
        Range<Token>[] toStream = ranges.left.toArray(new Range[0]);
        Range<Token>[] toFetch = ranges.right.toArray(new Range[0]);
        Arrays.sort(toStream);
        Arrays.sort(toFetch);

        // build expected ranges
        Range<Token>[] toStreamExpected = new Range[2];
        toStreamExpected[0] = new Range<Token>(getToken(movingNodeIdx - 2, tokens), getToken(movingNodeIdx - 1, tokens));
        toStreamExpected[1] = new Range<Token>(getToken(movingNodeIdx - 1, tokens), getToken(movingNodeIdx, tokens));
        Arrays.sort(toStreamExpected);
        Range<Token>[] toFetchExpected = new Range[2];
        toFetchExpected[0] = new Range<Token>(getToken(movingNodeIdxAfterMove - 1, tokens), getToken(movingNodeIdxAfterMove, tokens));
        toFetchExpected[1] = new Range<Token>(getToken(movingNodeIdxAfterMove, tokensAfterMove), getToken(movingNodeIdx, tokensAfterMove));
        Arrays.sort(toFetchExpected);

        assertEquals(Arrays.equals(toStream, toStreamExpected), true);
        assertEquals(Arrays.equals(toFetch, toFetchExpected), true);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMoveAfterNextNeighbors() throws UnknownHostException
    {
        // moves after its next neighbor in the ring

        int movingNodeIdx = 1;
        int movingNodeIdxAfterMove = 2;
        BigIntegerToken newToken = new BigIntegerToken("52535295865117307932921825928971026432");
        BigIntegerToken[] tokens = initTokens();
        BigIntegerToken[] tokensAfterMove = initTokensAfterMove(tokens, movingNodeIdx, newToken);
        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = calculateStreamAndFetchRanges(tokens, tokensAfterMove, movingNodeIdx);


        // sort the results, so they can be compared
        Range<Token>[] toStream = ranges.left.toArray(new Range[0]);
        Range<Token>[] toFetch = ranges.right.toArray(new Range[0]);
        Arrays.sort(toStream);
        Arrays.sort(toFetch);

        // build expected ranges
        Range<Token>[] toStreamExpected = new Range[1];
        toStreamExpected[0] = new Range<Token>(getToken(movingNodeIdx - 2, tokens), getToken(movingNodeIdx - 1, tokens));
        Arrays.sort(toStreamExpected);
        Range<Token>[] toFetchExpected = new Range[2];
        toFetchExpected[0] = new Range<Token>(getToken(movingNodeIdxAfterMove - 1, tokens), getToken(movingNodeIdxAfterMove, tokens));
        toFetchExpected[1] = new Range<Token>(getToken(movingNodeIdxAfterMove, tokensAfterMove), getToken(movingNodeIdx, tokensAfterMove));
        Arrays.sort(toFetchExpected);

        assertEquals(Arrays.equals(toStream, toStreamExpected), true);
        assertEquals(Arrays.equals(toFetch, toFetchExpected), true);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMoveBeforePreviousNeighbor() throws UnknownHostException
    {
        // moves before its previous neighbor in the ring

        int movingNodeIdx = 1;
        int movingNodeIdxAfterMove = 7;
        BigIntegerToken newToken = new BigIntegerToken("158873535527910577765226390751398592512");
        BigIntegerToken[] tokens = initTokens();
        BigIntegerToken[] tokensAfterMove = initTokensAfterMove(tokens, movingNodeIdx, newToken);
        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = calculateStreamAndFetchRanges(tokens, tokensAfterMove, movingNodeIdx);

        Range<Token>[] toStream = ranges.left.toArray(new Range[0]);
        Range<Token>[] toFetch = ranges.right.toArray(new Range[0]);
        Arrays.sort(toStream);
        Arrays.sort(toFetch);

        Range<Token>[] toStreamExpected = new Range[2];
        toStreamExpected[0] = new Range<Token>(getToken(movingNodeIdx, tokensAfterMove), getToken(movingNodeIdx - 1, tokensAfterMove));
        toStreamExpected[1] = new Range<Token>(getToken(movingNodeIdx - 1, tokens), getToken(movingNodeIdx, tokens));
        Arrays.sort(toStreamExpected);
        Range<Token>[] toFetchExpected = new Range[1];
        toFetchExpected[0] = new Range<Token>(getToken(movingNodeIdxAfterMove - 1, tokens), getToken(movingNodeIdxAfterMove, tokens));
        Arrays.sort(toFetchExpected);

        System.out.println("toStream : " + Arrays.toString(toStream));
        System.out.println("toFetch : " + Arrays.toString(toFetch));
        System.out.println("toStreamExpected : " + Arrays.toString(toStreamExpected));
        System.out.println("toFetchExpected : " + Arrays.toString(toFetchExpected));

        assertEquals(Arrays.equals(toStream, toStreamExpected), true);
        assertEquals(Arrays.equals(toFetch, toFetchExpected), true);
    }

    Token oneToken = new BigIntegerToken("1");
    Token twoToken = new BigIntegerToken("2");
    Token threeToken = new BigIntegerToken("3");
    Token fourToken = new BigIntegerToken("4");
    Token sixToken = new BigIntegerToken("6");
    Token sevenToken = new BigIntegerToken("7");
    Token nineToken = new BigIntegerToken("9");
    Token elevenToken = new BigIntegerToken("11");
    Token fourteenToken = new BigIntegerToken("14");
    Token negativeToken = new BigIntegerToken(Integer.toString(Integer.MIN_VALUE));

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

        assertEquals(ReplicaSet.EMPTY, result.left);
        assertEquals(ReplicaSet.of(new Replica(aAddress, new Range(threeToken, fourToken), true)), result.right);
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
    public void testCalculateStreamAndFetchRangesMoveBackwardsBetween() throws Exception
    {
        calculateStreamAndFetchRangesMoveBackwardsBetween();
    }

    public Pair<ReplicaSet, ReplicaSet> calculateStreamAndFetchRangesMoveBackwardsBetween() throws Exception
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
    public void testCalculateStreamAndFetchRangesMoveBackwards() throws Exception
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
    }

    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-11 E 11-1
     * A's moves from 3 to 7
     *
     * @throws Exception
     */
    private Pair<ReplicaSet, ReplicaSet> calculateStreamAndFetchRangesMoveForwardsBetween() throws Exception
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
    public void testCalculateStreamAndFetchRangesMoveForwardsBetween() throws Exception
    {
        calculateStreamAndFetchRangesMoveForwardsBetween();
    }

    /**
     * Construct the ring state for calculateStreamAndFetchRangesMoveBackwardsBetween
     * Where are A moves from 3 to 14
     * @return
     */
    private Pair<ReplicaMultimap<Range<Token>, ReplicaSet>, ReplicaMultimap<Range<Token>, ReplicaList>> constructRangeAddressesMoveBackwardsBetween()
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
     * Construct the ring state for calculateStreamAndFetchRangesMoveForwardsBetween
     * Where are A moves from 3 to 7
     * @return
     */
    private Pair<ReplicaMultimap<Range<Token>, ReplicaSet>, ReplicaMultimap<Range<Token>, ReplicaList>> constructRangeAddressesMoveForwardsBetween()
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

    @Test
    public void testMoveForwardsBetweenCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        ReplicaSet toFetch = calculateStreamAndFetchRangesMoveForwardsBetween().right;

        ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses = constructRangeAddressesMoveForwardsBetween().left;
        ReplicaMultimap<Range<Token>, ReplicaList> rangeAddressesSettled = constructRangeAddressesMoveForwardsBetween().right;

        ReplicaMultimap<Replica, ReplicaList>  result = RangeStreamer.calculateRangesToFetchWithPreferredEndpoints((address, replicas) -> new ReplicaList(replicas),
                                                               rangeAddresses,
                                                               toFetch,
                                                               true,
                                                               token -> {
                                                                 for (Range<Token> range : rangeAddressesSettled.keySet())
                                                                 {
                                                                     if (range.contains(token))
                                                                         return new ReplicaList(rangeAddressesSettled.get(range));
                                                                 }
                                                                   return null;
                                                               },
                                                               new ReplicationFactor(3, 1),
                                                               Predicates.alwaysTrue(),
                                                               "OldNetworkTopologyStrategyTest",
                                                               Collections.emptyList());
        System.out.println(result);
        ReplicaMultimap<Replica, ReplicaList> expectedResult = ReplicaMultimap.list();

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(new Replica(aAddress, new Range(sixToken, sevenToken), true),  new Replica(dAddress, new Range(sixToken, nineToken), true));
        expectedResult.put(new Replica(aAddress, new Range(sixToken, sevenToken), true), new Replica(eAddress, new Range(sixToken, nineToken), false));

        //Same need both here as well
        expectedResult.put(new Replica(aAddress, new Range(threeToken, sixToken), true), new Replica(cAddress, new Range(threeToken, sixToken), true));
        expectedResult.put(new Replica(aAddress, new Range(threeToken, sixToken), true), new Replica(dAddress, new Range(threeToken, sixToken), false));

        assertMultimapEqualsIgnoreOrder(expectedResult, result);
    }

    @Test
    public void testMoveBackwardsBetweenCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        runMoveBackwardsBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    public ReplicaMultimap<Replica, ReplicaList> runMoveBackwardsBetweenCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        ReplicaSet toFetch = calculateStreamAndFetchRangesMoveBackwardsBetween().right;

        ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses = constructRangeAddressesMoveBackwardsBetween().left;
        ReplicaMultimap<Range<Token>, ReplicaList> rangeAddressesSettled = constructRangeAddressesMoveBackwardsBetween().right;

        ReplicaMultimap<Replica, ReplicaList>  result = RangeStreamer.calculateRangesToFetchWithPreferredEndpoints((address, replicas) -> new ReplicaList(replicas),
                                                                                                               rangeAddresses,
                                                                                                               toFetch,
                                                                                                               true,
                                                                                                               token -> {
                                                                                                                   for (Range<Token> range : rangeAddressesSettled.keySet())
                                                                                                                   {
                                                                                                                       if (range.contains(token))
                                                                                                                           return new ReplicaList(rangeAddressesSettled.get(range));
                                                                                                                   }
                                                                                                                   return null;
                                                                                                               },
                                                                                                               new ReplicationFactor(3, 1),
                                                                                                               Predicates.alwaysTrue(),
                                                                                                               "OldNetworkTopologyStrategyTest",
                                                                                                               Collections.emptyList());
        System.out.println(result);
        ReplicaMultimap<Replica, ReplicaList> expectedResult = ReplicaMultimap.list();

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(new Replica(aAddress, new Range(nineToken, elevenToken), true), new Replica(eAddress, new Range(nineToken, elevenToken), true));
        expectedResult.put(new Replica(aAddress, new Range(sixToken, nineToken), false), new Replica(eAddress, new Range(sixToken, nineToken), false));
        assertMultimapEqualsIgnoreOrder(expectedResult, result);
        return result;
    }

    @Test
    public void testMoveForwardsBetweenCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        ReplicaSet toStream = calculateStreamAndFetchRangesMoveForwardsBetween().left;
        StorageService.RangeRelocator relocator = new StorageService.RangeRelocator();

        ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses = constructRangeAddressesMoveForwardsBetween().left;
        ReplicaMultimap<Range<Token>, ReplicaList> rangeAddressesSettled = constructRangeAddressesMoveForwardsBetween().right;

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
        assertMultimapEqualsIgnoreOrder(expectedResult, result);
    }

    @Test
    public void testMoveBackwardsBetweenCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        ReplicaSet toStream = calculateStreamAndFetchRangesMoveBackwardsBetween().left;
        StorageService.RangeRelocator relocator = new StorageService.RangeRelocator();

        ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses = constructRangeAddressesMoveBackwardsBetween().left;
        ReplicaMultimap<Range<Token>, ReplicaList> rangeAddressesSettled = constructRangeAddressesMoveBackwardsBetween().right;

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

        expectedResult.put(bAddress, new Replica(bAddress, new Range(fourteenToken, oneToken), true));

        expectedResult.put(dAddress, new Replica(dAddress, new Range(oneToken, threeToken), false));

        expectedResult.put(cAddress, new Replica(cAddress, new Range(oneToken, threeToken), true));
        expectedResult.put(cAddress, new Replica(cAddress, new Range(fourteenToken, oneToken), false));

        assertMultimapEqualsIgnoreOrder(expectedResult, result);
    }

    @Test
    public void testGetWorkMap() throws Exception
    {
//        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
//        ReplicaMultimap<Replica, ReplicaList> rangesToFetchWithPreferredEndpoints = runMoveBackwardsBetweenCalculateRangesToFetchWithPreferredEndpoints();
//        ReplicaMultimap<InetAddressAndPort, ReplicaSet> shitToFetch = RangeStreamer.getRangeFetchMap(rangesToFetchWithPreferredEndpoints, Collections.emptyList(), "OldNetworkTopologyStrategyTest", true);
//        System.out.println(shitToFetch);
    }

    private BigIntegerToken[] initTokensAfterMove(BigIntegerToken[] tokens,
            int movingNodeIdx, BigIntegerToken newToken)
    {
        BigIntegerToken[] tokensAfterMove = tokens.clone();
        tokensAfterMove[movingNodeIdx] = newToken;
        return tokensAfterMove;
    }

    private BigIntegerToken[] initTokens()
    {
        BigIntegerToken[] tokens = new BigIntegerToken[] {
                new BigIntegerToken("0"), // just to be able to test
                new BigIntegerToken("34028236692093846346337460743176821145"),
                new BigIntegerToken("42535295865117307932921825928971026432"),
                new BigIntegerToken("63802943797675961899382738893456539648"),
                new BigIntegerToken("85070591730234615865843651857942052864"),
                new BigIntegerToken("106338239662793269832304564822427566080"),
                new BigIntegerToken("127605887595351923798765477786913079296"),
                new BigIntegerToken("148873535527910577765226390751398592512")
        };
        return tokens;
    }

    private TokenMetadata initTokenMetadata(BigIntegerToken[] tokens)
            throws UnknownHostException
    {
        TokenMetadata tokenMetadataCurrent = new TokenMetadata();

        int lastIPPart = 1;
        for (BigIntegerToken token : tokens)
            tokenMetadataCurrent.updateNormalToken(token, InetAddressAndPort.getByName("254.0.0." + Integer.toString(lastIPPart++)));

        return tokenMetadataCurrent;
    }

    private BigIntegerToken getToken(int idx, BigIntegerToken[] tokens)
    {
        if (idx >= tokens.length)
            idx = idx % tokens.length;
        while (idx < 0)
            idx += tokens.length;

        return tokens[idx];

    }

    private Pair<Set<Range<Token>>, Set<Range<Token>>> calculateStreamAndFetchRanges(BigIntegerToken[] tokens, BigIntegerToken[] tokensAfterMove, int movingNodeIdx) throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        InetAddressAndPort movingNode = InetAddressAndPort.getByName("254.0.0." + Integer.toString(movingNodeIdx + 1));


        TokenMetadata tokenMetadataCurrent = initTokenMetadata(tokens);
        TokenMetadata tokenMetadataAfterMove = initTokenMetadata(tokensAfterMove);
        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tokenMetadataCurrent, endpointSnitch, optsWithRF(2));

        ReplicaSet currentRanges = strategy.getAddressReplicas().get(movingNode);
        ReplicaSet updatedRanges = strategy.getPendingAddressRanges(tokenMetadataAfterMove, tokensAfterMove[movingNodeIdx], movingNode);

        return asRanges(StorageService.calculateStreamAndFetchRanges(currentRanges, updatedRanges));
    }

    private static Map<String, String> optsWithRF(int rf)
    {
        return Collections.singletonMap("replication_factor", Integer.toString(rf));
    }

    public static Pair<Set<Range<Token>>, Set<Range<Token>>> asRanges(Pair<ReplicaSet, ReplicaSet> replicas)
    {
        Set<Range<Token>> leftRanges = replicas.left.asRangeSet();
        Set<Range<Token>> rightRanges = replicas.right.asRangeSet();
        return Pair.create(leftRanges, rightRanges);
    }

    public static <K>  void assertMultimapEqualsIgnoreOrder(ReplicaMultimap<K, ReplicaList> a, ReplicaMultimap<K, ReplicaList> b)
    {
        assertEquals(a.keySet(), b.keySet());
        for (K key : a.keySet())
        {
            ReplicaList aList = a.get(key);
            ReplicaList bList = b.get(key);
            if (a == null && b == null)
                return;
            assertEquals(aList.size(), bList.size());
            for (Replica r : aList)
            {
                if (!bList.anyMatch(replica -> r.equals(replica)))
                    assertEquals(a, b);
            }
        }
    }
}
