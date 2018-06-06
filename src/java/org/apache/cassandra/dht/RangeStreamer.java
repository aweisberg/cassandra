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
package org.apache.cassandra.dht;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.locator.ReplicaMultimap;
import org.apache.cassandra.locator.ReplicaSet;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Assists in streaming ranges to this node.
 */
public class RangeStreamer
{
    private static final Logger logger = LoggerFactory.getLogger(RangeStreamer.class);

    public static Predicate<Replica> ALIVE_PREDICATE = replica ->
                                                             (!Gossiper.instance.isEnabled() ||
                                                              (Gossiper.instance.getEndpointStateForEndpoint(replica.getEndpoint()) == null ||
                                                               Gossiper.instance.getEndpointStateForEndpoint(replica.getEndpoint()).isAlive())) &&
                                                             FailureDetector.instance.isAlive(replica.getEndpoint());

    /* bootstrap tokens. can be null if replacing the node. */
    private final Collection<Token> tokens;
    /* current token ring */
    private final TokenMetadata metadata;
    /* address of this node */
    private final InetAddressAndPort address;
    /* streaming description */
    private final String description;
    private final Multimap<String, Map.Entry<InetAddressAndPort, ReplicaSet>> toFetch = HashMultimap.create();
    private final Set<Predicate<Replica>> sourceFilters = new HashSet<>();
    private final StreamPlan streamPlan;
    private final boolean useStrictConsistency;
    private final IEndpointSnitch snitch;
    private final StreamStateStore stateStore;

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    public static class FailureDetectorSourceFilter implements Predicate<Replica>
    {
        private final IFailureDetector fd;

        public FailureDetectorSourceFilter(IFailureDetector fd)
        {
            this.fd = fd;
        }

        public boolean apply(Replica replica)
        {
            return fd.isAlive(replica.getEndpoint());
        }
    }

    /**
     * Source filter which excludes any endpoints that are not in a specific data center.
     */
    public static class SingleDatacenterFilter implements Predicate<Replica>
    {
        private final String sourceDc;
        private final IEndpointSnitch snitch;

        public SingleDatacenterFilter(IEndpointSnitch snitch, String sourceDc)
        {
            this.sourceDc = sourceDc;
            this.snitch = snitch;
        }

        public boolean apply(Replica replica)
        {
            return snitch.getDatacenter(replica).equals(sourceDc);
        }
    }

    /**
     * Source filter which excludes the current node from source calculations
     */
    public static class ExcludeLocalNodeFilter implements Predicate<Replica>
    {
        public boolean apply(Replica replica)
        {
            return !FBUtilities.getBroadcastAddressAndPort().equals(replica.getEndpoint());
        }
    }

    /**
     * Source filter which only includes endpoints contained within a provided set.
     */
    public static class WhitelistedSourcesFilter implements Predicate<Replica>
    {
        private final Set<InetAddressAndPort> whitelistedSources;

        public WhitelistedSourcesFilter(Set<InetAddressAndPort> whitelistedSources)
        {
            this.whitelistedSources = whitelistedSources;
        }

        public boolean apply(Replica replica)
        {
            return whitelistedSources.contains(replica.getEndpoint());
        }
    }

    public RangeStreamer(TokenMetadata metadata,
                         Collection<Token> tokens,
                         InetAddressAndPort address,
                         StreamOperation streamOperation,
                         boolean useStrictConsistency,
                         IEndpointSnitch snitch,
                         StreamStateStore stateStore,
                         boolean connectSequentially,
                         int connectionsPerHost)
    {
        Preconditions.checkArgument(streamOperation == StreamOperation.BOOTSTRAP || streamOperation == StreamOperation.REBUILD, streamOperation);
        this.metadata = metadata;
        this.tokens = tokens;
        this.address = address;
        this.description = streamOperation.getDescription();
        this.streamPlan = new StreamPlan(streamOperation, connectionsPerHost, connectSequentially, null, PreviewKind.NONE);
        this.useStrictConsistency = useStrictConsistency;
        this.snitch = snitch;
        this.stateStore = stateStore;
        streamPlan.listeners(this.stateStore);
    }

    public void addSourceFilter(Predicate<Replica> filter)
    {
        sourceFilters.add(filter);
    }

    /**
     * Add ranges to be streamed for given keyspace.
     *
     * @param keyspaceName keyspace name
     * @param replicas ranges to be streamed
     */
    public void addRanges(String keyspaceName, Replicas replicas)
    {
        if(Keyspace.open(keyspaceName).getReplicationStrategy() instanceof LocalStrategy)
        {
            logger.info("Not adding ranges for Local Strategy keyspace={}", keyspaceName);
            return;
        }

        Replicas.checkFull(replicas);

        boolean useStrictSource = useStrictSourcesForRanges(keyspaceName);
        ReplicaMultimap<Replica, ReplicaList> rangesForKeyspace = calculateRangesToFetchWithPreferredEndpoints(replicas, keyspaceName, useStrictSource);
        logger.info("Ranges for keyspace {}", rangesForKeyspace);

        for (Map.Entry<Replica, Replica> entry : rangesForKeyspace.entries())
            logger.info("{}: range {} exists on {} for keyspace {}", description, entry.getKey(), entry.getValue(), keyspaceName);

        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();

        ReplicaMultimap<InetAddressAndPort, ReplicaSet> rangeFetchMap;
        if (useStrictSource || strat == null || strat.getReplicationFactor().replicas == 1 || strat.getReplicationFactor().trans > 1)
        {
            rangeFetchMap = convertPreferredEndpointsToWorkMap(rangesForKeyspace);
        }
        else
        {
            rangeFetchMap = getOptimizedRangeFetchMap(rangesForKeyspace, sourceFilters, keyspaceName);
        }

        for (Map.Entry<InetAddressAndPort, ReplicaSet> entry : rangeFetchMap.asMap().entrySet())
        {
            if (logger.isTraceEnabled())
            {
                for (Replica r : entry.getValue())
                    logger.trace("{}: range {} for keyspace {}", description, r, keyspaceName);
            }
            toFetch.put(keyspaceName, entry);
        }
    }

    /**
     * @param keyspaceName keyspace name to check
     * @return true when the node is bootstrapping, useStrictConsistency is true and # of nodes in the cluster is more than # of replica
     */
    private boolean useStrictSourcesForRanges(String keyspaceName)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        return useStrictConsistency
                && tokens != null
                && metadata.getSizeOfAllEndpoints() != strat.getReplicationFactor().replicas;
    }

    private ReplicaMultimap<Replica, ReplicaList> calculateRangesToFetchWithPreferredEndpoints(Replicas fetchRanges, String keyspace, boolean useStrictConsistency)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspace).getReplicationStrategy();

        // Active ranges
        TokenMetadata metadataClone = metadata.cloneOnlyTokenMap();
        ReplicaMultimap<Range<Token>, ReplicaSet> addressRanges = strat.getRangeAddresses(metadataClone);

        Function<Token, ReplicaList> calculateNaturalReplicas = token -> {
            throw new AssertionError("Can't calculate natural replicas if updated tokens weren't supplied");
        };

        if (tokens != null)
        {
            // Pending ranges
            metadataClone.updateNormalTokens(tokens, address);
            calculateNaturalReplicas = token -> strat.calculateNaturalReplicas(token, metadataClone);
        }
        else if (useStrictConsistency)
        {
            throw new IllegalArgumentException("Can't ask for strict consistency and not supply tokens");
        }

        return  RangeStreamer.calculateRangesToFetchWithPreferredEndpoints((address, replicas) -> snitch.getSortedListByProximity(address, replicas),
                                                                           addressRanges,
                                                                           fetchRanges,
                                                                           useStrictConsistency,
                                                                           calculateNaturalReplicas,
                                                                           strat.getReplicationFactor(),
                                                                           ALIVE_PREDICATE,
                                                                           keyspace,
                                                                           sourceFilters);

    }

    /**
     * Get a map of all ranges and the source that will be cleaned up once this bootstrapped node is added for the given ranges.
     * For each range, the list should only contain a single source. This allows us to consistently migrate data without violating
     * consistency.
     *
     **/
     public static ReplicaMultimap<Replica, ReplicaList> calculateRangesToFetchWithPreferredEndpoints(BiFunction<InetAddressAndPort, ReplicaSet, ReplicaList> snitchGetSortedListByProximity,
                                                                                              ReplicaMultimap<Range<Token>, ReplicaSet> rangeAddresses,
                                                                                              Replicas fetchRanges,
                                                                                              boolean useStrictConsistency,
                                                                                              Function<Token, ReplicaList> calculateNaturalReplicas,
                                                                                              ReplicationFactor replicationFactor,
                                                                                              Predicate<Replica> isAlive,
                                                                                              String keyspace,
                                                                                              Collection<Predicate<Replica>> sourceFilters)
    {
        Predicate<Replica> isNotAlive = Predicates.not(isAlive);
        InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();
        System.out.printf("To fetch RN: %s%n", fetchRanges);
        System.out.printf("Fetch ranges: %s%n", rangeAddresses);

        Predicate<Replica> sourceFiltersPredicate = Predicates.and(sourceFilters);

        //This list of replicas is just candidates. With strict consistency it's going to be a narrow list.
        ReplicaMultimap<Replica, ReplicaList> rangesToFetchWithPreferredEndpoints = ReplicaMultimap.list();
        for (Replica toFetch : fetchRanges)
        {
            //Replica is sufficient for what data we need to fetch
            Predicate<Replica> replicaIsSufficientFilter = toFetch.isFull() ? Replica::isFull : Predicates.alwaysTrue();
            System.out.printf("To fetch %s%n", toFetch);
            for (Range<Token> range : rangeAddresses.keySet())
            {
                if (range.contains(toFetch.getRange()))
                {
                    Replicas endpoints;
                    Predicate<Replica> notSelf = replica -> !replica.getEndpoint().equals(localAddress);
                    if (useStrictConsistency)
                    {
                        ReplicaSet oldEndpoints = new ReplicaSet(rangeAddresses.get(range));
                        ReplicaSet newEndpoints = new ReplicaSet(calculateNaturalReplicas.apply(toFetch.getRange().right));
                        System.out.printf("Old endpoints %s%n", oldEndpoints);
                        System.out.printf("New endpoints %s%n", newEndpoints);

                        //Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                        //So we need to be careful to only be strict when endpoints == RF
                        if (oldEndpoints.size() == replicationFactor.replicas)
                        {
                            Predicate<Replica> endpointNotReplicatedAnymore = replica -> newEndpoints.noneMatch(newReplica -> newReplica.getEndpoint().equals(replica.getEndpoint()));
                            //Remove new endpoints from old endpoints based on address
                            oldEndpoints = oldEndpoints.filter(endpointNotReplicatedAnymore);

                            if (oldEndpoints.anyMatch(isNotAlive))
                                throw new IllegalStateException("A node required to move the data consistently is down: "
                                                                + oldEndpoints.filter(isNotAlive));

                            if (oldEndpoints.size() > 1)
                                throw new AssertionError("Expected <= 1 endpoint but found " + oldEndpoints);

                            //If we are transitioning from transient to full and and the set of replicas for the range is not changing
                            //we might end up with no endpoints to fetch from by address. In that case we can pick any full replica safely
                            //since we are already a transient replica.
                            //The old behavior where we might be asked to fetch ranges we don't need shouldn't occur anymore.
                            //So it's an error if we don't find what we need.
                            if (oldEndpoints.isEmpty())
                            {
                                if (toFetch.isTransient())
                                    throw new AssertionError("If there are no endpoints to fetch from then we must be transitioning from transient to full for range " + toFetch);
                                oldEndpoints = rangeAddresses.get(range).filter(Replica::isFull, notSelf, isAlive, sourceFiltersPredicate).limit(1);
                                if (oldEndpoints.isEmpty())
                                    throw new IllegalStateException("Couldn't find an alive full replica to stream from");
                            }

                            //Need an additional full replica
                            if (toFetch.isFull() && oldEndpoints.noneMatch(Replica::isFull))
                            {
                                Optional<Replica> fullReplica = rangeAddresses.get(range).findFirst(Predicates.and(Replica::isFull, notSelf, isAlive, sourceFiltersPredicate));
                                if (!fullReplica.isPresent())
                                {
                                    throw new IllegalStateException("Couldn't find an alive full replica");
                                }
                                oldEndpoints.add(fullReplica.get());
                            }
                        }
                        else
                        {
                            oldEndpoints = oldEndpoints.filter(notSelf, isAlive, replicaIsSufficientFilter);
                        }

                        endpoints = oldEndpoints;

                        //We have to check the source filters here to see if they will remove any replicas
                        //required for strict consistency
                        if (endpoints.anyMatch(Predicates.not(sourceFiltersPredicate)))
                        {
                            throw new IllegalStateException("Necessary replicas for strict consistency were removed by source filters: " + endpoints.filterToSet(Predicates.not(sourceFiltersPredicate)));
                        }
                    }
                    else
                    {
                        //Without strict consistency we have given up on correctness so no point in fetching from
                        //a random full + transient replica since it's also likely to lose data
                        endpoints = snitchGetSortedListByProximity.apply(localAddress, rangeAddresses.get(range))
                                                                  .filter(replicaIsSufficientFilter, notSelf, isAlive);
                    }

                    //Apply additional policy filters that were given to us, and establish everything remaining is alive for the strict case
                    endpoints = endpoints.filterToSet(sourceFiltersPredicate).filterToSet(isAlive);

                    // storing range and preferred endpoint set
                    rangesToFetchWithPreferredEndpoints.putAll(toFetch, endpoints);
                }
            }

            ReplicaList addressList = rangesToFetchWithPreferredEndpoints.get(toFetch);
            if (addressList == null)
                throw new IllegalStateException("Failed to find endpoints to fetch " + toFetch);

            /**
             * When we move forwards (shrink our bucket) we are the one losing a range an no one else loses
             * from that action (we also don't gain). When we move backwards there are two people losing a range. One is a full replica
             * and the other is a transient replica. So we must need fetch from two places in that case for the full range we gain.
             * For a transient range we only need to fetch from one.
             */
            if (useStrictConsistency && (addressList.count(Replica::isFull) > 1 || addressList.count(Replica::isTransient) > 1))
                throw new IllegalStateException(String.format("Multiple strict sources found for %s, sources: %s", toFetch, addressList));

            //We must have enough stuff to fetch from
            if ((toFetch.isFull() && !addressList.findFirst(Replica::isFull).isPresent()) ||
                addressList.isEmpty())
            {
                if (replicationFactor.replicas == 1)
                {
                    if (useStrictConsistency)
                    {
                        logger.warn("A node required to move the data consistently is down");
                        throw new IllegalStateException("Unable to find sufficient sources for streaming range " + toFetch + " in keyspace " + keyspace + " with RF=1. " +
                                                        "Ensure this keyspace contains replicas in the source datacenter.");
                    }
                    else
                        logger.warn("Unable to find sufficient sources for streaming range {} in keyspace {} with RF=1. " +
                                    "Keyspace might be missing data.", toFetch, keyspace);

                }
                else
                {
                    if (useStrictConsistency)
                        logger.warn("A node required to move the data consistently is down");
                    throw new IllegalStateException("Unable to find sufficient sources for streaming range " + toFetch + " in keyspace " + keyspace);
                }
            }
        }
        return rangesToFetchWithPreferredEndpoints;
    }

    public static ReplicaMultimap<InetAddressAndPort, ReplicaSet> convertPreferredEndpointsToWorkMap(ReplicaMultimap<Replica, ReplicaList> preferredEndpoints)
    {
        ReplicaMultimap<InetAddressAndPort, ReplicaSet> workMap = ReplicaMultimap.set();
        for (Replica toFetch : preferredEndpoints.keySet())
        {
            for (Replica source : preferredEndpoints.get(toFetch))
            {
                workMap.put(source.getEndpoint(), toFetch);
            }
        }
        return workMap;
    }

    private static ReplicaMultimap<InetAddressAndPort, ReplicaSet> getOptimizedRangeFetchMap(ReplicaMultimap<Replica, ReplicaList> rangesWithSources,
                                                                                             Collection<Predicate<Replica>> sourceFilters, String keyspace)
    {
        //The range fetch map calculator shouldn't really need to know anything about transient replication.
        //It just needs to know who the players are.
        //I think what we will end up doing is running the algorithm twice once for full ranges with only full replicas
        //and again with transient ranges with only transient replicas.
        //Or possibly punt and just do the other version with transient replicas for now.
        ReplicaMultimap<Range<Token>, ReplicaList> unwrapped = ReplicaMultimap.list();
        for (Map.Entry<Replica, Replica> entry : rangesWithSources.entries())
        {
            Replicas.checkFull(entry.getValue());
            unwrapped.put(entry.getKey().getRange(), entry.getValue());
        }

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(unwrapped, sourceFilters, keyspace);
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap = calculator.getRangeFetchMap();
        logger.info("Output from RangeFetchMapCalculator for keyspace {}", keyspace);
        validateRangeFetchMap(unwrapped, rangeFetchMapMap, keyspace);

        ReplicaMultimap<InetAddressAndPort, ReplicaSet> wrapped = ReplicaMultimap.set();
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : rangeFetchMapMap.entries())
        {
            Replica toFetch = null;
            for (Replica r : rangesWithSources.keySet())
            {
                if (r.getRange().equals(entry.getValue()))
                {
                    if (toFetch != null)
                        throw new AssertionError(String.format("There shouldn't be multiple replicas for range %s, replica %s and %s here", r.getRange(), r, toFetch));
                    toFetch = r;
                }
            }
            if (toFetch == null)
                throw new AssertionError("Shouldn't be possible for the Replica we fetch to be null here");
            wrapped.put(entry.getKey(), toFetch);
        }

        return wrapped;
    }

    /**
     * Verify that source returned for each range is correct
     * @param rangesWithSources
     * @param rangeFetchMapMap
     * @param keyspace
     */
    private static void validateRangeFetchMap(ReplicaMultimap<Range<Token>, ReplicaList> rangesWithSources, Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap, String keyspace)
    {
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : rangeFetchMapMap.entries())
        {
            if(entry.getKey().equals(FBUtilities.getBroadcastAddressAndPort()))
            {
                throw new IllegalStateException("Trying to stream locally. Range: " + entry.getValue()
                                        + " in keyspace " + keyspace);
            }

            if (!rangesWithSources.get(entry.getValue()).containsEndpoint(entry.getKey()))
            {
                throw new IllegalStateException("Trying to stream from wrong endpoint. Range: " + entry.getValue()
                                                + " in keyspace " + keyspace + " from endpoint: " + entry.getKey());
            }

            logger.info("Streaming range {} from endpoint {} for keyspace {}", entry.getValue(), entry.getKey(), keyspace);
        }
    }

    // For testing purposes
    @VisibleForTesting
    Multimap<String, Map.Entry<InetAddressAndPort, ReplicaSet>> toFetch()
    {
        return toFetch;
    }

    public StreamResultFuture fetchAsync()
    {
        for (Map.Entry<String, Map.Entry<InetAddressAndPort, ReplicaSet>> entry : toFetch.entries())
        {
            String keyspace = entry.getKey();
            InetAddressAndPort source = entry.getValue().getKey();
            Collection<Range<Token>> ranges = entry.getValue().getValue().asRangeSet();

            // filter out already streamed ranges
            Set<Range<Token>> availableRanges = stateStore.getAvailableRanges(keyspace, StorageService.instance.getTokenMetadata().partitioner);
            if (ranges.removeAll(availableRanges))
            {
                logger.info("Some ranges of {} are already available. Skipping streaming those ranges.", availableRanges);
            }

            if (logger.isTraceEnabled())
                logger.trace("{}ing from {} ranges {}", description, source, StringUtils.join(ranges, ", "));
            /* Send messages to respective folks to stream data over to me */
            streamPlan.requestRanges(source, keyspace, entry.getValue().getValue());
        }

        return streamPlan.execute();
    }
}
