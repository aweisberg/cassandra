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

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * A simple endpoint snitch implementation that treats Strategy order as proximity,
 * allowing non-read-repaired reads to prefer a single endpoint, which improves
 * cache locality.
 */
public class SimpleSnitch extends AbstractEndpointSnitch
{
    private final boolean throwOnMissingDC;
    private final Map<InetAddressAndPort, String> endpointToDC;

    public SimpleSnitch()
    {
        this(ImmutableMap.of(), false);
    }

    public SimpleSnitch(Map<InetAddressAndPort, String> endpointToDC, boolean throwOnMissingDC)
    {
        this.throwOnMissingDC = throwOnMissingDC;
        this.endpointToDC = endpointToDC;
    }

    public String getRack(InetAddressAndPort endpoint)
    {
        return "rack1";
    }

    public String getDatacenter(InetAddressAndPort endpoint)
    {
        if (endpointToDC == null)
            return "datacenter1";
        String dc = endpointToDC.get(endpoint);
        if (dc == null && throwOnMissingDC)
            throw new RuntimeException("Couldn't find DC for endpoint " + endpoint);
        return dc != null ? dc : "datacenter1";
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(final InetAddressAndPort address, C unsortedAddress)
    {
        // Optimization to avoid walking the list
        return unsortedAddress;
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        // Making all endpoints equal ensures we won't change the original ordering (since
        // Collections.sort is guaranteed to be stable)
        return 0;
    }
}
