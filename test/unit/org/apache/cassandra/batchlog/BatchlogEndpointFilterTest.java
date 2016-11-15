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
package org.apache.cassandra.batchlog;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;

import org.apache.cassandra.locator.InetAddressAndPorts;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BatchlogEndpointFilterTest
{
    private static final String LOCAL = "local";

    @Test
    public void shouldSelect2hostsFromNonLocalRacks() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPorts> endpoints = ImmutableMultimap.<String, InetAddressAndPorts> builder()
                .put(LOCAL, InetAddressAndPorts.getByName("0"))
                .put(LOCAL, InetAddressAndPorts.getByName("00"))
                .put("1", InetAddressAndPorts.getByName("1"))
                .put("1", InetAddressAndPorts.getByName("11"))
                .put("2", InetAddressAndPorts.getByName("2"))
                .put("2", InetAddressAndPorts.getByName("22"))
                .build();
        Collection<InetAddressAndPorts> result = new TestEndpointFilter(LOCAL, endpoints).filter();
        assertThat(result.size(), is(2));
        assertThat(result, JUnitMatchers.hasItem(InetAddressAndPorts.getByName("11")));
        assertThat(result, JUnitMatchers.hasItem(InetAddressAndPorts.getByName("22")));
    }

    @Test
    public void shouldSelectHostFromLocal() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPorts> endpoints = ImmutableMultimap.<String, InetAddressAndPorts> builder()
                .put(LOCAL, InetAddressAndPorts.getByName("0"))
                .put(LOCAL, InetAddressAndPorts.getByName("00"))
                .put("1", InetAddressAndPorts.getByName("1"))
                .build();
        Collection<InetAddressAndPorts> result = new TestEndpointFilter(LOCAL, endpoints).filter();
        assertThat(result.size(), is(2));
        assertThat(result, JUnitMatchers.hasItem(InetAddressAndPorts.getByName("1")));
        assertThat(result, JUnitMatchers.hasItem(InetAddressAndPorts.getByName("0")));
    }

    @Test
    public void shouldReturnAsIsIfNoEnoughEndpoints() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPorts> endpoints = ImmutableMultimap.<String, InetAddressAndPorts> builder()
                .put(LOCAL, InetAddressAndPorts.getByName("0"))
                .build();
        Collection<InetAddressAndPorts> result = new TestEndpointFilter(LOCAL, endpoints).filter();
        assertThat(result.size(), is(1));
        assertThat(result, JUnitMatchers.hasItem(InetAddressAndPorts.getByName("0")));
    }

    @Test
    public void shouldSelectTwoRandomHostsFromSingleOtherRack() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPorts> endpoints = ImmutableMultimap.<String, InetAddressAndPorts> builder()
                .put(LOCAL, InetAddressAndPorts.getByName("0"))
                .put(LOCAL, InetAddressAndPorts.getByName("00"))
                .put("1", InetAddressAndPorts.getByName("1"))
                .put("1", InetAddressAndPorts.getByName("11"))
                .put("1", InetAddressAndPorts.getByName("111"))
                .build();
        Collection<InetAddressAndPorts> result = new TestEndpointFilter(LOCAL, endpoints).filter();
        // result should contain random two distinct values
        assertThat(new HashSet<>(result).size(), is(2));
    }

    private static class TestEndpointFilter extends BatchlogManager.EndpointFilter
    {
        TestEndpointFilter(String localRack, Multimap<String, InetAddressAndPorts> endpoints)
        {
            super(localRack, endpoints);
        }

        @Override
        protected boolean isValid(InetAddressAndPorts input)
        {
            // We will use always alive non-localhost endpoints
            return true;
        }

        @Override
        protected int getRandomInt(int bound)
        {
            // We don't need random behavior here
            return bound - 1;
        }
    }
}
