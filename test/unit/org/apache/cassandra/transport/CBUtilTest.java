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

package org.apache.cassandra.transport;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.cassandra.locator.InetAddressAndPorts;

import static org.junit.Assert.assertEquals;

public class CBUtilTest
{

    @Before
    public void before()
    {
        CBUtil.disableV5 = false;
    }

    @After
    public void after()
    {
        CBUtil.disableV5 = true;
    }

    @Test
    public void testInetAddressAndPortsRoundtrip() throws Exception
    {
        InetAddressAndPorts ipv4 = InetAddressAndPorts.getByName("127.0.0.1:42:42");
        InetAddressAndPorts ipv6 = InetAddressAndPorts.getByName("[2001:db8:0:0:0:ff00:42:8329]:42:43");

        testAddress(ipv4, ProtocolVersion.V4);
        testAddress(ipv6, ProtocolVersion.V4);
        testAddress(ipv4, ProtocolVersion.V5);
        testAddress(ipv6, ProtocolVersion.V5);
    }

    private void testAddress(InetAddressAndPorts address, ProtocolVersion version) throws Exception
    {
        ByteBuf out = UnpooledByteBufAllocator.DEFAULT.heapBuffer(1024);
        CBUtil.writeInetAddressAndPorts(address, out, version);
        assertEquals(out.writerIndex(), CBUtil.sizeOfInetAddressAndPorts(address, version));
        InetAddressAndPorts roundtripped = CBUtil.readInetAddressAndPorts(out, version);

        if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
        {
            assertEquals(address, roundtripped);
        }
        else
        {
            assertEquals(roundtripped.address, address.address);
            assertEquals(42, roundtripped.port);
            assertEquals(42, roundtripped.sslport);
        }
    }
}
