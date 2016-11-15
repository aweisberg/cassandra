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

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import org.apache.cassandra.config.Config;

/**
 * A class to replace the usage of InetAddress to identify hosts in the cluster.
 * Opting for a full replacement class so that in the future if we change the nature
 * of the identifier the refactor will be easier in that we don't have to change the type
 * just the methods.
 *
 * Because an IP might contain multiple C* instances the identification must be done
 * using the IP + ports. Both regular and SSL ports are used for comparison and matching
 * to avoid ambiguity in mixed SSL non-ssl situations and transitions that may occur.
 */
public final class InetAddressAndPorts implements Comparable<InetAddressAndPorts>, Serializable
{
    private static final long serialVersionUID = 0;
    private static final Pattern splitRightBracket = Pattern.compile("]");
    private static final Pattern splitColon = Pattern.compile(":");
    private static final boolean allowSinglePort = Boolean.getBoolean(Config.PROPERTY_PREFIX + ".InetAddressAndPorts.allowSinglePort");

    //Store these here to avoid requiring DatabaseDescriptor to be loaded. DatabaseDescriptor will set
    //these when it loads the config. A lot of unit tests won't end up loading DatabaseDescriptor.
    //Tools that might use this class also might not load database descriptor. Those tools are expected
    //to always override the defaults.
    static volatile int defaultPort = 7000;
    static volatile int defaultSSLPort = 7001;

    public final InetAddress address;
    public final int port;
    public final int sslport;

    private InetAddressAndPorts(InetAddress address, int port, int sslport)
    {
        Preconditions.checkNotNull(address);
        validatePortRange(port);
        validatePortRange(sslport);
        this.address = address;
        this.port = port;
        this.sslport = sslport;
    }

    private static void validatePortRange(int port)
    {
        if (port < 0 | port > 65535)
        {
            throw new IllegalArgumentException("Port " + port + " is not a valid port number in the range 0-65535");
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InetAddressAndPorts that = (InetAddressAndPorts) o;

        if (port != that.port) return false;
        if (sslport != that.sslport) return false;
        return address.equals(that.address);
    }

    @Override
    public int hashCode()
    {
        int result = address.hashCode();
        result = 31 * result + port;
        result = 31 * result + sslport;
        return result;
    }

    @Override
    public int compareTo(InetAddressAndPorts o)
    {
        int retval = ByteBuffer.wrap(address.getAddress()).compareTo(ByteBuffer.wrap(o.address.getAddress()));
        if (retval != 0)
        {
            return retval;
        }

        retval = Integer.compare(port, o.port);
        if (retval != 0)
        {
            return retval;
        }

        return Integer.compare(sslport, o.sslport);
    }

    public String getHostAddress(boolean withPorts)
    {
        if (withPorts)
        {
            return toString();
        }
        else
        {
            return address.getHostAddress();
        }
    }

    @Override
    public String toString()
    {
        return toString(true);
    }

    public String toString(boolean withPorts)
    {
        if (withPorts)
        {
            if (address instanceof Inet6Address)
            {
                return "[" + address.getHostAddress() + "]:" + port + ":" + sslport;
            }
            return address.getHostAddress() + ":" + port + ":" + sslport;
        }
        else
        {
            return address.toString();
        }
    }

    public static InetAddressAndPorts getByName(String name) throws UnknownHostException
    {
        return getByNameOverrideDefaults(name, null, null);
    }

    /**
     *
     * @param name Hostname + optional ports string
     * @param port Unsecured port to connect on, nullity must match sslport, overridden by values in hostname string, defaults to DatabaseDescriptor default if not specified anywhere.
     * @param sslport Secured port to connect on, nullity must match port, overridden by values in hostname string, defaults to DatabaseDescriptor default if not specified anywhere.
     * @return
     * @throws UnknownHostException
     */
    public static InetAddressAndPorts getByNameOverrideDefaults(String name, Integer port, Integer sslport) throws UnknownHostException
    {
        Preconditions.checkArgument((port == null & sslport == null) | (port != null & sslport != null), "Either both ports must be set or both must be unset, had " + port + " and " + sslport);
        String ports = null;
        String nameOriginal = name;

        //Bracketed IPv6, required for a port number
        if (name.startsWith("["))
        {
            String[] parts = splitRightBracket.split(name);
            //Remove leading bracket
            name = parts[0].substring(1, parts[0].length());
            //Grab ports if any
            if (parts.length > 1)
            {
                ports = parts[1].substring(1, parts[1].length());
            }
        }
        else
        {
            //Not bracketed, but still maybe IPv6
            int colonCount = 0;
            for (int ii = 0; ii < name.length(); ii++)
            {
                if (name.charAt(ii) == ':')
                {
                    colonCount++;
                }
            }

            //If there are 7 colon it's an unbracketed IPv6
            //without ports. If it had ports it MUST be bracketed.
            //If it's not let InetAddress choke on it.
            int index = name.indexOf(":");
            if (colonCount != 7 && index != -1)
            {
                ports = name.substring(index + 1, name.length());
                name = name.substring(0, index);
            }
        }

        if (ports != null)
        {
            String parts[] = splitColon.split(ports);
            //In the future we might start allowing a single port
            if (parts.length != 2 & !allowSinglePort)
            {
                throw new IllegalArgumentException("Components must have an address followed by two ports " +
                                                   "(regular, SSL) separated by \":\". IPv6 MUST be bracketed. Had \"" + nameOriginal + "\"");
            }
            port = Integer.parseInt(parts[0]);
            if (parts.length > 1)
            {
                sslport = Integer.parseInt(parts[1]);
            }
            else
            {
                sslport = port;
            }
        }

        return getByAddressOverrideDefaults(InetAddress.getByName(name), port, sslport);
    }

    public static InetAddressAndPorts getByAddress(byte[] address) throws UnknownHostException
    {
        return getByAddressOverrideDefaults(InetAddress.getByAddress(address), null, null);
    }

    public static InetAddressAndPorts getByAddress(InetAddress address)
    {
        return getByAddressOverrideDefaults(address, null, null);
    }

    public static InetAddressAndPorts getByAddressOverrideDefaults(InetAddress address, Integer port, Integer sslport)
    {
        Preconditions.checkArgument((port == null & sslport == null) | (port != null & sslport != null), "Either both ports must be set or both must be unset, had " + port + " and " + sslport);

        if (port == null)
        {
            port = defaultPort;
            sslport = defaultSSLPort;
        }

        return new InetAddressAndPorts(address, port, sslport);
    }

    public static InetAddressAndPorts getLoopbackAddress()
    {
        return InetAddressAndPorts.getByAddress(InetAddress.getLoopbackAddress());
    }

    public static InetAddressAndPorts getLocalHost() throws UnknownHostException
    {
        return InetAddressAndPorts.getByAddress(InetAddress.getLocalHost());
    }

    public static void initializeDefaultPorts(int port, int sslport)
    {
        defaultPort = port;
        defaultSSLPort = sslport;
    }
}
