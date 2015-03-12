/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.config;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftConversion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(OrderedJUnit4ClassRunner.class)
public class DatabaseDescriptorTest
{
    @Test
    public void testCFMetaDataSerialization() throws ConfigurationException, InvalidRequestException
    {
        // test serialization of all defined test CFs.
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            for (CFMetaData cfm : Schema.instance.getKeyspaceMetaData(keyspaceName).values())
            {
                CFMetaData cfmDupe = ThriftConversion.fromThrift(ThriftConversion.toThrift(cfm));
                assertNotNull(cfmDupe);
                assertEquals(cfm, cfmDupe);
            }
        }
    }

    @Test
    public void testKSMetaDataSerialization() throws ConfigurationException
    {
        for (String ks : Schema.instance.getNonSystemKeyspaces())
        {
            // Not testing round-trip on the KsDef via serDe() because maps
            KSMetaData ksm = Schema.instance.getKSMetaData(ks);
            KSMetaData ksmDupe = ThriftConversion.fromThrift(ThriftConversion.toThrift(ksm));
            assertNotNull(ksmDupe);
            assertEquals(ksm, ksmDupe);
        }
    }

    // this came as a result of CASSANDRA-995
    @Test
    public void testTransKsMigration() throws ConfigurationException
    {
        SchemaLoader.cleanupAndLeaveDirs();
        Schema.instance.loadFromDisk();
        assertEquals(0, Schema.instance.getNonSystemKeyspaces().size());

        Gossiper.instance.start((int)(System.currentTimeMillis() / 1000));
        Keyspace.setInitialized();

        try
        {
            // add a few.
            MigrationManager.announceNewKeyspace(KSMetaData.testMetadata("ks0", SimpleStrategy.class, KSMetaData.optsWithRF(3)));
            MigrationManager.announceNewKeyspace(KSMetaData.testMetadata("ks1", SimpleStrategy.class, KSMetaData.optsWithRF(3)));

            assertNotNull(Schema.instance.getKSMetaData("ks0"));
            assertNotNull(Schema.instance.getKSMetaData("ks1"));

            Schema.instance.clearKeyspaceDefinition(Schema.instance.getKSMetaData("ks0"));
            Schema.instance.clearKeyspaceDefinition(Schema.instance.getKSMetaData("ks1"));

            assertNull(Schema.instance.getKSMetaData("ks0"));
            assertNull(Schema.instance.getKSMetaData("ks1"));

            Schema.instance.loadFromDisk();

            assertNotNull(Schema.instance.getKSMetaData("ks0"));
            assertNotNull(Schema.instance.getKSMetaData("ks1"));
        }
        finally
        {
            Gossiper.instance.stop();
        }
    }

    @Test
    public void testConfigurationLoader() throws Exception
    {
        // By default, we should load from the yaml
        Config config = DatabaseDescriptor.loadConfig();
        assertEquals("Test Cluster", config.cluster_name);
        Keyspace.setInitialized();

        // Now try custom loader
        ConfigurationLoader testLoader = new TestLoader();
        System.setProperty("cassandra.config.loader", testLoader.getClass().getName());

        config = DatabaseDescriptor.loadConfig();
        assertEquals("ConfigurationLoader Test", config.cluster_name);
    }

    public static class TestLoader implements ConfigurationLoader
    {
        public Config loadConfig() throws ConfigurationException
        {
            Config testConfig = new Config();
            testConfig.cluster_name = "ConfigurationLoader Test";;
            return testConfig;
        }
    }

    static NetworkInterface oneAddressInterface = null;
    static NetworkInterface suitableInterface = null;

    /*
     * Server only accepts interfaces by name if they have a single address
     * OS X seems to always have an ipv4 and ipv6 address on all interfaces which means some tests fail
     * if not checked for and skipped
     */
    @BeforeClass
    public static void selectInterfaces() throws Exception {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while(interfaces.hasMoreElements()) {
            NetworkInterface intf = interfaces.nextElement();

            System.out.println("Evaluating " + intf.getName());
            Enumeration<InetAddress> addresses = intf.getInetAddresses();
            int count = 0;
            while (addresses.hasMoreElements()) {
                count++;
                System.out.println("\t" + addresses.nextElement().toString());
            }

            if (count == 1)
            {
                oneAddressInterface = intf;
                suitableInterface = intf;
                break;
            }

            if (intf.isLoopback()) {
                suitableInterface = intf;
            }
        }
    }

    /*
     * Skip testing interfaces configured by name on OS X because there are ipv4 and ipv6 addresses
     * and the tests would fail because it doesn't know which to pick.
     */
    static boolean maybeSkipOnOSX() throws Exception
    {
        if (oneAddressInterface == null)
            if (System.getProperty("os.name").equals("Mac OS X"))
                return true;
            else
                Assert.fail("Should have a one address interface for this test to pass");
        return false;
    }

    @Test
    public void testRpcInterface() throws Exception
    {
        if (maybeSkipOnOSX()) return;
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.rpc_interface = oneAddressInterface.getName();
        testConfig.rpc_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
    }

    @Test
    public void testListenInterface() throws Exception
    {
        if (maybeSkipOnOSX()) return;
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.listen_interface = oneAddressInterface.getName();
        testConfig.listen_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
    }

    @Test
    public void testListenAddress() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.listen_address = suitableInterface.getInterfaceAddresses().get(0).getAddress().getHostAddress();
        testConfig.listen_interface = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
    }

    @Test
    public void testRpcAddress() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.rpc_address = suitableInterface.getInterfaceAddresses().get(0).getAddress().getHostAddress();
        testConfig.rpc_interface = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);

    }
}
