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

package org.apache.cassandra.distributed.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
import org.apache.commons.collections.IteratorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;

import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.quicktheories.QuickTheory;
import org.quicktheories.core.Profile;

public class DistributedTestBase
{
    private final static String TEST_LOGBACK_CONFIG_PROPERTY = "org.apache.cassandra.test.logback.configurationFile";
    private final static String LOGBACK_CONFIG_PROPERTY = "logback.configurationFile";
//    private final static String DEFAULT_LOGBACK_CONFIG = "test/conf/logback-dtest.xml"; // -silent
    private final static String DEFAULT_LOGBACK_CONFIG = "test/conf/logback-dtest-silent.xml"; //

    @After
    public void afterEach()
    {
        System.runFinalization();
        System.gc();
    }

    public static String KEYSPACE = "distributed_test_keyspace";

    @BeforeClass
    public static void setup()
    {
        // Set default dtest config file
        if (System.getProperty(TEST_LOGBACK_CONFIG_PROPERTY) == null)
            System.setProperty(TEST_LOGBACK_CONFIG_PROPERTY, DEFAULT_LOGBACK_CONFIG);
        if (System.getProperty(LOGBACK_CONFIG_PROPERTY) == null)
            System.setProperty(LOGBACK_CONFIG_PROPERTY, DEFAULT_LOGBACK_CONFIG);

        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
        if (!Profile.getProfile(DistributedTestBase.class, "default").isPresent())
        {
            Profile.registerDefaultProfile(DistributedTestBase.class, s -> {
                // Defaults for local running
                return s.withExamples(50)
                        .withMinStatefulSteps(100)
                        .withMaxStatefulSteps(1000);
            });
        }
    }

    protected static QuickTheory qt()
    {
        return QuickTheory.qt().withProfile(DistributedTestBase.class, "ci");
    }

    protected static <C extends AbstractCluster<?>> C init(C cluster)
    {
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + cluster.size() + "};");
        return cluster;
    }

    public static void assertRows(Object[][] actual, Object[]... expected)
    {
        Assert.assertEquals(rowsNotEqualErrorMessage(expected, actual),
                            expected.length, actual.length);

        for (int i = 0; i < expected.length; i++)
        {
            Object[] expectedRow = expected[i];
            Object[] actualRow = actual[i];
            Assert.assertTrue(rowsNotEqualErrorMessage(actual, expected),
                              Arrays.equals(expectedRow, actualRow));
        }
    }

    public static void assertRow(Object[] actual, Object... expected)
    {
        Assert.assertTrue(rowNotEqualErrorMessage(actual, expected),
                          Arrays.equals(actual, expected));
    }

    // Iterator that preserves values
    private static class SavingIterator<T> implements Iterator<T>
    {
        private final Iterator<T> delegate;
        private final List<T> results;

        public SavingIterator(Iterator<T> delegate)
        {
            this.delegate = delegate;
            this.results = new ArrayList<>();
        }
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        public T next()
        {
            T next = delegate.next();
            this.results.add(next);
            return next;
        }

        public List<T> results()
        {
            return results;
        }
    }

    private static <T> Iterator<T> forceIterator(Iterator<T> iter)
    {
        while (iter.hasNext())
            iter.next();
        return iter;
    }

    public static void assertRows(Iterator<Object[]> actualOrig, Iterator<Object[]> expectedOrig)
    {
        SavingIterator<Object[]> actual = new SavingIterator<>(actualOrig);
        SavingIterator<Object[]> expected = new SavingIterator<>(expectedOrig);

        boolean mismatch = false;
        while (actual.hasNext() && expected.hasNext())
        {
            if (!Arrays.equals(actual.next(), expected.next()))
            {
                mismatch = true;
                break;
            }
        }

        boolean l = actual.hasNext();
        boolean r = expected.hasNext();
        forceIterator(actual);
        forceIterator(expected);

        if (mismatch || l != r)
        {
            Assert.fail(String.format("Results are not equal:\nExpected: %s\nActual:   %s",
                                      rowsToString(expected.results),
                                      rowsToString(actual.results)));
        }
    }

    public static void assertRows(Iterator<Object[]> actual, Object[]... expected)
    {
        assertRows(actual, Iterators.forArray(expected));
    }

    public static String rowNotEqualErrorMessage(Object[] actual, Object[] expected)
    {
        return String.format("Expected: %s\nActual:%s\n",
                             Arrays.toString(expected),
                             Arrays.toString(actual));
    }

    public static String rowsNotEqualErrorMessage(Object[][] actual, Object[][] expected)
    {
        return String.format("Expected: %s\nActual: %s\n",
                             rowsToString(expected),
                             rowsToString(actual));
    }

    public static String rowsToString(Object[][] rows)
    {
        return rowsToString(Arrays.asList(rows));
    }

    public static String rowsToString(Iterator<Object[]> rows)
    {
        return rowsToString(IteratorUtils.toList(rows));
    }

    public static String rowsToString(Iterable<Object[]> rows)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        boolean isFirst = true;
        for (Object[] row : rows)
        {
            if (isFirst)
                isFirst = false;
            else
                builder.append(",");
            builder.append("[")
                   .append(Arrays.stream(row).map(v -> {
                       String s;
                       if (v instanceof ByteBuffer)
                           s = "hex:\"" + ByteBufferUtil.bytesToHex((ByteBuffer) v) + "...\"";
                       else
                           s = String.valueOf(v);

                       s = s.substring(0, Math.min(100, s.length()));
                       return s;
                   }).collect(Collectors.joining(",")))
                   .append("]");
        }
        builder.append("]");
        return builder.toString();
    }

    public static Object[][] toObjectArray(Iterator<Object[]> iter)
    {
        List<Object[]> res = new ArrayList<>();
        while (iter.hasNext())
            res.add(iter.next());

        return res.toArray(new Object[res.size()][]);
    }

    public static Object[] row(Object... expected)
    {
        return expected;
    }
}
