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
package org.apache.cassandra.utils;

import java.util.Collection;
import java.util.concurrent.Future;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public final class Throwables
{

    public static Throwable merge(Throwable existingFail, Throwable newFail)
    {
        if (existingFail == null)
            return newFail;
        existingFail.addSuppressed(newFail);
        return existingFail;
    }

    public static void maybeFail(Throwable fail)
    {
        if (fail != null)
            com.google.common.base.Throwables.propagate(fail);
    }

    public static Throwable close(Throwable accumulate, Iterable<? extends AutoCloseable> closeables)
    {
        for (AutoCloseable closeable : closeables)
        {
            try
            {
                closeable.close();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    public static  <Y, T extends Future<Y>> Iterable<Y> completedFuturesUnchecked(Collection<T> c)
    {
        Iterable<T> filter = Iterables.filter(c, new Predicate<T>() {

            @Override
            public boolean apply(T input)
            {
                return input.isDone();
            }});
        return Iterables.transform(filter, new Function<T, Y>(){

            @Override
            public Y apply(T input)
            {
                try
                {
                    return input.get();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }});
    }
}
