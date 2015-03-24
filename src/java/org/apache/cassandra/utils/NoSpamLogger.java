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
package org.apache.cassandra.utils;

import java.util.concurrent.TimeUnit;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;

public class NoSpamLogger
{
    public enum Level
    {
        INFO, WARN, ERROR;
    }

    private static final NonBlockingHashMap<Logger, NoSpamLogger> wrappedLoggers = new NonBlockingHashMap<>();

    public static NoSpamLogger getLogger(Logger logger, long minInterval, TimeUnit unit)
    {
        NoSpamLogger wrapped = wrappedLoggers.get(logger);
        if (wrapped == null)
        {
            wrapped = new NoSpamLogger(logger, minInterval, unit);
            NoSpamLogger temp = wrappedLoggers.putIfAbsent(logger, wrapped);
            if (temp != null)
                wrapped = temp;
        }
        return wrapped;
    }

    public static void log(Logger logger, Level level, long minInterval, TimeUnit unit, String message, Object... objects)
    {
        log(logger, level, minInterval, unit, System.nanoTime(), message, objects);
    }

    public static void log(Logger logger, Level level, long minInterval, TimeUnit unit, long nowNanos, String message, Object... objects)
    {
        NoSpamLogger wrapped = getLogger(logger, minInterval, unit);

        switch (level)
        {
        case INFO:
            wrapped.info(nowNanos, message, objects);
            break;
        case WARN:
            wrapped.warn(nowNanos,  message, objects);
            break;
        case ERROR:
            wrapped.error(nowNanos,  message, objects);
            break;
            default:
                throw new AssertionError();
        }
    }

    private final Logger wrapped;
    private final long minIntervalNanos;
    private final NonBlockingHashMap<String, Long> lastMessage = new NonBlockingHashMap<>();

    private NoSpamLogger(Logger wrapped, long minInterval, TimeUnit timeUnit)
    {
        this.wrapped = wrapped;
        minIntervalNanos = timeUnit.toNanos(minInterval);
    }

    public void info(long nowNanos, String s, Object... objects)
    {
        if (log(s, nowNanos))
            wrapped.info(s, objects);
    }

    public void info(String s, Object... objects)
    {
        info(System.nanoTime(), s, objects);
    }

    public void warn(long nowNanos, String s, Object... objects)
    {
        if (log(s, nowNanos))
            wrapped.warn(s, objects);
    }

    public void warn(String s, Object... objects)
    {
        warn(System.nanoTime(), s, objects);
    }

    public void error(long nowNanos, String s, Object... objects)
    {
        if (log(s, nowNanos))
            wrapped.error(s, objects);
    }

    public void error(String s, Object... objects)
    {
        error(System.nanoTime(), s, objects);
    }

    private boolean log(String s, long nowNanos)
    {
        Long last = lastMessage.get(s);
        if (last == null)
        {
            last = nowNanos;
            return lastMessage.putIfAbsent(s, last) == null;
        }

        if (nowNanos - last >= minIntervalNanos)
        {
            return lastMessage.replace(s, last, nowNanos);
        }

        return false;
    }
}
