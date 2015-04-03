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
import java.util.concurrent.atomic.AtomicLong;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;

/**
 * Logging that limits each log statement to firing based on time since the statement last fired.
 *
 * Every logger has a unique timer per statement. Minimum time between logging is set for each statement
 * the first time it is used and a subsequent attempt to request that statement with a different minimum time will
 * result in the original time being used. No warning is provided if there is a mismatch.
 *
 * If the statement is cached and used to log directly then only a volatile read will be required in the common case.
 * If the Logger is cached then there is a single concurrent hash map lookup + the volatile read.
 * If neither the logger nor the statement is cached then it is two concurrent hash map lookups + the volatile read.
 *
 */
public class NoSpamLogger
{
    /**
     * Levels for programmatically specifying the severity of a log statement
     */
    public enum Level
    {
        INFO, WARN, ERROR;
    }

    @VisibleForTesting
    static interface Clock
    {
        long nanoTime();
    }

    @VisibleForTesting
    static Clock CLOCK = new Clock()
    {
        public long nanoTime()
        {
            return System.nanoTime();
        }
    };

    public class NoSpamLogStatement extends AtomicLong
    {
        private static final long serialVersionUID = 1L;

        private final String statement;
        private final long minIntervalNanos;

        public NoSpamLogStatement(String statement, long minIntervalNanos)
        {
            this.statement = statement;
            this.minIntervalNanos = minIntervalNanos;
        }

        private boolean shouldLog(long nowNanos)
        {
            long expected = get();
            return nowNanos - expected >= minIntervalNanos && compareAndSet(expected, nowNanos);
        }

        public void log(Level l, long nowNanos, Object... objects)
        {
            if (!shouldLog(nowNanos)) return;

            switch (l)
            {
            case INFO:
                wrapped.info(statement, objects);
                break;
            case WARN:
                wrapped.warn(statement, objects);
                break;
            case ERROR:
                wrapped.error(statement, objects);
                break;
                default:
                    throw new AssertionError();
            }
        }

        void info(long nowNanos, Object... objects)
        {
            log(Level.INFO, nowNanos, objects);
        }

        public void info(Object... objects)
        {
            info(CLOCK.nanoTime(), objects);
        }

        void warn(long nowNanos, Object... objects)
        {
            log(Level.WARN, nowNanos, objects);
        }

        public void warn(Object... objects)
        {
            warn(CLOCK.nanoTime(), objects);
        }

        void error(long nowNanos, Object... objects)
        {
            log(Level.ERROR, nowNanos, objects);
        }

        public void error(Object... objects)
        {
            error(CLOCK.nanoTime(), objects);
        }
    }

    private static final NonBlockingHashMap<Logger, NoSpamLogger> wrappedLoggers = new NonBlockingHashMap<>();

    @VisibleForTesting
    static void clearWrappedLoggersForTest()
    {
        wrappedLoggers.clear();
    }

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
        log(logger, level, minInterval, unit, CLOCK.nanoTime(), message, objects);
    }

    static void log(Logger logger, Level level, long minInterval, TimeUnit unit, long nowNanos, String message, Object... objects)
    {
        NoSpamLogger wrapped = getLogger(logger, minInterval, unit);
        NoSpamLogStatement statement = wrapped.getStatement(message);
        statement.log(level, nowNanos, objects);
    }

    public static NoSpamLogStatement getStatement(Logger logger, String message, long minInterval, TimeUnit unit) {
        NoSpamLogger wrapped = getLogger(logger, minInterval, unit);
        return wrapped.getStatement(message);
    }

    private final Logger wrapped;
    private final long minIntervalNanos;
    private final NonBlockingHashMap<String, NoSpamLogStatement> lastMessage = new NonBlockingHashMap<>();

    private NoSpamLogger(Logger wrapped, long minInterval, TimeUnit timeUnit)
    {
        this.wrapped = wrapped;
        minIntervalNanos = timeUnit.toNanos(minInterval);
    }

    void info(long nowNanos, String s, Object... objects)
    {
        log( Level.INFO, s, nowNanos, objects);
    }

    public void info(String s, Object... objects)
    {
        info(CLOCK.nanoTime(), s, objects);
    }

    void warn(long nowNanos, String s, Object... objects)
    {
        log( Level.WARN, s, nowNanos, objects);
    }

    public void warn(String s, Object... objects)
    {
        warn(CLOCK.nanoTime(), s, objects);
    }

    void error(long nowNanos, String s, Object... objects)
    {
        log( Level.ERROR, s, nowNanos, objects);
    }

    public void error(String s, Object... objects)
    {
        error(CLOCK.nanoTime(), s, objects);
    }

    public void log(Level l, String s, long nowNanos, Object... objects) {
        getStatement(s, minIntervalNanos).log(l, nowNanos, objects);
    }

    public NoSpamLogStatement getStatement(String s)
    {
        return getStatement(s, minIntervalNanos);
    }

    public NoSpamLogStatement getStatement(String s, long minInterval, TimeUnit unit) {
        return getStatement(s, unit.toNanos(minInterval));
    }

    public NoSpamLogStatement getStatement(String s, long minIntervalNanos)
    {
        NoSpamLogStatement statement = lastMessage.get(s);
        if (statement == null)
        {
            statement = new NoSpamLogStatement(s, minIntervalNanos);
            NoSpamLogStatement temp = lastMessage.putIfAbsent(s, statement);
            if (temp != null)
                statement = temp;
        }
        return statement;
    }
}
