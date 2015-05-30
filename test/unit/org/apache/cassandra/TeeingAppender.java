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
package org.apache.cassandra;

import java.util.Iterator;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;

public class TeeingAppender<E> extends UnsynchronizedAppenderBase<E> implements AppenderAttachable<E>
{
    AppenderAttachableImpl<E> aai = new AppenderAttachableImpl<E>();

    @Override
    protected void append(E arg0)
    {
        aai.appendLoopOnAppenders(arg0);
    }

    @Override
    public void addAppender(Appender<E> arg0)
    {
        aai.addAppender(arg0);
    }

    @Override
    public void detachAndStopAllAppenders()
    {
        aai.detachAndStopAllAppenders();
    }

    @Override
    public boolean detachAppender(Appender<E> arg0)
    {
        return aai.detachAppender(arg0);
    }

    @Override
    public boolean detachAppender(String arg0)
    {
        return aai.detachAppender(arg0);
    }

    @Override
    public Appender<E> getAppender(String arg0)
    {
        return aai.getAppender(arg0);
    }

    @Override
    public boolean isAttached(Appender<E> arg0)
    {
        return aai.isAttached(arg0);
    }

    @Override
    public Iterator<Appender<E>> iteratorForAppenders()
    {
        return aai.iteratorForAppenders();
    }

}
