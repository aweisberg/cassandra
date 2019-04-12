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

package org.apache.cassandra.quicktheories;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.cassandra.quicktheories.tests.DistributedQTTest;
import org.quicktheories.core.Configuration;
import org.quicktheories.core.DetatchedRandomnessSource;
import org.quicktheories.core.Gen;
import org.quicktheories.core.PseudoRandom;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.Constraint;
import org.quicktheories.impl.stateful.StatefulTheory;

public class QuickTheories
{
    // We do not use QT-provided randomness source, since we do not use shrinking and do not need precursors
    // especially in stateful
    public static class DelegatingDetatchedRandomnessSource implements DetatchedRandomnessSource
    {

        private RandomnessSource delegate;

        private DelegatingDetatchedRandomnessSource(RandomnessSource delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public long next(Constraint constraints)
        {
            return delegate.next(constraints);
        }

        @Override
        public DetatchedRandomnessSource detach()
        {
            return delegate.detach();
        }

        @Override
        public void registerFailedAssumption()
        {
            delegate.registerFailedAssumption();
        }

        @Override
        public void commit()
        {
            // no-op
        }
    }

    private static class SimpleRandomnessSource implements RandomnessSource
    {
        private final PseudoRandom random;

        public SimpleRandomnessSource()
        {
            this(Configuration.defaultPRNG(System.nanoTime()));
        }

        public SimpleRandomnessSource(PseudoRandom random)
        {
            this.random = random;
        }


        @Override
        public long next(Constraint constraint)
        {
            return random.nextLong(constraint.min, constraint.max);
        }

        @Override
        public DetatchedRandomnessSource detach()
        {
            return new DelegatingDetatchedRandomnessSource(this);
        }

        @Override
        public void registerFailedAssumption()
        {
            // do nothing as we do not care
        }
    }

    public static void stateful(Supplier<StatefulTheory<StatefulTheory.Step>> stateful)
    {
        StatefulCore<StatefulTheory.Step> runner = new StatefulCore<>(new SimpleRandomnessSource(), stateful);
        runner.run(() -> Configuration.profileStrategy(Configuration.ensureLoaded(DistributedQTTest.class), "ci"));
    }

    public static <T> void stateless(Gen<T> gen, StatelessCore.StatelessModel<T> stateless)
    {
        StatelessCore<T> runner = new StatelessCore<T>(new SimpleRandomnessSource(), gen, stateless);
        runner.run(() -> Configuration.profileStrategy(Configuration.ensureLoaded(DistributedQTTest.class), "ci"));
    }

    public static <T> void stateless(Gen<T> gen, Consumer<T> stateless)
    {
        StatelessCore<T> runner = new StatelessCore<T>(new SimpleRandomnessSource(), gen,
                                                       new StatelessCore.StatelessModel<T>()
                                                       {
                                                           void check(T v)
                                                           {
                                                               stateless.accept(v);
                                                           }
                                                       });
        runner.run(() -> Configuration.profileStrategy(Configuration.ensureLoaded(DistributedQTTest.class), "ci")
                                      .withExamples(1)
                                      .withMinStatefulSteps(100)
                                      .withMaxStatefulSteps(500));
    }
}
