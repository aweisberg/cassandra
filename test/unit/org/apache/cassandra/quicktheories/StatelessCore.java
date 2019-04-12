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

import java.util.function.Supplier;

import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.core.Strategy;

public class StatelessCore<T> {

    private final RandomnessSource prng;
    private final Gen<T> gen;
    private final StatelessModel<T> model;

    public StatelessCore(RandomnessSource prng, Gen<T> gen, StatelessModel<T> check) {
        this.prng = prng;
        this.gen = gen;
        this.model = check;
    }

    public boolean run(Supplier<Strategy> state) {
        Strategy strategy = state.get();
        int numSteps = strategy.examples();
        try {
            model.init();
            for (int i = 0; i < numSteps; i++) {
                model.check(gen.generate(prng));
            }
        } finally {
            model.teardown();
        }

        return true;
    }

    public abstract static class StatelessModel<T>
    {
        void init() {}
        abstract void check(T check);
        void teardown() {}
    }
}
