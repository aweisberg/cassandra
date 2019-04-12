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


import java.util.Iterator;
import java.util.function.Supplier;

import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.core.Strategy;
import org.quicktheories.generators.Generate;
import org.quicktheories.impl.stateful.StatefulTheory;

public class StatefulCore<T> {

    private final RandomnessSource prng;
    private final Supplier<StatefulTheory<T>> theorySupplier;

    public StatefulCore(RandomnessSource prng, Supplier<StatefulTheory<T>> theorySupplier) {
        this.prng = prng;
        this.theorySupplier = theorySupplier;
    }

    public void run(Supplier<Strategy> strategySupplier) {
        int examples = strategySupplier.get().examples();
        for (int i = 0; i < examples; i++)
            runOne(strategySupplier);
    }

    public void runOne(Supplier<Strategy> strategySupplier) {
        Strategy strategy = strategySupplier.get();
        StatefulTheory<T> theory = theorySupplier.get();

        theory.init();
        Iterator<Gen<T>> setupSteps = theory.setupSteps();
        while (setupSteps.hasNext()) {
            theory.executeStep(setupSteps.next().generate(prng));
        }


        int numSteps = Generate.longRange(strategy.minStatefulSteps(), strategy.maxStatefulSteps(), strategy.minStatefulSteps())
                               .map(Long::intValue)
                               .generate(prng);
        try {
            Gen<T> stepGen = theory.steps();
            for (int i = 0; i < numSteps; i++) {
                theory.executeStep(stepGen.generate(prng));
            }
        } finally {
            theory.teardown();
        }
    }
}
