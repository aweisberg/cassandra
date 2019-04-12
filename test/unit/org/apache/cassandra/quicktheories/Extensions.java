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

package org.apache.cassandra.quicktheories.generators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;
import org.quicktheories.impl.stateful.StatefulTheory;

import static org.quicktheories.generators.SourceDSL.*;

public class Extensions
{
    private static class MonotonicGen implements Gen<Integer>
    {
        private int i;

        public Integer generate(RandomnessSource randomnessSource)
        {
            return i++;
        }
    }

    public static Gen<Integer> monotonicGen()
    {
        return new MonotonicGen();
    }


    public interface TriFunction<T1, T2, T3, RES>
    {
        RES apply(T1 t1, T2 t2, T3 t3);
    }

    public interface QuatFunction<T1, T2, T3, T4, RES>
    {
        RES apply(T1 t1, T2 t2, T3 t3, T4 t4);
    }

    public interface QuinFunction<T1, T2, T3, T4, T5, RES>
    {
        RES apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5);
    }

    public interface SechFunction<T1, T2, T3, T4, T5, T6, RES>
    {
        RES apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6);
    }

    public static <T1, END> Gen<END> combine(Gen<T1> gen1, BiFunction<RandomnessSource, Gen<T1>, END> combine)
    {
        return prng -> combine.apply(prng, gen1);
    }

    public static <T1, T2, END> Gen<END> combine(Gen<T1> gen1, Gen<T2> gen2,
                                                 TriFunction<RandomnessSource, Gen<T1>, Gen<T2>, END> combine)
    {
        return prng -> combine.apply(prng, gen1, gen2);
    }

    public static <T1, T2, T3, END> Gen<END> combine(Gen<T1> gen1, Gen<T2> gen2, Gen<T3> gen3,
                                                     QuatFunction<RandomnessSource, Gen<T1>, Gen<T2>, Gen<T3>, END> combine)
    {
        return prng -> combine.apply(prng, gen1, gen2, gen3);
    }

    public static <T1, T2, T3, T4, END> Gen<END> combine(Gen<T1> gen1, Gen<T2> gen2, Gen<T3> gen3, Gen<T4> gen4,
                                                         QuinFunction<RandomnessSource, Gen<T1>, Gen<T2>, Gen<T3>, Gen<T4>, END> combine)
    {
        return prng -> combine.apply(prng, gen1, gen2, gen3, gen4);
    }

    public static <T> Supplier<T> toSupplier(Gen<T> gen, RandomnessSource prng)
    {
        return () -> gen.generate(prng);
    }

    public interface DynamicGen<G> extends Gen<G>
    {
        public default G generate(RandomnessSource in)
        {
            return getGen().generate(in);
        }

        public Gen<G> getGen();
    }

    public static <T> Gen<List<T>> subsetGenerator(List<T> list)
    {
        return subsetGenerator(list, 0, list.size() - 1);
    }

    public static <T> Gen<List<T>> subsetGenerator(List<T> list, int min, int max)
    {
        return lists()
               .of(integers().between(0, list.size() - 1)) // Random ID generator
               .ofSizeBetween(min, max)
               .map(ids -> {
                   Set<T> set = new HashSet<>();
                   for (Integer id : ids)
                       set.add(list.get(id));
                   return (List<T>) new ArrayList<>(set);
               });
//               .assuming((l) -> l.size() > min); // Since it's a set
    }

}
