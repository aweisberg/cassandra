/**
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

package org.apache.cassandra.test.microbench;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.*;
import org.openjdk.jmh.runner.options.*;
import org.openjdk.jmh.results.*;

import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 5,jvmArgsAppend = "-Xmx512M")
@State(Scope.Thread)
public class Sample
{
   @Benchmark
   public long nanoTime() {
       return System.nanoTime();
   }
   
   @Benchmark
   public long currentTimeMillis() {
       return System.currentTimeMillis();
   }
   
   public static void main(String... args) throws RunnerException {
       PrintWriter pw = new PrintWriter(System.out, true);
       for (int t = 1; t <= Runtime.getRuntime().availableProcessors(); t++) {
           runWith(pw, t);
       }
   }

   private static void runWith(PrintWriter pw, int threads) throws RunnerException {
       Options opts = new OptionsBuilder()
               .include(".*" + Sample.class.getName() + ".nanoTime")
               .threads(threads)
               .verbosity(VerboseMode.SILENT)
               .build();

       RunResult r = new Runner(opts).runSingle();
       double score = r.getPrimaryResult().getScore();
       double scoreError = r.getPrimaryResult().getStatistics().getMeanErrorAt(0.99);
       pw.printf("%3d, %11.3f, %10.3f%n", threads, score, scoreError);
   }
}
