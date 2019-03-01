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

import java.util.List;

import com.datastax.driver.core.querybuilder.Clause;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

public enum Sign
{
    LT
    {
        @Override
        public Clause getClause(String name, Object obj)
        {
            return lt(name, obj);
        }

        public Clause getClause(List<String> name, List<Object> obj)
        {
            return lt(name, obj);
        }

        public boolean isNegatable()
        {
            return true;
        }

        public Sign negate()
        {
            return GT;
        }

        public String toString()
        {
            return "<";
        }
    },
    GT
    {
        @Override
        public Clause getClause(String name, Object obj)
        {
            return gt(name, obj);
        }

        public Clause getClause(List<String> name, List<Object> obj)
        {
            return gt(name, obj);
        }

        public boolean isNegatable()
        {
            return true;
        }

        public Sign negate()
        {
            return LT;
        }

        public String toString()
        {
            return ">";
        }
    },
    LTE
    {
        @Override
        public Clause getClause(String name, Object obj)
        {
            return lte(name, obj);
        }

        public Clause getClause(List<String> name, List<Object> obj)
        {
            return lt(name, obj);
        }

        public boolean isNegatable()
        {
            return true;
        }

        public Sign negate()
        {
            return GTE;
        }

        public String toString()
        {
            return ">=";
        }
    },
    GTE
    {
        @Override
        public Clause getClause(String name, Object obj)
        {
            return gte(name, obj);
        }

        public Clause getClause(List<String> name, List<Object> obj)
        {
            return gte(name, obj);
        }

        public boolean isNegatable()
        {
            return true;
        }

        public Sign negate()
        {
            return LTE;
        }

        public String toString()
        {
            return "<=";
        }
    },
    EQ
    {
        @Override
        public Clause getClause(String name, Object obj)
        {
            return eq(name, obj);
        }

        public Clause getClause(List<String> name, List<Object> obj)
        {
            return eq(name, obj);
        }

        public boolean isNegatable()
        {
            return false;
        }

        public Sign negate()
        {
            throw new IllegalArgumentException("Cannot negate EQ");
        }

        public String toString()
        {
            return "=";
        }
    };

    public abstract Clause getClause(String name, Object obj);
    public abstract Clause getClause(List<String> name, List<Object> obj);
    public abstract boolean isNegatable();
    public abstract Sign negate();
}
