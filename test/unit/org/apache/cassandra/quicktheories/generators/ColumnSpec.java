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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.quicktheories.core.Gen;

public class ColumnSpec<T>
{
    public final String name;
    public final AbstractType<T> type;
    public final Gen<T> generator;
    public final ColumnMetadata.Kind kind;

    public ColumnSpec(String name,
                      AbstractType<T> type,
                      ColumnMetadata.Kind kind)
    {
        this.name = name;
        this.type = type;
        this.generator = (Gen<T>) SchemaDSL.types.get(type.isReversed() ? ((ReversedType)type).baseType : type);
        this.kind = kind;
    }

    public String toCQL()
    {
        return String.format("%s %s%s",
                             name,
                             type.asCQL3Type().toString(),
                             kind == ColumnMetadata.Kind.STATIC ? " static" : "");
    }

    public String name()
    {
        return name;
    }

    public boolean isReversed()
    {
        return type.isReversed();
    }
    public String toString()
    {
        return name + '(' + type.asCQL3Type().toString() + ")";
    }
}
