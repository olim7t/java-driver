/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.schemabuilder;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;

import com.google.common.base.Strings;

public abstract class SchemaStatement extends RegularStatement {

    static final String STATEMENT_START = "\n\t";
    static final String COLUMN_FORMATTING = "\n\t\t";

    static final List<String> RESERVED_KEYWORDS = Arrays.asList("add,allow,alter,and,any,apply,asc,authorize,batch,begin,by,columnfamily,create,delete,desc,drop,each_quorum,from,grant,in,index,inet,infinity,insert,into,keyspace,keyspaces,limit,local_one,local_quorum,modify,nan,norecursive,of,on,order,password,primary,quorum,rename,revoke,schema,select,set,table,three,to,token,truncate,two,unlogged,update,use,using,where,with".split(","));

    private volatile String cache;

    abstract String buildInternal();

    @Override
    public String getQueryString() {
        if (cache == null)
            cache = buildInternal();
        return cache;
    }

    @Override
    public ByteBuffer[] getValues(ProtocolVersion protocolVersion) {
        // DDL statements never have values
        return new ByteBuffer[0];
    }

    @Override
    public boolean hasValues() {
        return false;
    }

    @Override
    public String getKeyspace() {
        // This is exposed for token-aware routing. Since there is no token awareness for DDL statements, we don't need to
        // return anything here (even if a keyspace has been explicitly set in the statement).
        return null;
    }

    @Override
    public ByteBuffer getRoutingKey() {
        return null; // there is no token awareness for DDL statements
    }

    protected static void validateNotEmpty(String columnName, String label) {
        if (Strings.isNullOrEmpty(columnName)) {
            throw new IllegalArgumentException(label + " should not be null or blank");
        }
    }

    protected static void validateNotNull(Object value, String label) {
        if (value == null) {
            throw new IllegalArgumentException(label + " should not be null");
        }
    }

    protected static void validateNotKeyWord(String label, String message) {
        if (RESERVED_KEYWORDS.contains(label)) {
            throw new IllegalArgumentException(message);
        }
    }


}
