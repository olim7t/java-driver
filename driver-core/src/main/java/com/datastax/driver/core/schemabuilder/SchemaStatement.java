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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.DataType;
import com.google.common.base.Strings;

public abstract class SchemaStatement {

    static final String STATEMENT_START = "\n\t";
    static final String COLUMN_FORMATTING = "\n\t\t";

    static final List<String> RESERVED_KEYWORDS = Arrays.asList("add,allow,alter,and,any,apply,asc,authorize,batch,begin,by,columnfamily,create,delete,desc,drop,each_quorum,from,grant,in,index,inet,infinity,insert,into,keyspace,keyspaces,limit,local_one,local_quorum,modify,nan,norecursive,of,on,order,password,primary,quorum,rename,revoke,schema,select,set,table,three,to,token,truncate,two,unlogged,update,use,using,where,with".split(","));

    abstract String buildInternal();

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
