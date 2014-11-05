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

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A built CREATE TYPE statement
 */
public class CreateType extends AbstractCreateStatement<CreateType> {

    private String typeName;

    CreateType(String keyspaceName, String typeName) {
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotEmpty(typeName, "Custom type name");
        validateNotKeyWord(keyspaceName, String.format("The keyspace name '%s' is not allowed because it is a reserved keyword", keyspaceName));
        validateNotKeyWord(typeName, String.format("The custom type name '%s' is not allowed because it is a reserved keyword", typeName));
        this.typeName = typeName;
        this.keyspaceName = Optional.fromNullable(keyspaceName);
    }

    CreateType(String typeName) {
        validateNotEmpty(typeName, "Custom type name");
        validateNotKeyWord(typeName, String.format("The custom type name '%s' is not allowed because it is a reserved keyword", typeName));
        this.typeName = typeName;
    }


    /**
     * Generate the script for custom type creation
     * @return a CREATE TYPE statement
     */
    public String build() {
        return buildInternal();
    }

    @Override
    protected CreateType getThis() {
        return this;
    }


    @Override
    String buildInternal() {

        StringBuilder createStatement = new StringBuilder(NEW_LINE).append(TAB).append(CREATE_TYPE);
        if (ifNotExists) {
            createStatement.append(SPACE).append(IF_NOT_EXISTS);
        }
        createStatement.append(SPACE);
        if (keyspaceName.isPresent()) {
            createStatement.append(keyspaceName.get()).append(DOT);
        }
        createStatement.append(typeName);

        List<String> allColumns = new ArrayList<String>();
        for (Map.Entry<String, ColumnType> entry : simpleColumns.entrySet()) {
            allColumns.add(buildColumnType(entry).toString());
        }

        createStatement.append(OPEN_PAREN).append(NEW_LINE).append(TAB).append(TAB);
        createStatement.append(Joiner.on(COLUMN_FORMATTING).join(allColumns));
        createStatement.append(CLOSE_PAREN);

        return createStatement.toString();
    }
}
