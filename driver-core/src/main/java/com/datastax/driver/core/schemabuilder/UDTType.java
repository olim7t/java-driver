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

import com.datastax.driver.core.DataType;

/**
 * Represents a CQL type containing a user-defined type (UDT) in a SchemaBuilder statement.
 * <p>
 * Use {@link SchemaBuilder#frozen(String)} and {@link SchemaBuilder#udtLiteral(String)} to build instances of this type.
 */
public final class UDTType implements ColumnType {
    private final String asCQLString;

    private UDTType(String asCQLString) {
        this.asCQLString = asCQLString;
    }

    @Override public String asCQLString() {
        return asCQLString;
    }

    static com.datastax.driver.core.schemabuilder.UDTType frozen(String udtName) {
        SchemaStatement.validateNotEmpty(udtName, "UDT name");
        return new com.datastax.driver.core.schemabuilder.UDTType(SchemaStatement.FROZEN + SchemaStatement.OPEN_TYPE + udtName + SchemaStatement.CLOSE_TYPE);
    }

    static com.datastax.driver.core.schemabuilder.UDTType list(com.datastax.driver.core.schemabuilder.UDTType elementType) {
        return new com.datastax.driver.core.schemabuilder.UDTType(SchemaStatement.LIST + SchemaStatement.OPEN_TYPE + elementType.asCQLString() + SchemaStatement.CLOSE_TYPE);
    }

    static com.datastax.driver.core.schemabuilder.UDTType set(com.datastax.driver.core.schemabuilder.UDTType elementType) {
        return new com.datastax.driver.core.schemabuilder.UDTType(SchemaStatement.SET + SchemaStatement.OPEN_TYPE + elementType.asCQLString() + SchemaStatement.CLOSE_TYPE);
    }

    static com.datastax.driver.core.schemabuilder.UDTType mapWithUDTKey(com.datastax.driver.core.schemabuilder.UDTType keyType, DataType valueType) {
        return new com.datastax.driver.core.schemabuilder.UDTType(SchemaStatement.MAP + SchemaStatement.OPEN_TYPE + keyType.asCQLString() + SchemaStatement.SEPARATOR + valueType + SchemaStatement.CLOSE_TYPE);
    }

    static com.datastax.driver.core.schemabuilder.UDTType mapWithUDTValue(DataType keyType, com.datastax.driver.core.schemabuilder.UDTType valueType) {
        return new com.datastax.driver.core.schemabuilder.UDTType(SchemaStatement.MAP + SchemaStatement.OPEN_TYPE + keyType + SchemaStatement.SEPARATOR + valueType.asCQLString() + SchemaStatement.CLOSE_TYPE);
    }

    static com.datastax.driver.core.schemabuilder.UDTType mapWithUDTKeyAndValue(com.datastax.driver.core.schemabuilder.UDTType keyType, com.datastax.driver.core.schemabuilder.UDTType valueType) {
        return new com.datastax.driver.core.schemabuilder.UDTType(SchemaStatement.MAP + SchemaStatement.OPEN_TYPE + keyType.asCQLString() + SchemaStatement.SEPARATOR + valueType.asCQLString() + SchemaStatement.CLOSE_TYPE);
    }

    static com.datastax.driver.core.schemabuilder.UDTType literal(String literal) {
        SchemaStatement.validateNotEmpty(literal, "UDT type literal");
        return new com.datastax.driver.core.schemabuilder.UDTType(literal);
    }
}
