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
import com.google.common.base.Optional;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AbstractCreateStatement<T extends AbstractCreateStatement<T>> extends SchemaStatement {

    protected Optional<String> keyspaceName = Optional.absent();
    protected boolean ifNotExists;
    protected Map<String, ColumnType> simpleColumns = new LinkedHashMap<String, ColumnType>();

    protected abstract T getThis();

    /**
     * Use 'IF NOT EXISTS' CAS condition for the creation.
     *
     * @return this CREATE statement.
     */
    public T ifNotExists() {
        this.ifNotExists = true;
        return getThis();
    }

    /**
     * Adds a columnName and dataType for the custom type.
     *
     * <p>
     *     To add a list column:
     *     <pre class="code"><code class="java">
     *         addColumn("myList",DataType.list(DataType.text()))
     *     </code></pre>
     *
     *     To add a set column:
     *     <pre class="code"><code class="java">
     *         addColumn("mySet",DataType.set(DataType.text()))
     *     </code></pre>
     *
     *     To add a map column:
     *     <pre class="code"><code class="java">
     *         addColumn("myMap",DataType.map(DataType.cint(),DataType.text()))
     *     </code></pre>
     * </p>
     * @param columnName the name of the column to be added
     * @param dataType the data type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addColumn(String columnName, DataType dataType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(dataType, "Column type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, new ColumnType.NativeColumnType(dataType));
        return getThis();
    }

    /**
     * Adds a columnName and udt type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param udtType the udt type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTColumn(String columnName, UDTType udtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(udtType, "Column type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, udtType);
        return getThis();
    }

    /**
     * Adds a columnName and list of udt type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param udtType the udt type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTListColumn(String columnName, UDTType udtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(udtType, "Column element type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, UDTType.list(udtType));
        return getThis();
    }

    /**
     * Adds a columnName and set of udt type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param udtType the udt type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTSetColumn(String columnName, UDTType udtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(udtType, "Column element type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, UDTType.set(udtType));
        return getThis();
    }

    /**
     * Adds a columnName and map of <raw,udt> type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param keyType the key raw type of the column to be added.
     * @param valueUdtType the value udt type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTMapColumn(String columnName, DataType keyType, UDTType valueUdtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(keyType, "Map key type");
        validateNotNull(valueUdtType, "Map value UDT type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, UDTType.mapWithUDTValue(keyType, valueUdtType));
        return getThis();
    }

    /**
     * Adds a columnName and map of <udt,raw> type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param udtKeyType the key udt type of the column to be added.
     * @param valueType the value raw type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTMapColumn(String columnName, UDTType udtKeyType, DataType valueType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(udtKeyType, "Map key UDT type");
        validateNotNull(valueType, "Map value type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, UDTType.mapWithUDTKey(udtKeyType, valueType));
        return getThis();
    }

    /**
     * Adds a columnName and map of <udt,udt> type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param udtKeyType the key udt type of the column to be added.
     * @param udtValueType the value udt type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTMapColumn(String columnName, UDTType udtKeyType, UDTType udtValueType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(udtKeyType, "Map key UDT type");
        validateNotNull(udtValueType, "Map value UDT type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, UDTType.mapWithUDTKeyAndValue(udtKeyType, udtValueType));
        return getThis();
    }

    protected String buildColumnType(Map.Entry<String, ColumnType> entry) {
        final ColumnType columnType = entry.getValue();
        return entry.getKey() + " " + columnType.asCQLString();
    }
}
