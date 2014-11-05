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

import static java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.datastax.driver.core.DataType;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.datastax.driver.core.schemabuilder.SchemaBuilder.Sorting;

/**
 * A built CREATE TABLE statement
 */
public class Create extends AbstractCreateStatement<Create> {

    private String tableName;
    private Map<String, ColumnType> partitionColumns = new LinkedHashMap<String, ColumnType>();
    private Map<String, ColumnType> clusteringColumns = new LinkedHashMap<String, ColumnType>();
    private Map<String, ColumnType> staticColumns = new LinkedHashMap<String, ColumnType>();

    Create(String keyspaceName, String tableName) {
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotEmpty(tableName, "Table name");
        validateNotKeyWord(keyspaceName, String.format("The keyspace name '%s' is not allowed because it is a reserved keyword", keyspaceName));
        validateNotKeyWord(tableName, String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
        this.tableName = tableName;
        this.keyspaceName = Optional.fromNullable(keyspaceName);
    }

    Create(String tableName) {
        validateNotEmpty(tableName, "Table name");
        validateNotKeyWord(tableName, String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
        this.tableName = tableName;
    }

    @Override
    protected Create getThis() {
        return this;
    }


    /**
     * Adds a columnName and dataType for a partition key.
     * <p>
     * Please note that partition keys are added in the order of their declaration;
     *
     * @param columnName the name of the partition key to be added;
     * @param dataType the data type of the partition key to be added.
     * @return this CREATE statement.
     */
    public Create addPartitionKey(String columnName, DataType dataType) {
        validateNotEmpty(columnName, "Partition key name");
        validateNotNull(dataType, "Partition key type");
        validateNotKeyWord(columnName, String.format("The partition key name '%s' is not allowed because it is a reserved keyword", columnName));
        partitionColumns.put(columnName, new ColumnType.NativeColumnType(dataType));
        return this;
    }

    /**
     * Adds a columnName and dataType for a partition key, when the type is a frozen UDT.
     * <p>
     * Please note that partition keys are added in the order of their declaration;
     *
     * @param columnName the name of the clustering key to be added;
     * @param udtType the UDT type of the partition key to be added.
     * @return this CREATE statement.
     */
    public Create addUDTPartitionKey(String columnName, UDTType udtType) {
        validateNotEmpty(columnName, "Clustering key name");
        validateNotNull(udtType, "UDT partition key type");
        validateNotKeyWord(columnName, String.format("The partition key name '%s' is not allowed because it is a reserved keyword", columnName));
        partitionColumns.put(columnName, udtType);
        return this;
    }

    /**
     * Adds a columnName and dataType for a clustering key.
     * <p>
     * Please note that clustering keys are added in the order of their declaration;
     *
     * @param columnName the name of the clustering key to be added;
     * @param dataType the data type of the clustering key to be added.
     * @return this CREATE statement.
     */
    public Create addClusteringKey(String columnName, DataType dataType) {
        validateNotEmpty(columnName, "Clustering key name");
        validateNotNull(dataType, "Clustering key type");
        validateNotKeyWord(columnName, String.format("The clustering key name '%s' is not allowed because it is a reserved keyword", columnName));
        clusteringColumns.put(columnName, new ColumnType.NativeColumnType(dataType));
        return this;
    }

    /**
     * Adds a columnName and UDT type for a clustering key.
     * <p>
     * Please note that clustering keys are added in the order of their declaration;
     *
     * @param columnName the name of the clustering key to be added;
     * @param udtType the UDT type of the clustering key to be added.
     * @return this CREATE statement.
     */
    public Create addUDTClusteringKey(String columnName, UDTType udtType) {
        validateNotEmpty(columnName, "Clustering key name");
        validateNotNull(udtType, "UDT clustering key type");
        validateNotKeyWord(columnName, String.format("The clustering key name '%s' is not allowed because it is a reserved keyword", columnName));
        clusteringColumns.put(columnName, udtType);
        return this;
    }

    /**
     * Adds a <strong>static</strong> columnName and dataType for a simple column.
     *
     * @param columnName the name of the <strong>static</strong> column to be added;
     * @param dataType the data type of the column to be added.
     * @return this CREATE statement.
     */
    public Create addStaticColumn(String columnName, DataType dataType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(dataType, "Column type");
        validateNotKeyWord(columnName, String.format("The static column name '%s' is not allowed because it is a reserved keyword", columnName));
        staticColumns.put(columnName, new ColumnType.NativeColumnType(dataType));
        return this;
    }

    /**
     * Adds a <strong>static</strong> columnName and UDT type for a simple column.
     *
     * @param columnName the name of the <strong>static</strong> column to be added;
     * @param udtType the UDT type of the column to be added.
     * @return this CREATE statement.
     */
    public Create addUDTStaticColumn(String columnName, UDTType udtType) {
        validateNotEmpty(tableName, "Column name");
        validateNotNull(udtType, "Column UDT type");
        validateNotKeyWord(columnName, String.format("The static column name '%s' is not allowed because it is a reserved keyword", columnName));
        staticColumns.put(columnName, udtType);
        return this;
    }

    /**
     * Adds options for this CREATE TABLE statement.
     *
     * @return the options of this CREATE TABLE statement.
     */
    public Options withOptions() {
        return new Options(this);
    }

    /**
     * The table options of a CREATE TABLE statement.
     */
    public static class Options extends TableOptions<Options> {

        private final Create create;

        private Options(Create create) {
            super(create);
            this.create = create;
        }

        private List<ClusteringOrder> clusteringOrderKeys = Collections.emptyList();

        private boolean compactStorage;

        /**
         * Adds a clustering order for this table.
         *
         * @param clusteringOrders the columns defining the order (use {@link SchemaBuilder#clusteringOrder(String, Sorting)} to create instances).
         *                         They will be added in their declaration order.
         * @return this {@code Options} object
         */
        public Options clusteringOrder(ClusteringOrder... clusteringOrders) {
            if (clusteringOrders == null || clusteringOrders.length == 0) {
                throw new IllegalArgumentException(String.format("Cannot create table '%s' with null or empty clustering order keys", create.tableName));
            }
            for (ClusteringOrder clusteringOrder : clusteringOrders) {
                final String clusteringColumnName = clusteringOrder.getClusteringColumnName();
                if (!create.clusteringColumns.containsKey(clusteringColumnName)) {
                    throw new IllegalArgumentException(String.format("Clustering key '%s' is unknown. Did you forget to declare it first ?", clusteringColumnName));
                }
            }
            clusteringOrderKeys = Arrays.asList(clusteringOrders);
            return this;
        }

        /**
         * Enables the compact storage option for the table.
         *
         * @return this {@code Options} object
         */
        public Options compactStorage() {
            this.compactStorage = true;
            return this;
        }


        /**
         * Defines a clustering order
         */
        public static class ClusteringOrder {
            private String clusteringColumnName;
            private Sorting sorting = Sorting.ASC;


            /**
             * Create a new clustering ordering with colummName/order
             *
             * @param clusteringColumnName the clustering column.
             * @param sorting the {@link Sorting}. Possible values are: ASC, DESC.
             */
            public ClusteringOrder(String clusteringColumnName, Sorting sorting) {
                validateNotEmpty(clusteringColumnName, "Column name for clustering order");
                this.clusteringColumnName = clusteringColumnName;
                this.sorting = sorting;
            }

            /**
             * Create a new clustering ordering with colummName and default ASC sorting order
             *
             * @param clusteringColumnName the clustering column.
             */
            public ClusteringOrder(String clusteringColumnName) {
                validateNotEmpty(clusteringColumnName, "Column name for clustering order");
                this.clusteringColumnName = clusteringColumnName;
            }

            public String getClusteringColumnName() {
                return clusteringColumnName;
            }

            public Sorting getSorting() {
                return sorting;
            }

            public String toStatement() {
                return clusteringColumnName + " " + sorting.name();
            }

            @Override
            public String toString() {
                return toStatement();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o instanceof ClusteringOrder) {
                    ClusteringOrder that = (ClusteringOrder) o;
                    return Objects.equal(this.clusteringColumnName, that.clusteringColumnName) && Objects.equal(this.sorting, that.sorting);
                }
                return false;
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(clusteringColumnName, sorting);
            }
        }


        @Override
        String buildOptions() {
            final List<String> commonOptions = super.buildCommonOptions();
            List<String> options = new ArrayList<String>(commonOptions);

            if (!clusteringOrderKeys.isEmpty()) {
                options.add(new StringBuilder(CLUSTERING_ORDER_BY).append(OPEN_PAREN).append(Joiner.on(SUB_OPTION_SEPARATOR).join(clusteringOrderKeys)).append(CLOSE_PAREN).toString());
            }

            if (compactStorage) {
                if (!create.staticColumns.isEmpty()) {
                    throw new IllegalStateException(String.format("Cannot create table '%s' with compact storage and static columns '%s'", create.tableName, create.staticColumns.keySet()));
                }
                options.add(COMPACT_STORAGE);
            }

            return new StringBuilder(NEW_LINE).append(TAB).append(WITH).append(SPACE).append(Joiner.on(OPTION_SEPARATOR).join(options)).toString();
        }

        /**
         * Generate the final CREATE TABLE statement <strong>with</strong> table options
         *
         * @return the final CREATE TABLE statement <strong>with</strong> table options
         */
        @Override
        public String build() {
            StringBuilder statement = new StringBuilder(super.build());
            statement.append(this.buildOptions());
            return statement.toString();
        }
    }

    @Override
    String buildInternal() {
        if (partitionColumns.size() < 1) {
            throw new IllegalStateException(String.format("There should be at least one partition key defined for the table '%s'", tableName));
        }

        validateColumnsDeclaration();

        StringBuilder createStatement = new StringBuilder(NEW_LINE).append(TAB).append(CREATE_TABLE);
        if (ifNotExists) {
            createStatement.append(SPACE).append(IF_NOT_EXISTS);
        }
        createStatement.append(SPACE);
        if (keyspaceName.isPresent()) {
            createStatement.append(keyspaceName.get()).append(DOT);
        }
        createStatement.append(tableName);

        List<String> allColumns = new ArrayList<String>();
        List<String> partitionKeyColumns = new ArrayList<String>();
        List<String> clusteringKeyColumns = new ArrayList<String>();


        for (Entry<String, ColumnType> entry : partitionColumns.entrySet()) {
            allColumns.add(entry.getKey() + SPACE + entry.getValue().asCQLString());
            partitionKeyColumns.add(entry.getKey());
        }

        for (Entry<String, ColumnType> entry : clusteringColumns.entrySet()) {
            allColumns.add(entry.getKey() + SPACE + entry.getValue().asCQLString());
            clusteringKeyColumns.add(entry.getKey());
        }

        for (Entry<String, ColumnType> entry : staticColumns.entrySet()) {
            allColumns.add(entry.getKey() + SPACE + entry.getValue().asCQLString() + SPACE + STATIC);
        }

        for (Entry<String, ColumnType> entry : simpleColumns.entrySet()) {
            allColumns.add(buildColumnType(entry));
        }

        String partitionKeyPart = partitionKeyColumns.size() == 1 ?
                partitionKeyColumns.get(0)
                : OPEN_PAREN + Joiner.on(SEPARATOR).join(partitionKeyColumns) + CLOSE_PAREN;

        String primaryKeyPart = clusteringKeyColumns.size() == 0 ?
                partitionKeyPart
                : partitionKeyPart + SEPARATOR + Joiner.on(SEPARATOR).join(clusteringKeyColumns);

        createStatement.append(OPEN_PAREN).append(NEW_LINE).append(TAB).append(TAB);
        createStatement.append(Joiner.on(COLUMN_FORMATTING).join(allColumns));
        createStatement.append(COLUMN_FORMATTING).append(PRIMARY_KEY);
        createStatement.append(OPEN_PAREN).append(primaryKeyPart).append(CLOSE_PAREN);
        createStatement.append(CLOSE_PAREN);

        return createStatement.toString();
    }

    /**
     * Generate a CREATE TABLE statement <strong>without</strong> table options
     * @return the final CREATE TABLE statement
     */
    public String build() {
        return buildInternal();
    }

    private void validateColumnsDeclaration() {

        final Collection partitionAndClusteringColumns = this.intersection(partitionColumns.keySet(), clusteringColumns.keySet());
        final Collection partitionAndSimpleColumns = this.intersection(partitionColumns.keySet(), simpleColumns.keySet());
        final Collection clusteringAndSimpleColumns = this.intersection(clusteringColumns.keySet(), simpleColumns.keySet());
        final Collection partitionAndStaticColumns = this.intersection(partitionColumns.keySet(), staticColumns.keySet());
        final Collection clusteringAndStaticColumns = this.intersection(clusteringColumns.keySet(), staticColumns.keySet());
        final Collection simpleAndStaticColumns = this.intersection(simpleColumns.keySet(), staticColumns.keySet());

        if (!partitionAndClusteringColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as partition keys and clustering keys at the same time", partitionAndClusteringColumns));
        }

        if (!partitionAndSimpleColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as partition keys and simple columns at the same time", partitionAndSimpleColumns));
        }

        if (!clusteringAndSimpleColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as clustering keys and simple columns at the same time", clusteringAndSimpleColumns));
        }

        if (!partitionAndStaticColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as partition keys and static columns at the same time", partitionAndStaticColumns));
        }

        if (!clusteringAndStaticColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as clustering keys and static columns at the same time", clusteringAndStaticColumns));
        }

        if (!simpleAndStaticColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The '%s' columns can not be declared as simple columns and static columns at the same time", simpleAndStaticColumns));
        }

        if (!staticColumns.isEmpty() && clusteringColumns.isEmpty()) {
            throw new IllegalStateException(String.format("The table '%s' cannot declare static columns '%s' without clustering columns", tableName, staticColumns.keySet()));
        }
    }

    private <T> Collection<T> intersection(Collection<T> col1, Collection<T> col2) {
        Set<T> set = new HashSet<T>();

        for (T t : col1) {
            if(col2.contains(t)) {
                set.add(t);
            }
        }
        return set;
    }
}
