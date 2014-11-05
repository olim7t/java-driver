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

import static com.datastax.driver.core.schemabuilder.Drop.DroppedItem;

/**
 * Static methods to build a CQL3 DML statement.
 * <p>
 * Available DML statements: CREATE/ALTER/DROP
 * <p>
 * Note that it could be convenient to use an 'import static' to use the methods of this class.
 */
public final class SchemaBuilder {

    private SchemaBuilder() {
    }

    /**
     * Start building a new CREATE TABLE statement
     *
     * @param tableName the name of the table to create.
     * @return an in-construction CREATE TABLE statement.
     */
    public static Create createTable(String tableName) {
        return new Create(tableName);
    }

    /**
     * Start building a new CREATE TABLE statement
     *
     * @param keyspaceName the name of the keyspace to be used.
     * @param tableName the name of the table to create.
     * @return an in-construction CREATE TABLE statement.
     */
    public static Create createTable(String keyspaceName, String tableName) {
        return new Create(keyspaceName, tableName);
    }

    /**
     * Start building a new ALTER TABLE statement
     *
     * @param tableName the name of the table to be altered.
     * @return an in-construction ALTER TABLE statement.
     */
    public static Alter alterTable(String tableName) {
        return new Alter(tableName);
    }

    /**
     * Start building a new ALTER TABLE statement
     *
     * @param keyspaceName the name of the keyspace to be used.
     * @param tableName the name of the table to be altered.
     * @return an in-construction ALTER TABLE statement.
     */
    public static Alter alterTable(String keyspaceName, String tableName) {
        return new Alter(keyspaceName, tableName);
    }

    /**
     * Start building a new DROP TABLE statement
     *
     * @param tableName the name of the table to be dropped.
     * @return an in-construction DROP TABLE statement.
     */
    public static Drop dropTable(String tableName) {
        return new Drop(tableName, DroppedItem.TABLE);
    }

    /**
     * Start building a new DROP TABLE statement
     *
     * @param keyspaceName the name of the keyspace to be used.
     * @param tableName the name of the table to be dropped.
     * @return an in-construction DROP TABLE statement.
     */
    public static Drop dropTable(String keyspaceName, String tableName) {
        return new Drop(keyspaceName, tableName, DroppedItem.TABLE);
    }

    /**
     * Start building a new CREATE INDEX statement
     *
     * @param indexName the name of the table to create.
     * @return an in-construction CREATE INDEX statement.
     */
    public static CreateIndex createIndex(String indexName) {
        return new CreateIndex(indexName);
    }

    /**
     * Start building a new DROP INDEX statement
     *
     * @param indexName the name of the index to be dropped.
     * @return an in-construction DROP INDEX statement.
     */
    public static Drop dropIndex(String indexName) {
        return new Drop(indexName, DroppedItem.INDEX);
    }

    /**
     * Start building a new DROP INDEX statement
     *
     * @param keyspaceName the name of the keyspace to be used.
     * @param indexName the name of the index to be dropped.
     * @return an in-construction DROP INDEX statement.
     */
    public static Drop dropIndex(String keyspaceName, String indexName) {
        return new Drop(keyspaceName, indexName, DroppedItem.INDEX);
    }

    /**
     * Shortcut for new Create.Options.ClusteringOrder(clusteringColumnName, clusteringOrder)
     * @param clusteringColumnName the clustering column name
     * @param clusteringOrder the clustering order (DESC/ASC)
     * @return
     */
    public static Create.Options.ClusteringOrder clusteringOrder(String clusteringColumnName, Create.Options.ClusteringOrder.Sorting clusteringOrder) {
        return new Create.Options.ClusteringOrder(clusteringColumnName, clusteringOrder);
    }

    /**
     * Start building a new CREATE TYPE statement
     *
     * @param typeName the name of the custom type to create.
     * @return an in-construction CREATE TYPE statement.
     */
    public static CreateType createType(String typeName) {
        return new CreateType(typeName);
    }

    /**
     * Start building a new CREATE TYPE statement
     *
     * @param keyspaceName the name of the keyspace to be used.
     * @param typeName the name of the custom type to create.
     * @return an in-construction CREATE TYPE statement.
     */
    public static CreateType createType(String keyspaceName, String typeName) {
        return new CreateType(keyspaceName, typeName);
    }

    /**
     * Start building a new DROP TYPE statement
     *
     * @param typeName the name of the type to be dropped.
     * @return an in-construction DROP TYPE statement.
     */
    public static Drop dropType(String typeName) {
        return new Drop(typeName, DroppedItem.TYPE);
    }

    /**
     * Start building a new DROP TYPE statement
     *
     * @param keyspaceName the name of the keyspace to be used.
     * @param typeName the name of the type to be dropped.
     * @return an in-construction DROP TYPE statement.
     */
    public static Drop dropType(String keyspaceName, String typeName) {
        return new Drop(keyspaceName, typeName, DroppedItem.TYPE);
    }

    /**
     * Builds the datatype representation of a frozen UDT, to include in a schema builder statement.
     * <p>
     * <code>frozen("foo")</code> will produce <code>frozen&lt;foo&gt;</code>.
     *
     * @param udtName the name of the UDT.
     * @return the type.
     */
    public static UDTType frozen(String udtName) {
        return UDTType.frozen(udtName);
    }

    /**
     * Builds the datatype representation of a complex UDT type, to include in a schema builder statement.
     * <p>
     * As of Cassandra 2.1, this method is not strictly necessary because {@link Create} and {@link Alter}
     * provide specialized methods to express simple collections of UDTs, but future versions will make it
     * possible to use types such as <code>map&lt;text, map&lt;text, frozen&lt;user&gt;&gt;&gt;</code>.
     *
     * @param literal the type literal as it will appear in the final CQL statement.
     * @return the type
     */
    public static UDTType udtLiteral(String literal) {
        return UDTType.literal(literal);
    }
}
