/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.cassandra;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiURI;

/**
 * <p>Multiple instances of Kiji can be installed on a single Cassandra cluster.  Within a Kiji
 * instance, several Cassandra tables are created to manage system, metadata, schemas, and
 * user-space tables.  This class represents the name of one of those Cassandra tables that are
 * created and managed by Kiji.  This class should only be used internally in Kiji modules, or by
 * framework application developers who need direct access to Cassandra tables managed by Kiji.</p>
 *
 * <p> The names of tables in Cassandra created and managed by Kiji are made of a list of delimited
 * components.  There are at least 3 components of a name:
 *
 * <ol>
 *   <li>
 *     Prefix: a literal string "kiji" used to mark that this table is managed by Kiji.
 *   </li>
 *   <li>
 *     KijiInstance: the name of kiji instance managing this table.
 *   </li>
 *   <li>
 *     Type: the type of table (system, schema, meta, user).
 *   </li>
 *   <li>
 *     Name: if the type of the table is "user", then its name (the name users of Kiji would use to
 *     refer to it), is the fourth component.
 *   </li>
 *   <li>
 *     Locality Group: if the type of the table is "user", then the Kiji locality group is the
 *     fifth component.
 *   </li>
 * </ol>
 *
 * <p>
 * For example, a Cassandra cluster might have the following tables:
 * <pre>
 * devices
 * kiji_default.meta
 * kiji_default.schema
 * kiji_default.schema_hash
 * kiji_default.schema_id
 * kiji_default.system
 * kiji_default.t_foo_info
 * kiji_default.t_foo_data
 * kiji_default.c_foo
 * kiji_default.t_bar_default
 * kiji_default.c_bar
 * kiji_experimental.meta
 * kiji_experimental.schema
 * kiji_experimental.schema_hash
 * kiji_experimental.schema_id
 * kiji_experimental.system
 * kiji_experimental.t_baz_info
 * kiji_experimental.c_baz
 * </pre>
 *
 * In this example, there is a Cassandra keyspace completely unrelated to Kiji called "devices."
 * There are two Kiji installations, one called "default" and another called "experimental."  Within
 * the "default" installation, there are two Kiji tables, "foo" and "bar."  Within the
 * "experimental" installation, there is a single Kiji table "baz."
 * </p>
 *
 * Note that Cassandra does not allow the "." character in keyspace or table names, so the "_"
 * character is used instead.
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class KijiManagedCassandraTableName {

  /** The first component of all Cassandra keyspaces managed by Kiji. */
  public static final String KEYSPACE_PREFIX = "kiji";

  /** The name component used for the Kiji meta table. */
  private static final String KIJI_META_KEY_VALUE_COMPONENT = "meta_key_value";

  /** The name component used for the Kiji meta table (layout). */
  private static final String KIJI_META_LAYOUT_COMPONENT= "meta_layout";

  /** The name component used for the Kiji schema hash table. */
  private static final String KIJI_SCHEMA_HASH_COMPONENT = "schema_hash";

  /** The name component used for the Kiji schema IDs table. */
  private static final String KIJI_SCHEMA_ID_COMPONENT = "schema_id";

  /** The name component used for the Kiji schema IDs counter table. */
  private static final String KIJI_SCHEMA_COUNTER_COMPONENT = "schema_id_counter";

  /** The name component used for the Kiji system table. */
  private static final String KIJI_SYSTEM_COMPONENT = "system";

  /** The name component used for all user-space Kiji tables. */
  private static final String KIJI_USER_TABLE_COMPONENT = "t";

  /** The name component used for all storing counters for user-space Kiji tables. */
  private static final String KIJI_USER_COUNTER_COMPONENT = "counter";

  /** The Cassandra table name. */
  private final String mCassandraTableName;

  /** The Kiji instance name. */
  private final String mKijiInstanceName;

  private enum TableType {
    META_KEY_VALUE("meta_key_value"),
    META_LAYOUT("meta_layout"),
    SCHEMA_HASH("schema_hash"),
    SCHEMA_ID("schema_id"),
    SCHEMA_COUNTER("schema_counter"),
    SYSTEM("system"),
    USER("t"),
    USER_COUNTER("c");

    private final String mName;
    TableType(final String name) {
      mName = name;
    }
    public String getName() {
      return mName;
    }
  }

  /**
   * Constructs a Kiji-managed Cassandra table name.  The name will have quotes in it so that it
   * can be used in CQL queries without additional processing (CQL is case-insensitive without
   * quotes).
   *
   * @param kijiInstanceName The kiji instance name.
   * @param type The type component of the Cassandra table name (e.g., meta, schema, system, table).
   */
  private KijiManagedCassandraTableName(String kijiInstanceName, String type) {
    // mCassandraTableName = KIJI_COMPONENT + "_" + kijiInstanceName + "." + type;
    mCassandraTableName = String.format(
        "\"%s_%s\".\"%s\"",
        KEYSPACE_PREFIX,
        kijiInstanceName,
        type);
    mKijiInstanceName = kijiInstanceName;
  }

  /**
   * Constructs a Kiji-managed Cassandra table name for the backing C* table for user data or for
   * counters.  The name will have quotes in it so that it can be used in CQL queries without
   * additional processing (CQL is case-insensitive without quotes).
   *
   * @param instanceName The kiji instance name.
   * @param type The type of the table. Must be `USER` or `USER_COUNTER`.
   * @param tableName The name of the user-space Kiji table.
   * @param localityGroup of the user table, if type `USER`, or null.
   */
  private KijiManagedCassandraTableName(
      String instanceName,
      TableType type,
      String tableName,
      String localityGroup) {
    switch (type) {
      case USER: {
        Preconditions.checkArgument(localityGroup != null, "Locality group may not be null.");
        mCassandraTableName = String.format(
            "\"%s_%s\".\"%s_%s_%s\"",
            KEYSPACE_PREFIX,
            instanceName,
            type.getName(),
            tableName,
            localityGroup);
        break;
      }
      case USER_COUNTER: {
        Preconditions.checkArgument(localityGroup != null, "Locality group must be null.");
        mCassandraTableName = String.format(
            "\"%s_%s\".\"%s_%s\"",
            KEYSPACE_PREFIX,
            instanceName,
            type.getName(),
            tableName);
        break;
      }
      default: throw new IllegalArgumentException();
    }
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji meta table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji meta table.
   */
  public static KijiManagedCassandraTableName getMetaLayoutTableName(KijiURI kijiURI) {
    return new KijiManagedCassandraTableName(kijiURI.getInstance(), KIJI_META_LAYOUT_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji user-defined
   * key-value pairs.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji meta table.
   */
  public static KijiManagedCassandraTableName getMetaKeyValueTableName(KijiURI kijiURI) {
    return new KijiManagedCassandraTableName(kijiURI.getInstance(), KIJI_META_KEY_VALUE_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji schema hash table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji schema hash table.
   */
  public static KijiManagedCassandraTableName getSchemaHashTableName(KijiURI kijiURI) {
    return new KijiManagedCassandraTableName(kijiURI.getInstance(), KIJI_SCHEMA_HASH_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji schema IDs table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji schema IDs table.
   */
  public static KijiManagedCassandraTableName getSchemaIdTableName(KijiURI kijiURI) {
    return new KijiManagedCassandraTableName(kijiURI.getInstance(), KIJI_SCHEMA_ID_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji schema IDs counter
   * table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji schema IDs counter table.
   */
  public static KijiManagedCassandraTableName getSchemaCounterTableName(KijiURI kijiURI) {
    return new KijiManagedCassandraTableName(kijiURI.getInstance(), KIJI_SCHEMA_COUNTER_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji system table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji system table.
   */
  public static KijiManagedCassandraTableName getSystemTableName(KijiURI kijiURI) {
    return new KijiManagedCassandraTableName(kijiURI.getInstance(), KIJI_SYSTEM_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds a user-space Kiji table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @param tableName The name of the user-space Kiji table.
   * @return The name of the Cassandra table used to store the user-space Kiji table.
   */
  public static KijiManagedCassandraTableName getKijiTableName(KijiURI kijiURI, String tableName) {
    return new KijiManagedCassandraTableName(
        kijiURI.getInstance(),
        KIJI_USER_TABLE_COMPONENT,
        tableName);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds counters for a user-space
   * Kiji table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @param tableName The name of the user-space Kiji table.
   * @return The name of the Cassandra table used to store the user-space Kiji table.
   */
  public static KijiManagedCassandraTableName getKijiCounterTableName(
      KijiURI kijiURI,
      String tableName) {
    return new KijiManagedCassandraTableName(
        kijiURI.getInstance(),
        KIJI_USER_COUNTER_COMPONENT,
        tableName);
  }

  /**
   * Gets the name of the Kiji instance this named table belongs to.
   *
   * @return The name of the kiji instance.
   */
  public String getKijiInstanceName() {
    return mKijiInstanceName;
  }

  /**
   * Get the name of the keyspace (formatted for CQL) in C* for the Kiji instance specified in the
   * URI.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the C* keyspace.
   */
  public static String getCassandraKeyspaceFormattedForCQL(KijiURI kijiURI) {
    return String.format(
        "\"%s_%s\"",
        KEYSPACE_PREFIX,
        kijiURI.getInstance());
  }

  /**
   * Check whether a table name is formatted correctly for a CQL query.
   *
   * The table name should be of the form "\"keyspace\".\"table\"" or "\"table\"".
   *
   * @param tableName The table name to check.
   * @return Whether the name is formatted correctly for CQL or not.
   */
  public static boolean tableNameIsFormattedForCQL(String tableName) {
    // TODO: Add additional checks, e.g., having only a single "." outside of quotes.
    return (tableName.startsWith("\"") && tableName.endsWith("\""));
  }

  /**
   * Check whether a keyspace name is formatted correctly for a CQL query.
   *
   * The keyspace name should be of the form "\"keyspace\".
   *
   * @param keyspaceName The keyspace name to check.
   * @return Whether the name is formatted correctly for CQL or not.
   */
  public static boolean keyspaceNameIsFormattedForCQL(String keyspaceName) {
    // TODO: Add additional checks, e.g., not having a "." outside of quotes.
    return (keyspaceName.startsWith("\"") && keyspaceName.endsWith("\""));
  }

  /**
   * Get the Cassandra-formatted name for this table.
   *
   * The name include the keyspace, and is formatted with quotes so that it is ready to get into a
   * CQL query.
   *
   * @return The Cassandra-formatted name of this table.
   */
  public String toString() {
    return mCassandraTableName;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KijiManagedCassandraTableName)) {
      return false;
    }
    return mCassandraTableName.equals(((KijiManagedCassandraTableName) other).mCassandraTableName);
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
