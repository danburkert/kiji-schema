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

import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.LocalityGroupDesc;
import org.kiji.schema.layout.KijiTableLayout;

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
public final class CassandraTableName {

  /** The first component of all Cassandra keyspaces managed by Kiji. */
  public static final String KEYSPACE_PREFIX = "kiji";

  /** The Cassandra keyspace. */
  private final String mKeyspace;

  /** The Cassandra table name. */
  private final String mTable;

  private enum TableType {
    META_KEY_VALUE("meta_key_value"),
    META_LAYOUT("meta_layout"),
    SCHEMA_HASH("schema_hash"),
    SCHEMA_ID("schema_id"),
    SCHEMA_COUNTER("schema_counter"),
    SYSTEM("system"),
    KIJI_TABLE_LOCALITY_GROUP("t"),
    KIJI_TABLE_COUNTER("c");

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
   * @param type of the Cassandra table (e.g., meta, schema, system, user).
   * @param instanceName of the table.
   */
  private CassandraTableName(TableType type, String instanceName) {
    this(type, instanceName, null, null);
  }

  /**
   * Constructs a Kiji-managed Cassandra table name.  The name will have quotes in it so that it
   * can be used in CQL queries without additional processing (CQL is case-insensitive without
   * quotes).
   *
   * @param type of the Cassandra table (e.g., meta, schema, system, user).
   * @param instanceName of the table.
   * @param tableName of the Kiji table.
   */
  private CassandraTableName(TableType type, String instanceName, String tableName) {
    this(type, instanceName, tableName, null);
  }

  /**
   * Constructs a Kiji-managed Cassandra table name.  The name will have quotes in it so that it
   * can be used in CQL queries without additional processing (CQL is case-insensitive without
   * quotes).
   *
   * @param type of the table.
   * @param instanceName of the table.
   * @param tableName of the Kiji table, or null.
   * @param localityGroup of the table, or null.
   */
  private CassandraTableName(
      TableType type,
      String instanceName,
      String tableName,
      String localityGroup) {
    // mKeyspace = "${KEYSPACE_PREFIX}_${instanceName}"
    // mTable = "${type}[_${table_name}[_${locality_group}]]"
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(instanceName);
    Preconditions.checkArgument(
        (type != TableType.KIJI_TABLE_LOCALITY_GROUP && type != TableType.KIJI_TABLE_COUNTER) || tableName != null,
        "Table name must be defined for a user Kiji table.");
    Preconditions.checkArgument(
        type != TableType.KIJI_TABLE_LOCALITY_GROUP || localityGroup != null,
        "Locality group must be defined for a user Kiji table.");

    mKeyspace = KEYSPACE_PREFIX + '_' + instanceName;
    mTable = Joiner.on('_').skipNulls().join(type.getName(), tableName, localityGroup);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji meta table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji meta table.
   */
  public static CassandraTableName getMetaLayoutTableName(KijiURI kijiURI) {
    return new CassandraTableName(TableType.META_LAYOUT, kijiURI.getInstance());
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji user-defined
   * key-value pairs.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji meta table.
   */
  public static CassandraTableName getMetaKeyValueTableName(KijiURI kijiURI) {
    return new CassandraTableName(TableType.META_KEY_VALUE, kijiURI.getInstance());
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji schema hash table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji schema hash table.
   */
  public static CassandraTableName getSchemaHashTableName(KijiURI kijiURI) {
    return new CassandraTableName(TableType.SCHEMA_HASH, kijiURI.getInstance());
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji schema IDs table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji schema IDs table.
   */
  public static CassandraTableName getSchemaIdTableName(KijiURI kijiURI) {
    return new CassandraTableName(TableType.SCHEMA_ID, kijiURI.getInstance());
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji schema IDs counter
   * table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji schema IDs counter table.
   */
  public static CassandraTableName getSchemaCounterTableName(KijiURI kijiURI) {
    return new CassandraTableName(TableType.SCHEMA_COUNTER, kijiURI.getInstance());
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji system table.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji system table.
   */
  public static CassandraTableName getSystemTableName(KijiURI kijiURI) {
    return new CassandraTableName(TableType.SYSTEM, kijiURI.getInstance());
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the specified locality group
   * for a user-space Kiji table.
   *
   * @param tableURI The name of the Kiji table.
   * @param localityGroup of the table.
   * @return The name of the Cassandra table used to store the user-space Kiji table's locality
   * group.
   */
  public static CassandraTableName getKijiLocalityGroupTableName(
      KijiURI tableURI,
      String localityGroup) {
    return new CassandraTableName(
        TableType.KIJI_TABLE_LOCALITY_GROUP,
        tableURI.getInstance(),
        tableURI.getTable(),
        localityGroup);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds counters for a user-space
   * Kiji table.
   *
   * @param tableURI The name of the Kiji table.
   * @return The name of the Cassandra table used to store the user-space Kiji table.
   */
  public static CassandraTableName getKijiCounterTableName(KijiURI tableURI) {
    return new CassandraTableName(
        TableType.KIJI_TABLE_COUNTER,
        tableURI.getInstance(),
        tableURI.getTable());
  }

  /**
   * Get the set of C* table names that belong to the provided Kiji table's locality groups.
   *
   * @param tableURI of user Kiji table.
   * @param tableLayout of user Kiji table.
   * @return the set of C* table names corresponding to the Kiji table's locality groups.
   */
  public static Set<CassandraTableName> getKijiLocalityGroupTableNames(
      KijiURI tableURI,
      KijiTableLayout tableLayout) {
    ImmutableSet.Builder<CassandraTableName> tableNames = ImmutableSet.builder();
    for (LocalityGroupDesc localityGroup : tableLayout.getDesc().getLocalityGroups()) {
      tableNames.add(getKijiLocalityGroupTableName(tableURI, localityGroup.getName()));
    }
    return tableNames.build();
  }

  /**
   * Get the name of the keyspace (formatted for CQL) in C* for the Kiji instance specified in the
   * URI.
   *
   * @param kijiURI The name of the Kiji instance.
   * @return The name of the C* keyspace.
   */
  public static String getCassandraKeyspace(KijiURI kijiURI) {
    return String.format("\"%s_%s\"", KEYSPACE_PREFIX, kijiURI.getInstance());
  }

  /**
   * Get the keyspace of this Cassandra table name.
   *
   * @return the keyspace of this Cassandra table name.
   */
  public String getKeyspace() {
    return mKeyspace;
  }

  /**
   * Get the keyspace of this Cassandra table name.
   *
   * The keyspace is formatted with quotes to be CQL-compatible.
   *
   * @return the quoted keyspace of this Cassandra table name.
   */
  public String getQuotedKeyspace() {
    return '"' + mKeyspace + '"';
  }

  /**
   * Get the table name of this Cassandra table name.
   *
   * @return the table name of this Cassandra table name.
   */
  public String getTable() {
    return mTable;
  }

  /**
   * Get the table name of this Cassandra table name.
   *
   * the table name is formatted with quotes to be CQL-compatible.
   *
   * @return the quoted table name of this Cassandra table name.
   */
  public String getQuotedTable() {
    return '"' + mTable + '"';
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
    return getKeyspace() + '.' + getTable();
  }

  public String toStringQuoted() {
    return getQuotedKeyspace() + '.' + getQuotedTable();
  }

  /** {@inheritDoc}. */
  @Override
  public boolean equals(Object other) {
    return other instanceof CassandraTableName && toString().equals(other.toString());
  }

  /** {@inheritDoc}. */
  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
