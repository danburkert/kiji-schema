/**
 * (c) Copyright 2014 WibiData, Inc.
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

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * The name of a Kiji-controlled Cassandra table name.
 *
 * <p>
 *   Multiple instances of Kiji can be installed on a single Cassandra cluster.  Within a Kiji
 *   instance, several Cassandra tables are created to manage system, metadata, schemas, and
 *   user-space tables. This class represents the name of one of those Cassandra tables that are
 *   created and managed by Kiji.  This class should only be used internally in Kiji modules, or by
 *   framework application developers who need direct access to Cassandra tables managed by Kiji.
 * </p>
 *
 * <p>
 *   The names of tables in Cassandra created and managed by Kiji are made of a list of delimited
 *   components.  There are at least 3 components of a name:
 * </p>
 *
 * <ol>
 *   <li>
 *     Prefix: a literal string "kiji" used to mark that this table is managed by Kiji.
 *   </li>
 *   <li>
 *     KijiInstance: the name of kiji instance managing this table.
 *   </li>
 *   <li>
 *     Type: the type of table (system, schema, meta, locality group, counter).
 *   </li>
 *   <li>
 *     Name: if the table is a locality group or counter table ("lg" or "c", respectively), then the
 *     Kiji table's name is the fourth component.
 *   </li>
 *   <li>
 *     Name: if the table is a locality group ("lg"), then the locality group ID is the fifth
 *     component.
 *   </li>
 * </ol>
 *
 * <p>
 *   For example, a Cassandra cluster might have the following tables:
 * </p>
 * <pre>
 * devices
 * kiji_default.meta
 * kiji_default.schema
 * kiji_default.schema_hash
 * kiji_default.schema_id
 * kiji_default.system
 * kiji_default.lg_foo_BB
 * kiji_default.lg_foo_BC
 * kiji_default.c_foo
 * kiji_default.t_bar_BB
 * kiji_default.c_bar
 * kiji_experimental.meta
 * kiji_experimental.schema
 * kiji_experimental.schema_hash
 * kiji_experimental.schema_id
 * kiji_experimental.system
 * kiji_experimental.t_baz_BB
 * kiji_experimental.c_baz
 * </pre>
 *
 * <p>
 *   In this example, there is a Cassandra keyspace completely unrelated to Kiji called "devices."
 *   There are two Kiji installations, one called "default" and another called "experimental."
 *   Within the "default" installation, there are two Kiji tables, "foo" and "bar."  Within the
 *   "experimental" installation, there is a single Kiji table "baz."
 * </p>
 *
 * <p>
 *   Note that Cassandra does not allow the "." character in keyspace or table names, so the '_'
 *   character is used as a delimiter.
 * </p>
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class CassandraTableName {

  /** The first component of all Cassandra keyspaces managed by Kiji. */
  public static final String KEYSPACE_PREFIX = "kiji";

  /** The Kiji table type. */
  private final TableType mType;

  /** The Kiji instance name. */
  private final String mInstance;

  /** The Kiji table name, or null. */
  private final String mTable;

  /** The Kiji locality group ID, or null. */
  private final ColumnId mLocalityGroup;

  /** The types of Cassandra tables used by Kiji. */
  private enum TableType {
    META_KEY_VALUE("meta_key_value"),
    META_LAYOUT("meta_layout"),
    SCHEMA_HASH("schema_hash"),
    SCHEMA_ID("schema_id"),
    SCHEMA_COUNTER("schema_counter"),
    SYSTEM("system"),
    LOCALITY_GROUP("lg"),
    COUNTER_GROUP("c");

    private final String mName;

    /**
     * Default constructor.
     *
     * @param name of table type.
     */
    TableType(final String name) {
      mName = name;
    }


    /**
     * Get the table type name prefixed to table names in Cassandra.
     *
     * @return the table type prefix name.
     */
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
   * @param type The {@code TableType} of the table.
   * @param instance The Kiji instance the table belongs to.
   * @param table The name of the Kiji table, or null.
   * @param localityGroup The ID of the Kiji table's locality group, or null.
   */
  private CassandraTableName(
      final TableType type,
      final String instance,
      final String table,
      final ColumnId localityGroup) {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(instance);
    Preconditions.checkArgument(
        (type != TableType.LOCALITY_GROUP && type != TableType.COUNTER_GROUP) || table != null,
        "Table name must be defined for a user Kiji table.");
    Preconditions.checkArgument(
        type != TableType.LOCALITY_GROUP || localityGroup != null,
        "Locality group ID must be defined for a locality group table.");

    mType = type;
    mInstance = instance;
    mTable = table;
    mLocalityGroup = localityGroup;
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
   * Gets a new instance of a Kiji-managed Cassandra table that holds counters for a user-space
   * Kiji table.
   *
   * @param tableURI The name of the Kiji table.
   * @return The name of the Cassandra table used to store the user-space Kiji table.
   */
  public static CassandraTableName getCounterTableName(KijiURI tableURI) {
    return new CassandraTableName(
        TableType.COUNTER_GROUP,
        tableURI.getInstance(),
        tableURI.getTable(),
        null);
  }

  public static CassandraTableName getLocalityGroupTableName(
      final KijiURI tableURI,
      final ColumnId localityGroupID
  ) {
    return new CassandraTableName(
        TableType.LOCALITY_GROUP,
        tableURI.getInstance(),
        tableURI.getTable(),
        localityGroupID);
  }

  public static CassandraTableName getLocalityGroupTableName(
      final KijiURI tableURI,
      final KijiColumnName column,
      final KijiTableLayout layout
  ) {
    // Does not check if the column is fully qualified, and if so if it is a counter column
    final ColumnId lgID = layout.getFamilyMap().get(column.getFamily()).getLocalityGroup().getId();
    return getLocalityGroupTableName(tableURI, lgID);
  }

  /**
   * Get a Cassandra table name.
   *
   * @param tableURI of user Kiji table.
   * @return the Cassandra table name corresponding to the Kiji table.
   */
  @Deprecated
  public static CassandraTableName getKijiTableName(KijiURI tableURI) {
    return new CassandraTableName(
        TableType.LOCALITY_GROUP,
        tableURI.getInstance(),
        tableURI.getTable(),
        null);
  }

  /**
   * Get the name of the keyspace (formatted for CQL) in C* for the Kiji instance specified in the
   * URI.
   *
   * @param instanceURI The name of the Kiji instance.
   * @return The name of the C* keyspace.
   */
  public static String getQuotedKeyspace(KijiURI instanceURI) {
    return appendCassandraKeyspace(new StringBuilder("\""), instanceURI.getInstance())
        .append("\"")
        .toString();
  }

  /**
   * Get the name of the keyspace (formatted for CQL) in C* for the Kiji instance specified in the
   * URI.
   *
   * @param instanceURI The name of the Kiji instance.
   * @return The name of the C* keyspace.
   */
  public static String getKeyspace(KijiURI instanceURI) {
    return appendCassandraKeyspace(new StringBuilder(), instanceURI.getInstance()).toString();
  }

  /**
   * Add the unquoted Cassandra keyspace to the provided StringBuilder, and return it.
   *
   * @param builder to add the Cassandra keyspace to.
   * @param instance name.
   * @return the builder.
   */
  private static StringBuilder appendCassandraKeyspace(StringBuilder builder, String instance) {
    // "${KEYSPACE_PREFIX}_${instanceName}"
    return builder.append(KEYSPACE_PREFIX).append('_').append(instance);
  }

  /**
   * Get the keyspace of this Cassandra table name.
   *
   * @return the keyspace of this Cassandra table name.
   */
  public String getKeyspace() {
    return appendCassandraKeyspace(new StringBuilder(), mInstance).toString();
  }

  /**
   * Get the table name of this Cassandra table name.
   *
   * @return the table name of this Cassandra table name.
   */
  public String getTable() {
    return appendCassandraTableName(new StringBuilder()).toString();
  }

  /**
   * Get the table name of this Cassandra table name.
   *
   * the table name is formatted with quotes to be CQL-compatible.
   *
   * @return the quoted table name of this Cassandra table name.
   */
  public String getQuotedTable() {
    return appendCassandraTableName(new StringBuilder().append('"')).append('"').toString();
  }

  /**
   * Add the unquoted Cassandra table name to the provided StringBuilder, and return it.
   *
   * @param builder to add the Cassandra table name to.
   * @return the builder.
   */
  private StringBuilder appendCassandraTableName(StringBuilder builder) {
    // "${type}[_${table_name}][_${locality_group_id}]
    return Joiner.on('_').skipNulls().appendTo(builder, mType.getName(), mTable, mLocalityGroup);
  }

  /**
   * Gets the Kiji instance of this Cassandra table.
   *
   * @return the Kiji instance.
   */
  public String getKijiInstance() {
    return mInstance;
  }

  /**
   * Gets the Kiji table of this Cassandra table.
   *
   * @return the Kiji table.
   */
  public String getKijiTable() {
    return mTable;
  }

  /**
   * Gets the locality group ID of this Cassandra table.
   *
   * @return the locality group ID.
   */
  public ColumnId getLocalityGroupId() {
    return mLocalityGroup;
  }

  /**
   * Returns whether this Cassandra table name is for a Kiji locality group.
   *
   * @return Whether this Cassandra table name is for a Kiji locality group.
   */
  public boolean isLocalityGroup() {
    return mType == TableType.LOCALITY_GROUP;
  }

  /**
   * Returns whether this Cassandra table name is for Kiji counters.
   *
   * @return Whether this Cassandra table name is for a Kiji counters.
   */
  public boolean isCounter() {
    return mType == TableType.COUNTER_GROUP;
  }

  /**
   * Get the Cassandra-formatted name for this table.
   *
   * The name include the keyspace, and is formatted with quotes so that it is ready to get into a
   * CQL query.
   *
   * @return The Cassandra-formatted name of this table.
   */
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append('"');
    appendCassandraKeyspace(builder, mInstance);
    builder.append("\".\"");
    appendCassandraTableName(builder);
    builder.append('"');
    return builder.toString();
  }

  /** {@inheritDoc}. */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CassandraTableName)) {
      return false;
    }
    final CassandraTableName other = (CassandraTableName) obj;
    return Objects.equal(mType, other.mType)
        && Objects.equal(mInstance, other.mInstance)
        && Objects.equal(mTable, other.mTable);
  }

  /** {@inheritDoc}. */
  @Override
  public int hashCode() {
    return Objects.hashCode(mType, mInstance, mTable);
  }
}
