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

package org.kiji.schema.impl.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.incr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiRowKeyComponents;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Provides utility methods and constants for constructing CQL statements.
 *
 * <h2>Notes on Kiji & Cassandra data model Entity ID to Primary Key translation</h2>
 *
 * <p>
 *   Cassandra (CQL) has the notion of a primary key, which consists of 1 or more CQL columns.  A
 *   primary key composed of >1 column is a compound primary key.  For example, the following table
 *   definition has a compound primary key consisting of two columns (c1, c2):
 * </p>
 *
 * <pre>
 *    CREATE TABLE t1 (
 *      c1 varchar,
 *      c2 int,
 *      c3 blob,
 *      PRIMARY KEY (c1, c2)
 *    )
 * </pre>
 *
 * <p>
 *   The first element of a compound primary key (or the sole element of a non-compound primary key)
 *   is the partition key. For example, in table t1, c1 is the partition key. The partition key is
 *   tokenized in order to determine what partition a row will fall into. IUD operations on rows
 *   with the same partition key are performed atomically and in isolation (theoretically).
 * </p>
 *
 * <p>
 *   The remaining elements of a primary key (if they exist) are referred to as the clustering
 *   columns.  For example, in table t1, c2 is the sole clustering column.
 * </p>
 *
 * <p>
 *   Partition keys can be made up of multiple columns using a composite partition key, for example:
 * </p>
 *
 * <pre>
 *    CREATE TABLE t2 (
 *      c1 uuid,
 *      c2 varchar,
 *      c3 int,
 *      c4 int,
 *      c5 blob,
 *      PRIMARY KEY((c1, c2), c3, c4)
 *    );
 * </pre>
 *
 * <p>
 *   Table t2 has a composite partition key consisting of c1 and c2. Table t2 has clustering columns
 *   c3 and c4.
 * </p>
 *
 * <p>
 *   Kiji RowKeyFormat2 defines 2 valid entity ID formats: formatted and raw.
 * </p>
 *
 * <ul>
 *   <li><em>Formatted</em>: formatted entity IDs consist of 1 or more components of type STRING,
 *     INT, or LONG. additionally, 1 or more of the components (in sequence) must be hashed.  The
 *     hashed components correspond exactly to the partition key of the CQL primary key.  The
 *     unhashed components correspond to the first clustering columns of the CQL primary key. The
 *     name of the columns will match the component names of the entity ID.
 *   </li>
 *
 *   <li><em>Raw</em>: raw entity IDs consist of a single byte array blob component. This single
 *     component corresponds to the partition key of the CQL primary key. There are no clustering
 *     columns in the CQL primary key. The name of the single primary key column is
 *     {@value #RAW_KEY_COL}.
 *   </li>
 * </ul>
 *
 *
 * <h2>Notes on Kiji Cassandra Tables</h2>
 *
 * <p>
 *   A single Kiji table is stored in Cassandra as multiple tables. There will be a Cassandra table
 *   per Kiji locality group, plus a Cassandra table to hold counters.  The counter table holds
 *   counters from all locality groups.  See the javadoc for {@link CassandraTableName} for details
 *   on what the tables are named.
 * </p>
 *
 * <p>
 *   The Cassandra table schema for Kiji locality groups is slightly different than the counters
 *   table. A Cassandra table holding a Kiji locality group contains columns for each entity ID
 *   component, the family, the qualifier, the version, and the value.  The Cassandra table holding
 *   the Kiji table counters contains columns for each entity ID component, the locality group,
 *   the family, the qualifier, and the value.
 * </p>
 */
public final class CQLUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CQLUtils.class);

  // Useful static members for referring to different fields in the C* tables.
  public static final String RAW_KEY_COL = "key";         // Only used for tables with raw eids
  public static final String LOCALITY_GROUP_COL = "lg";   // Only used for counter tables
  public static final String FAMILY_COL = "family";
  public static final String QUALIFIER_COL = "qualifier";
  public static final String VERSION_COL = "version";     // Only used for locality group tables
  public static final String VALUE_COL = "value";

  private static final String BYTES_TYPE = "blob";
  private static final String STRING_TYPE = "varchar";
  private static final String INT_TYPE = "int";
  private static final String LONG_TYPE = "bigint";
  private static final String COUNTER_TYPE = "counter";

  private static final Joiner COMMA_JOINER = Joiner.on(", ");

  /**
   * Private constructor for utility class.
   */
  private CQLUtils() {
  }

  private static LinkedHashMap<String, String> getEntityIdColumnTypes(
      final KijiTableLayout layout
  ) {
    LinkedHashMap<String, String> columns = Maps.newLinkedHashMap();
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: {
        columns.put(RAW_KEY_COL, BYTES_TYPE);
        break;
      }
      case FORMATTED: {
        for (RowKeyComponent component : keyFormat.getComponents()) {
          columns.put(
              translateEntityIDComponentNameToColumnName(component.getName()),
              getCQLType(component.getType()));
        }
        break;
      }
      default: throw new IllegalArgumentException(
          String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
    return columns;
  }

  private static LinkedHashMap<String, Object> getEntityIdColumnValues(
      final KijiTableLayout layout,
      final EntityId entityId
  ) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    final LinkedHashMap<String, Object> columnValues = Maps.newLinkedHashMap();
    switch (keyFormat.getEncoding()) {
      case RAW: {
        columnValues.put(RAW_KEY_COL, ByteBuffer.wrap(entityId.getHBaseRowKey()));
        break;
      }
      case FORMATTED: {
        final List<RowKeyComponent> components = keyFormat.getComponents();
        final List<Object> values = entityId.getComponents();
        Preconditions.checkArgument(components.size() == values.size(),
          "Number of entity ID components (%s) must match the number of entity ID values (%s).",
            components, values);
        for (int i = 0; i < components.size(); i++) {
          columnValues.put(components.get(i).getName(), values.get(i));
        }
        break;
      }
      default: throw new IllegalArgumentException(
          String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
    return columnValues;
  }

  /**
   * Return the columns and their associated types of the primary key for the associated table
   * layout. The returned LinkedHashMap can be iterated through in primary key column order.
   *
   * @param layout to get primary key column and types for.
   * @return a map of column name to CQL column type with proper iteration order.
   */
  private static LinkedHashMap<String, String> getLocalityGroupPrimaryKeyColumns(
      final KijiTableLayout layout
  ) {
    final LinkedHashMap<String, String> columns = getEntityIdColumnTypes(layout);
    columns.put(FAMILY_COL, BYTES_TYPE);
    columns.put(QUALIFIER_COL, BYTES_TYPE);
    columns.put(VERSION_COL, LONG_TYPE);
    return columns;
  }

  private static LinkedHashMap<String, String> getCounterPrimaryKeyColumns(
      final KijiTableLayout layout
  ) {
    final LinkedHashMap<String, String> columns = getEntityIdColumnTypes(layout);
    columns.put(LOCALITY_GROUP_COL, BYTES_TYPE);
    columns.put(FAMILY_COL, BYTES_TYPE);
    columns.put(QUALIFIER_COL, BYTES_TYPE);
    return columns;
  }

  /**
   * Translates an EntityID ComponentType into a CQL type.
   *
   * @param type of entity id component to get CQL type for.
   * @return the CQL type of the provided ComponentType.
   */
  private static String getCQLType(ComponentType type) {
    switch (type) {
      case INTEGER: return INT_TYPE;
      case LONG: return LONG_TYPE;
      case STRING: return STRING_TYPE;
      default: throw new IllegalArgumentException();
    }
  }

  /**
   * Return the ordered list of columns in the partition key for the table layout.
   *
   * @param layout to return partition key columns for.
   * @return the primary key columns for the layout.
   */
  public static List<String> getPartitionKeyColumns(KijiTableLayout layout) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: return Lists.newArrayList(RAW_KEY_COL);
      case FORMATTED:
        return transformToColumns(
            keyFormat.getComponents().subList(0, keyFormat.getRangeScanStartIndex()));
      default:
        throw new IllegalArgumentException(
          String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }

  /**
   * Return the ordered list of columns in the Kiji entity ID for the table layout.
   *
   * @param layout to return entity ID columns for
   * @return the entity ID columns of the table.
   */
  public static List<String> getEntityIDColumns(KijiTableLayout layout) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: return Lists.newArrayList(RAW_KEY_COL);
      case FORMATTED: return transformToColumns(keyFormat.getComponents());
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }

  /**
   * Get the ordered list of cluster columns originating from the entity ID. This is the set of
   * 'scannable' entity ID components.
   *
   * @param layout The layou of the table.
   * @return the cluster columns of the table from the entity ID.
   */
  private static List<String> getEntityIdClusterColumns(KijiTableLayout layout) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: {
        return Lists.newArrayList();
      }
      case FORMATTED: {
        int size = keyFormat.getComponents().size();
        int start = keyFormat.getRangeScanStartIndex();
        if (start == size) {
          return Lists.newArrayList();
        } else {
          return transformToColumns(
              keyFormat
                  .getComponents()
                  .subList(keyFormat.getRangeScanStartIndex(), keyFormat.getComponents().size()));
        }
      }
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }


  /**
   * Return the ordered list of cluster columns for the table layout.
   *
   * @param layout to return cluster columns for.
   * @return the primary key columns for the layout.
   */
  public static List<String> getLocalityGroupClusterColumns(KijiTableLayout layout) {
    List<String> columns = getEntityIdClusterColumns(layout);
    columns.add(FAMILY_COL);
    columns.add(QUALIFIER_COL);
    columns.add(VERSION_COL);
    return columns;
  }

  /**
   * Return the ordered list of cluster columns for the table layout.
   *
   * @param layout to return cluster columns for.
   * @return the primary key columns for the layout.
   */
  public static List<String> getCounterClusterColumns(KijiTableLayout layout) {
    List<String> columns = getEntityIdClusterColumns(layout);
    columns.add(LOCALITY_GROUP_COL);
    columns.add(FAMILY_COL);
    columns.add(QUALIFIER_COL);
    return columns;
  }

  /**
   * Return the CQL token column for a Kiji table layout.
   *
   * @param layout to create CQL token column for.
   * @return the CQL token column for the layout.
   */
  public static String getTokenColumn(KijiTableLayout layout) {
    return String.format("token(%s)", COMMA_JOINER.join(getPartitionKeyColumns(layout)));
  }

  /**
   * Given the name of an entity ID component, returns the corresponding Cassandra column name.
   *
   * Inserts a prefix to make sure that the column names for entity ID components don't conflict
   * with CQL reserved words or with other column names in Kiji Cassandra tables.
   *
   * @param entityIDComponentName The name of the entity ID component.
   * @return the name of the Cassandra column for this component.
   */
  public static String translateEntityIDComponentNameToColumnName(
      final String entityIDComponentName
  ) {
    return "eid_" + entityIDComponentName;
  }

  /**
   * Extract the Entity ID components from a row for a given table layout.
   *
   * @param layout of the table.
   * @param row to extract Entity ID components from.
   * @return the Entity ID components of the row.
   */
  public static KijiRowKeyComponents getRowKeyComponents(KijiTableLayout layout, Row row) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    Object[] components;

    switch (keyFormat.getEncoding()) {
      case RAW: {
        components = new Object[] {CassandraByteUtil.byteBuffertoBytes(row.getBytes(RAW_KEY_COL))};
        break;
      }
      case FORMATTED: {
        List<RowKeyComponent> formatComponents = keyFormat.getComponents();
        components = new Object[formatComponents.size()];

        for (int i = 0; i < formatComponents.size(); i++) {
          RowKeyComponent component = formatComponents.get(i);
          final String columnName = translateEntityIDComponentNameToColumnName(component.getName());
          switch (component.getType()) {
            case STRING: {
              components[i] = row.getString(columnName);
              break;
            }
            case INTEGER: {
              components[i] = row.getInt(columnName);
              break;
            }
            case LONG: {
              components[i] = row.getLong(columnName);
              break;
            }
            default: throw new IllegalArgumentException("Unknown row key component type.");
          }
        }
        break;
      }
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
    return KijiRowKeyComponents.fromComponents(components);
  }

  /**
   * Transforms a list of RowKeyComponents into a list of the column names.
   *
   * @param components to transform into columns.
   * @return a list of columns.
   */
  private static List<String> transformToColumns(List<RowKeyComponent> components) {
    List<String> list = Lists.newArrayList();
    for (RowKeyComponent component : components) {
      list.add(translateEntityIDComponentNameToColumnName(component.getName()));
    }
    return list;
  }

  /**
   * Returns a 'CREATE TABLE' statement for the provided table name and table layout.
   *
   * @param tableName of table to be created.
   * @param layout of kiji table.
   * @return a CQL 'CREATE TABLE' statement which will create the provided table.
   */
  public static String getCreateLocalityGroupTableStatement(
      final CassandraTableName tableName,
      final KijiTableLayout layout
  ) {
    Preconditions.checkArgument(tableName.isLocalityGroup(),
        "Table name '%s' is not for a locality group table.", tableName);

    LinkedHashMap<String, String> columns = getLocalityGroupPrimaryKeyColumns(layout);
    columns.put(VALUE_COL, BYTES_TYPE);

    // statement being built:
    //  "CREATE TABLE ${tableName} (
    //   ${PKColumn1} ${PKColumn1Type}, ${PKColumn2} ${PKColumn2Type}..., ${VALUE_COL} ${valueType}
    //   PRIMARY KEY ((${PartitionKeyComponent1} ${type}, ${PartitionKeyComponent2} ${type}...),
    //                ${ClusterColumn1} ${type}, ${ClusterColumn2} ${type}..))
    //   WITH CLUSTERING
    //   ORDER BY (${ClusterColumn1} ASC, ${ClusterColumn2} ASC..., ${VERSION_COL} DESC);

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(tableName).append(" (");

    COMMA_JOINER.withKeyValueSeparator(" ").appendTo(sb, columns);

    sb.append(", PRIMARY KEY ((");
    COMMA_JOINER.appendTo(sb, getPartitionKeyColumns(layout));
    sb.append(")");

    List<String> clusterColumns = getLocalityGroupClusterColumns(layout);
    if (clusterColumns.size() > 0) {
      sb.append(", ");
    }
    COMMA_JOINER.appendTo(sb, clusterColumns);

    sb.append(")) WITH CLUSTERING ORDER BY (");
    Joiner.on(" ASC, ").appendTo(sb, clusterColumns);

    sb.append(" DESC);");

    String query = sb.toString();

    LOG.info("Prepared query string for table create: {}", query);

    return query;
  }

  /**
   * Returns a 'CREATE TABLE' statement for the counter table for the provided table name and table
   * layout.
   *
   * @param tableName of table to be created.
   * @param layout of kiji table.
   * @return a CQL 'CREATE TABLE' statement which will create the provided table's counter table.
   */
  public static String getCreateCounterTableStatement(
      final CassandraTableName tableName,
      final KijiTableLayout layout
  ) {
    LinkedHashMap<String, String> columns = getCounterPrimaryKeyColumns(layout);
    columns.put(VALUE_COL, COUNTER_TYPE);

    // statement being built:
    //  "CREATE TABLE ${tableName} (
    //   ${PKColumn1} ${PKColumn1Type}, ${PKColumn2} ${PKColumn2Type}..., ${VALUE_COL} ${valueType}
    //   PRIMARY KEY ((${PartitionKeyComponent1} ${type}, ${PartitionKeyComponent2} ${type}...),
    //                ${ClusterColumn1} ${type}, ${ClusterColumn2} ${type}..));

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(tableName).append(" (");

    COMMA_JOINER.withKeyValueSeparator(" ").appendTo(sb, columns);

    sb.append(", PRIMARY KEY ((");
    COMMA_JOINER.appendTo(sb, getPartitionKeyColumns(layout));
    sb.append(")");

    List<String> clusterColumns = getCounterClusterColumns(layout);
    if (clusterColumns.size() > 0) {
      sb.append(", ");
    }
    COMMA_JOINER.appendTo(sb, clusterColumns);

    sb.append("));");

    String query = sb.toString();

    LOG.info("Prepared query string for table create: {}", query);

    return query;
  }

  /**
   * Returns a CQL statement which will create a secondary index on the provided table and column.
   *
   * @param tableName of table to create index on.
   * @param columnName of column to create index on.
   * @return a CQL statement to create a secondary index on the provided table and column.
   */
  public static String getCreateIndexStatement(CassandraTableName tableName, String columnName) {
    return String.format("CREATE INDEX ON %s (%s)", tableName, columnName);
  }

  /**
   * Returns a CQL statement which drop a table.
   *
   * @param table The table to delete.
   * @return A CQL statement to drop the provided table.
   */
  public static String getDropTableStatement(CassandraTableName table) {
    return String.format("DROP TABLE IF EXISTS %s;", table);
  }

  /**
   * Create a CQL statement for selecting a column from a row of a Cassandra Kiji table.
   *
   * @param layout The layout of the table.
   * @param table The name of the table.
   * @param entityId The entity id of row to get.
   * @param column The name of the column to get.
   * @param dataRequest The data request defining the get.
   * @param columnRequest The column request defining the get.
   * @return a statement which will get the column.
   */
  public static Statement getColumnGetStatement(
      KijiTableLayout layout,
      CassandraTableName table,
      EntityId entityId,
      CassandraColumnName column,
      KijiDataRequest dataRequest,
      Column columnRequest
  ) {
    Preconditions.checkArgument(column.containsFamily());

    final Select get;

    if (table.isLocalityGroup()) {
      get = getLocalityGroupGet(table, column, dataRequest, columnRequest);
    } else if (table.isCounter()) {
      get = getCounterGet(table, column);
    } else {
      throw new IllegalArgumentException(
          String.format("Table %s must be a locality group or counter table.", table));
    }

    for (Map.Entry<String, Object> component : getEntityIdColumnValues(layout, entityId).entrySet()) {
      get.where(eq(component.getKey(), component.getValue()));
    }

    return get;
  }

  /**
   * Create a CQL statement for selecting a column from a row in a locality group Cassandra Kiji
   * table.
   *
   * @param table The name of the table.
   * @param column The name of the column to get.
   * @param dataRequest The data request defining the get.
   * @param columnRequest The column request defining the get.
   * @return a statement which will get the column.
   */
  private static Select getLocalityGroupGet(
      final CassandraTableName table,
      final CassandraColumnName column,
      final KijiDataRequest dataRequest,
      final Column columnRequest
  ) {
    Select select =
        select()
            .all()
            .from(table.getKeyspace(), table.getTable())
            .where(eq(FAMILY_COL, column.getFamilyBuffer()))
            .limit(columnRequest.getMaxVersions());

    if (column.containsQualifier()) {
      select.where(eq(QUALIFIER_COL, column.getQualifierBuffer()));
    }

    if (dataRequest.getMaxTimestamp() != Long.MAX_VALUE) {
      select.where(lt(VERSION_COL, dataRequest.getMaxTimestamp()));
    }

    if (dataRequest.getMinTimestamp() != 0L) {
      select.where(gte(VERSION_COL, dataRequest.getMaxTimestamp()));
    }

    select.setFetchSize(
        columnRequest.getPageSize() == 0 ? Integer.MAX_VALUE : columnRequest.getPageSize()
    );

    return select;
  }

  /**
   * Create a CQL statement for selecting a column from a row in a counter Cassandra Kiji table.
   *
   * @param table The name of the table.
   * @param column The name of the column to get.
   * @return a statement which will get the column.
   */
  public static Select getCounterGet(
      CassandraTableName table,
      CassandraColumnName column
  ) {
    return select()
        .all()
        .from(table.getKeyspace(), table.getTable())
        .where(eq(LOCALITY_GROUP_COL, column.getLocalityGroup()))
        .and(eq(FAMILY_COL, column.getFamilyBuffer()))
        .limit(1);
  }

  /**
   * Create a CQL statement for selecting a column from a range of rows in a Cassandra table.
   *
   * @param layout The table layout.
   * @param table The name of the table.
   * @param column The name of the column to get.
   * @param dataRequest The data request defining the get.
   * @param columnRequest The column request defining the get.
   * @param scannerOptions Cassandra specific scanner options (start and end tokens).
   * @return A statement which will scan the column.
   */
  public static Statement getColumnScanStatement(
      final KijiTableLayout layout,
      final CassandraTableName table,
      final CassandraColumnName column,
      final KijiDataRequest dataRequest,
      final Column columnRequest,
      final CassandraKijiScannerOptions scannerOptions
  ) {
    Preconditions.checkArgument(column.containsFamily());

    if (table.isLocalityGroup()) {
      return
          getLocalityGroupScan(layout, table, column, dataRequest, columnRequest, scannerOptions);
    } else if (table.isCounter()) {
      return getCounterScan(layout, table, column, columnRequest, scannerOptions);
    } else {
      throw new IllegalArgumentException(
          String.format("Table %s must be a locality group or counter table.", table));
    }
  }

  /**
   * Create a CQL statement for selecting a column from a range of rows in a locality group
   * Cassandra table.
   *
   * @param layout The table layout.
   * @param table The name of the table.
   * @param column The name of the column to get.
   * @param dataRequest The data request defining the get.
   * @param columnRequest The column request defining the get.
   * @param scannerOptions Cassandra specific scanner options (start and end tokens).
   * @return A select statement which will scan the column.
   */
  private static Statement getLocalityGroupScan(
      final KijiTableLayout layout,
      final CassandraTableName table,
      final CassandraColumnName column,
      final KijiDataRequest dataRequest,
      final Column columnRequest,
      final CassandraKijiScannerOptions scannerOptions
  ) {
    final String tokenColumn = getTokenColumn(layout);

    final Select.Selection selectFrom = select();
    selectFrom.column(tokenColumn);
    for (String eidColumn : getEntityIDColumns(layout)) {
      selectFrom.column(eidColumn);
    }

    selectFrom.column(FAMILY_COL);
    selectFrom.column(QUALIFIER_COL);
    selectFrom.column(VERSION_COL);
    selectFrom.column(VALUE_COL);

    final Select select =
        selectFrom
            .from(table.getKeyspace(), table.getTable())
            .where(eq(FAMILY_COL, column.getFamilyBuffer()))
            .limit(columnRequest.getMaxVersions())
            .allowFiltering();

    if (scannerOptions.hasStartToken()) {
      select.where(gte(tokenColumn, scannerOptions.getStartToken()));
    }

    if (scannerOptions.hasStopToken()) {
      select.where(lt(tokenColumn, scannerOptions.getStopToken()));
    }

    if (column.containsQualifier()) {
      select.where(eq(QUALIFIER_COL, column.getQualifierBuffer()));
    }

    if (dataRequest.getMaxTimestamp() != Long.MAX_VALUE) {
      select.where(lt(VERSION_COL, dataRequest.getMaxTimestamp()));
    }

    if (dataRequest.getMinTimestamp() != 0L) {
      select.where(gte(VERSION_COL, dataRequest.getMaxTimestamp()));
    }

    return select;
  }

  /**
   * Create a CQL statement for selecting a column from a range of rows in a counter Cassandra
   * table.
   *
   * @param layout The table layout.
   * @param table The name of the table.
   * @param column The name of the column to get.
   * @param columnRequest The column request defining the get.
   * @param scannerOptions Cassandra specific scanner options (start and end tokens).
   * @return A select statement which will scan the column.
   */
  public static Statement getCounterScan(
      final KijiTableLayout layout,
      final CassandraTableName table,
      final CassandraColumnName column,
      final Column columnRequest,
      final CassandraKijiScannerOptions scannerOptions
  ) {
    Preconditions.checkArgument(column.containsLocalityGroup());
    final String tokenColumn = getTokenColumn(layout);

    final Select.Selection selectFrom = select();
    selectFrom.column(tokenColumn);
    for (String eidColumn : getEntityIDColumns(layout)) {
      selectFrom.column(eidColumn);
    }

    selectFrom.column(LOCALITY_GROUP_COL);
    selectFrom.column(FAMILY_COL);
    selectFrom.column(QUALIFIER_COL);
    selectFrom.column(VALUE_COL);

    final Select select =
        selectFrom
            .from(table.getKeyspace(), table.getTable())
            .where(eq(LOCALITY_GROUP_COL, column.getLocalityGroup()))
            .and(eq(FAMILY_COL, column.getFamilyBuffer()))
            .limit(columnRequest.getMaxVersions())
            .allowFiltering();

    if (scannerOptions.hasStartToken()) {
      select.where(gte(tokenColumn, scannerOptions.getStartToken()));
    }

    if (scannerOptions.hasStopToken()) {
      select.where(lt(tokenColumn, scannerOptions.getStopToken()));
    }

    if (column.containsQualifier()) {
      select.where(eq(QUALIFIER_COL, column.getQualifierBuffer()));
    }

    return select;
  }

  /**
   * Create a CQL statement for selecting the columns which makeup the Entity ID from a Cassandra
   * Kiji Table.
   *
   * @param layout The table layout.
   * @param table The translated Cassandra table name.
   * @return a statement which will get the single column.
   */
  public static Statement getEntityIDScanStatement(
      final KijiTableLayout layout,
      final CassandraTableName table
  ) {
    final Select.Selection select = select();
    select.column(getTokenColumn(layout));
    for (String column : getEntityIDColumns(layout)) {
      select.column(column);
    }
    return select
        .distinct()
        .from(table.getKeyspace(), table.getTable());
  }

  /**
   * Create a CQL statement to increment the counter in a column.
   *
   * @param layout of table.
   * @param tableName of table.
   * @param entityId of row.
   * @param column to increment.
   * @param amount to increment value.
   * @return a CQL statement to increment a counter column.
   */
  public static Statement getCounterIncrement(
      final KijiTableLayout layout,
      final CassandraTableName tableName,
      final EntityId entityId,
      final CassandraColumnName column,
      final long amount
  ) {
    final Update.Where update =
        update(tableName.getKeyspace(), tableName.getTable())
            .with(incr(VALUE_COL, amount))
            .where(eq(LOCALITY_GROUP_COL, column.getLocalityGroup()))
            .and(eq(FAMILY_COL, column.getFamilyBuffer()))
            .and(eq(QUALIFIER_COL, column.getQualifierBuffer()));

    for (Map.Entry<String, Object> component
        : getEntityIdColumnValues(layout, entityId).entrySet()) {
      update.and(eq(component.getKey(), component.getValue()));
    }

    return update;
  }

  /**
   * Create a CQL statement that executes a Kiji put into a non-counter cell.
   *
   * @param layout table layout of table.
   * @param table translated table name as known by Cassandra.
   * @param entityId of row to select.
   * @param column to insert into.
   * @param version to write the value at.
   * @param value to be written into column.
   * @param ttl of value, or null if forever.
   * @return a Statement which will execute the insert.
   */
  public static Statement getLocalityGroupInsert(
      final KijiTableLayout layout,
      final CassandraTableName table,
      final EntityId entityId,
      final CassandraColumnName column,
      final Long version,
      final ByteBuffer value,
      final Integer ttl
  ) {
    final Insert insert = insertInto(table.getKeyspace(), table.getTable());

    for (Map.Entry<String, Object> component
        : getEntityIdColumnValues(layout, entityId).entrySet()) {
      insert.value(component.getKey(), component.getValue());
    }

    insert
        .value(FAMILY_COL, column.getFamilyBuffer())
        .value(QUALIFIER_COL, column.getQualifierBuffer())
        .value(VERSION_COL, version)
        .value(VALUE_COL, value);

    if (ttl != null && ttl < 630720000) { // 630720000 is the maximum Cassandra TTL
      insert.using(ttl(ttl));
    }

    return insert;
  }

  /**
   * Create a CQL statement to delete a cell.
   *
   * @param layout of table.
   * @param tableName of table.
   * @param entityID of row.
   * @param column containing cell to delete.
   * @param version of cell.
   * @return a CQL statement to delete a cell.
   */
  public static Statement getLocalityGroupDeleteCell(
      final KijiTableLayout layout,
      final CassandraTableName tableName,
      final EntityId entityID,
      final CassandraColumnName column,
      final long version
  ) {
    Preconditions.checkArgument(column.containsQualifier());
    return getLocalityGroupDeleteStatement(layout, tableName, entityID, column, version);
  }

  /**
   * Create a CQL statement to delete a non-counter column from a row.
   *
   * @param layout of table.
   * @param tableName of table.
   * @param entityID of row.
   * @param column containing column to delete.
   *
   * @return a CQL statement to delete a column.
   */
  public static Statement getLocalityGroupDeleteColumn(
      final KijiTableLayout layout,
      final CassandraTableName tableName,
      final EntityId entityID,
      final CassandraColumnName column
  ) {
    return getLocalityGroupDeleteStatement(layout, tableName, entityID, column, null);
  }

  /**
   * Create a CQL statement to delete all columns in a locality group from a row.
   *
   * @param layout of table.
   * @param tableName of table.
   * @param entityID of row.
   * @return a CQL statement to delete a row.
   */
  public static Statement getLocalityGroupDelete(
      final KijiTableLayout layout,
      final CassandraTableName tableName,
      final EntityId entityID
  ) {
    final CassandraColumnName column = new CassandraColumnName(null, (byte[]) null, null);
    return getLocalityGroupDeleteStatement(layout, tableName, entityID, column, null);
  }

  /**
   * Create a CQL statement for deleting from a locality group in a row of a Cassandra Kiji table.
   *
   * @param layout table layout of table.
   * @param tableName translated table name as known by Cassandra.
   * @param entityId of row to delete from.
   * @param column to delete. May be unqualified.
   * @param version to delete, or null if all versions.
   * @return a statement which will delete the column.
   */
  private static Statement getLocalityGroupDeleteStatement(
      KijiTableLayout layout,
      CassandraTableName tableName,
      EntityId entityId,
      CassandraColumnName column,
      Long version
  ) {
    final Delete delete = delete()
        .all()
        .from(tableName.getKeyspace(), tableName.getTable());

    for (Map.Entry<String, Object> component
        : getEntityIdColumnValues(layout, entityId).entrySet()) {
      delete.where(eq(component.getKey(), component.getValue()));
    }

    if (column.containsFamily()) {
      delete.where(eq(FAMILY_COL, column.getFamilyBuffer()));
      if (column.containsQualifier()) {
        delete.where(eq(QUALIFIER_COL, column.getQualifierBuffer()));
        if (version != null) {
          delete.where(eq(VERSION_COL, version));
        }
      }
    }

    return delete;
  }

  /**
   * Create a CQL statement to delete a counter column from a row.
   *
   * @param layout of table.
   * @param tableName of table.
   * @param entityID of row.
   * @param column containing counter(s) to delete.
   *
   * @return a CQL statement to delete a column.
   */
  public static Statement getCounterDeleteColumn(
      final KijiTableLayout layout,
      final CassandraTableName tableName,
      final EntityId entityID,
      final CassandraColumnName column
  ) {
    return getCounterDeleteStatement(layout, tableName, entityID, column);
  }

  /**
   * Create a CQL statement to delete all counters in a row.
   *
   * @param layout of table.
   * @param tableName of table.
   * @param entityID of row.
   * @return a CQL statement to delete a row.
   */
  public static Statement getCounterDelete(
      final KijiTableLayout layout,
      final CassandraTableName tableName,
      final EntityId entityID
  ) {
    final CassandraColumnName column = new CassandraColumnName(null, (byte[]) null, null);
    return getCounterDeleteStatement(layout, tableName, entityID, column);
  }

  /**
   * Create a CQL statement for deleting from a locality group in a row of a Cassandra Kiji table.
   *
   * @param layout table layout of table.
   * @param tableName translated table name as known by Cassandra.
   * @param entityId of row to delete from.
   * @param column to delete.
   * @return a statement which will delete the column.
   */
  private static Statement getCounterDeleteStatement(
      KijiTableLayout layout,
      CassandraTableName tableName,
      EntityId entityId,
      CassandraColumnName column
  ) {
    final Delete delete = delete()
        .all()
        .from(tableName.getKeyspace(), tableName.getTable());

    for (Map.Entry<String, Object> component
        : getEntityIdColumnValues(layout, entityId).entrySet()) {
      delete.where(eq(component.getKey(), component.getValue()));
    }

    if (column.containsLocalityGroup()) {
      delete.where(eq(LOCALITY_GROUP_COL, column.getLocalityGroup()));
      if (column.containsFamily()) {
        delete.where(eq(FAMILY_COL, column.getFamilyBuffer()));
        if (column.containsQualifier()) {
          delete.where(eq(QUALIFIER_COL, column.getQualifierBuffer()));
        }
      }
    }

    return delete;
  }
}
