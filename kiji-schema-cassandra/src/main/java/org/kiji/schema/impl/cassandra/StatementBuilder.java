package org.kiji.schema.impl.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import static org.kiji.schema.impl.cassandra.CQLUtils.FAMILY_COL;
import static org.kiji.schema.impl.cassandra.CQLUtils.QUALIFIER_COL;
import static org.kiji.schema.impl.cassandra.CQLUtils.RAW_KEY_COL;
import static org.kiji.schema.impl.cassandra.CQLUtils.VERSION_COL;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.KijiTableLayout;

/**
 *
 */
public class StatementBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(StatementBuilder.class);

  private static enum ColumnType {
    BYTES, STRING, INT, LONG, COUNTER
  }

  private final KijiTableLayout mLayout;

  /** All of the following maps rely on the ordering guarantees of ImmutableMap. */

  /**
   * The columns contained in the Cassandra partition key. Corresponds to Kiji hashed entity ID
   * components.
   */
  private final ImmutableMap<String, ColumnType> mPartitionKeyColumns;

  /**
   * The columns contained in the Kiji Entity ID.
   */
  private final ImmutableMap<String, ColumnType> mEntityIdColumns;

  public StatementBuilder(final KijiTableLayout layout) {
    mLayout = layout;

    final ImmutableMap.Builder<String, ColumnType> columns = ImmutableMap.builder();

    RowKeyFormat2 keyFormat = (RowKeyFormat2) mLayout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: {
        columns.put(RAW_KEY_COL, ColumnType.BYTES);
        mPartitionKeyColumns = columns.build();
        mEntityIdColumns = mPartitionKeyColumns;
        break;
      }
      case FORMATTED: {
        final List<RowKeyComponent> components = keyFormat.getComponents();

        for (RowKeyComponent partitionKeyColumn
            : components.subList(0, keyFormat.getRangeScanStartIndex())) {
          columns.put(
              translateEntityIDComponentNameToColumnName(partitionKeyColumn.getName()),
              getCQLType(partitionKeyColumn.getType()));
        }
        mPartitionKeyColumns = columns.build();

        for (RowKeyComponent partitionKeyColumn
            : components.subList(
                keyFormat.getRangeScanStartIndex(),
                keyFormat.getComponents().size())) {
          columns.put(
              translateEntityIDComponentNameToColumnName(partitionKeyColumn.getName()),
              getCQLType(partitionKeyColumn.getType()));
        }
        mEntityIdColumns = columns.build();
        break;
      }
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }

  public Statement getColumnGetStatement(
      CassandraTableName table,
      EntityId entityId,
      CassandraColumnName column,
      KijiDataRequest dataRequest,
      Column columnRequest
  ) {
    Preconditions.checkArgument(table.isLocalityGroup());
    Preconditions.checkArgument(column.containsFamily());

    Select select =
        select()
            .all()
            .from(table.getKeyspace(), table.getUnquotedTable())
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

    for (Map.Entry<String, ColumnType> entityIdColumn : mEntityIdColumns.entrySet()) {


    }

    return select;


  }


  /**
   * Translates an EntityID ComponentType into a CQL type.
   *
   * @param type of entity id component to get CQL type for.
   * @return the CQL type of the provided ComponentType.
   */
  private static ColumnType getCQLType(ComponentType type) {
    switch (type) {
      case INTEGER:
        return ColumnType.INT;
      case LONG:
        return ColumnType.LONG;
      case STRING:
        return ColumnType.STRING;
      default:
        throw new IllegalArgumentException();
    }
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


}
