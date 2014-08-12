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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;

/**
 * Contains code common to a TableWriter and BufferedWriter.
 */
class CassandraKijiWriterCommon {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiWriterCommon.class);

  private final CassandraKijiTable mTable;

  /**
   * Create an object for performing common write operations for a given table.
   *
   * @param table to which to write.
   */
  public CassandraKijiWriterCommon(CassandraKijiTable table) {
    mTable = table;
  }

  /**
   * Check whether a given table contains a counter.
   *
   * @param family of the column to check.
   * @param qualifier of the column to check.
   * @return whether the column contains a counter.
   * @throws java.io.IOException if there is a problem reading the table layout.
   */
  public boolean isCounterColumn(final String family, final String qualifier) throws IOException {
    return mTable.getLayout().getCellSpec(KijiColumnName.create(family, qualifier)).isCounter();
  }

  /**
   * Get the TTL for a column family.
   *
   * @param layout of the table.
   * @param family for which to get the TTL.
   * @return the TTL.
   */
  private static int getTTL(final KijiTableLayout layout, final String family) {
    // Get the locality group name from the column name.
    return layout
      .getFamilyMap()
      .get(family)
      .getLocalityGroup()
      .getDesc()
      .getTtlSeconds();
  }

  /**
   * Create a (bound) CQL statement that implements a Kiji put into a non-counter cell.
   *
   * @param entityId The entity ID of the destination cell.
   * @param family The column family of the destination cell.
   * @param qualifier The column qualifier of the destination cell.
   * @param timestamp The timestamp of the destination cell.
   * @param value The bytes to be written to the destination cell.
   * @param <T> the type of the value to put.
   * @param encoderProvider for the value to put.
   * @return A CQL `Statement` that implements the put.
   * @throws java.io.IOException If something goes wrong (e.g., the column does not exist).
   */
  public <T> Statement getPutStatement(
      final CellEncoderProvider encoderProvider,
      final EntityId entityId,
      final String family,
      final String qualifier,
      long timestamp,
      final T value
  ) throws IOException {
    Preconditions.checkArgument(!isCounterColumn(family, qualifier));

    // In Cassandra Kiji, a write to HConstants.LATEST_TIMESTAMP should be a write with the
    // current system time.
    if (timestamp == HConstants.LATEST_TIMESTAMP) {
      timestamp = System.currentTimeMillis();
    }

    final KijiTableLayout layout = mTable.getLayout();

    int ttl = getTTL(layout, family);

    final KijiColumnName columnName = KijiColumnName.create(family, qualifier);
    final CassandraColumnName cassandraColumn =
        mTable.getColumnNameTranslator().toCassandraColumnName(columnName);

    final ByteBuffer valueBytes =
        CassandraByteUtil.bytesToByteBuffer(
            encoderProvider.getEncoder(family, qualifier).encode(value));

    final CassandraTableName table =
        CassandraTableName.getLocalityGroupTableName(
            mTable.getURI(),
            columnName,
            layout);

    return CQLUtils.getInsertStatement(
        layout,
        table,
        entityId,
        cassandraColumn,
        timestamp,
        valueBytes,
        ttl);
  }

  /**
   * Create a delete statement for a fully-qualified cell.
   *
   * @param entityId of the cell to delete.
   * @param family of the cell to delete.
   * @param qualifier of the cell to delete.
   * @param version of the cell to delete.
   * @return a statement that will delete the cell.
   * @throws java.io.IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteCellStatement(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final long version
  ) throws IOException {
    checkFamily(family);

    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    final CassandraColumnName cassandraColumn =
        mTable.getColumnNameTranslator().toCassandraColumnName(column);

    final KijiTableLayout layout = mTable.getLayout();
    final CassandraTableName table =
        CassandraTableName.getLocalityGroupTableName(mTable.getURI(), column, layout);

    return CQLUtils.getDeleteCellStatement(
        layout,
        table,
        entityId,
        cassandraColumn,
        version);
  }

  /**
   * Create a delete statement for the latest version of a cell.
   *
   * @param entityId of the cell to delete.
   * @param family of the cell to delete.
   * @param qualifier of the cell to delete.
   * @return a statement that will delete the cell.
   * @throws java.io.IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteColumnStatement(
      final EntityId entityId,
      final String family,
      final String qualifier
  ) throws IOException {
    checkFamily(family);

    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    final CassandraColumnName cassandraColumn =
        mTable.getColumnNameTranslator().toCassandraColumnName(column);

    final KijiTableLayout layout = mTable.getLayout();
    final CassandraTableName table =
        CassandraTableName.getLocalityGroupTableName(mTable.getURI(), column, layout);

    return CQLUtils.getDeleteColumnStatement(
        layout,
        table,
        entityId,
        cassandraColumn);
  }

  /**
   * Create a delete statement for a cell containing a counter.
   *
   * @param entityId of the cell to delete.
   * @param family of the cell to delete.
   * @param qualifier of the cell to delete.
   * @return a statement that will delete the cell.
   * @throws java.io.IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteCounterStatement(
      final EntityId entityId,
      final String family,
      final String qualifier
  ) throws IOException {
    checkFamily(family);

    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    final CassandraColumnName cassandraColumn =
        mTable.getColumnNameTranslator().toCassandraColumnName(column);

    final CassandraTableName table =
        CassandraTableName.getCounterTableName(mTable.getURI());

    return CQLUtils.getDeleteColumnStatement(
        mTable.getLayout(),
        table,
        entityId,
        cassandraColumn);
  }

  /**
   * Create a delete statement for a column family.
   *
   * @param entityId to delete.
   * @param family to delete.
   * @return a statement that will delete the family.
   * @throws java.io.IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteFamilyStatement(
      final EntityId entityId,
      final String family
  ) throws IOException {
    checkFamily(family);

    final KijiColumnName column = KijiColumnName.create(family, null);
    final CassandraColumnName cassandraColumn =
        mTable.getColumnNameTranslator().toCassandraColumnName(column);

    final KijiTableLayout layout = mTable.getLayout();
    final CassandraTableName table =
        CassandraTableName.getLocalityGroupTableName(mTable.getURI(), column, layout);

    return CQLUtils.getDeleteColumnStatement(
        layout,
        table,
        entityId,
        cassandraColumn);
  }

  /**
   * Create a delete statement for a column family containing counters.
   *
   * @param entityId to delete.
   * @param family to delete.
   * @return a statement that will delete the family.
   * @throws java.io.IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteCounterFamilyStatement(
      final EntityId entityId,
      final String family
  ) throws IOException {
    checkFamily(family);

    final KijiColumnName column = KijiColumnName.create(family, null);
    final CassandraColumnName cassandraColumn =
        mTable.getColumnNameTranslator().toCassandraColumnName(column);

    final CassandraTableName table =
        CassandraTableName.getCounterTableName(mTable.getURI());

    return CQLUtils.getDeleteColumnStatement(
        mTable.getLayout(),
        table,
        entityId,
        cassandraColumn
    );
  }

  /**
   * Create a delete statement for an entire row.
   *
   * @param entityId of the row to delete.
   * @return a statement that will delete the row.
   * @throws java.io.IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteRowStatement(final EntityId entityId) throws IOException {
    return CQLUtils.getDeleteRowStatement(mTable.getLayout(), mTableName, entityId);
  }

  /**
   * Create a delete statement for the counters within a row.
   *
   * @param entityId of the row to delete.
   * @return a statement that will delete the row.
   * @throws java.io.IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteCounterRowStatement(final EntityId entityId) throws IOException {
    return CQLUtils.getDeleteRowStatement(mTable.getLayout(), mCounterTableName, entityId);
  }

  /**
   * Checks that the provided column family exists in this table.
   *
   * @param family to check.
   * @throws NoSuchColumnException if the family does not exist.
   */
  private void checkFamily(final String family) throws NoSuchColumnException {
    if (!mTable.getLayout().getFamilyMap().containsKey(family)) {
      throw new NoSuchColumnException(String.format("Family '%s' not found.", family));
    }
  }
}
