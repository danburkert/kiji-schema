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
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * Contains code common to a TableWriter and BufferedWriter. This class should only be cached for
 * the lifetime of the table layout (it should be recreated when the table layout changes).
 */
class CassandraKijiWriterCommon {
  private final KijiURI mTableURI;

  private final KijiTableLayout mLayout;

  private final CellEncoderProvider mEncoderProvider;

  private final CassandraColumnNameTranslator mColumnTranslator;

  /**
   * Create an object for performing common write operations for a given table.
   *
   * @param layout The layout of the table to which to write.
   */
  public CassandraKijiWriterCommon(
      final KijiURI tableURI,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator columnTranslator,
      final CellEncoderProvider encoderProvider
  ) {
    mTableURI = tableURI;
    mLayout = layout;
    mColumnTranslator = columnTranslator;
    mEncoderProvider = encoderProvider;

    final ImmutableMap.Builder<ColumnId, CassandraTableName> localityGroupTables =
        ImmutableMap.builder();

    for (LocalityGroupLayout localityGroup : mLayout.getLocalityGroups()) {
      final ColumnId localityGroupId = localityGroup.getId();
      final CassandraTableName tableName =
          CassandraTableName.getLocalityGroupTableName(tableURI, localityGroupId);
      localityGroupTables.put(localityGroupId, tableName);
    }
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
    return mLayout.getCellSpec(KijiColumnName.create(family, qualifier)).isCounter();
  }

  /**
   * Get the TTL for a column family.
   *
   * @param family for which to get the TTL.
   * @return the TTL.
   */
  private int getTTL(final String family) {
    // Get the locality group name from the column name.
    return mLayout
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
   * @return A CQL `Statement` that implements the put.
   * @throws java.io.IOException If something goes wrong (e.g., the column does not exist).
   */
  public <T> Statement getPutStatement(
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

    int ttl = getTTL(family);

    final KijiColumnName columnName = KijiColumnName.create(family, qualifier);
    final CassandraColumnName cassandraColumn = mColumnTranslator.toCassandraColumnName(columnName);

    final ByteBuffer valueBytes =
        CassandraByteUtil.bytesToByteBuffer(
            mEncoderProvider.getEncoder(family, qualifier).encode(value));

    final CassandraTableName table =
        CassandraTableName.getLocalityGroupTableName(
            mTableURI,
            columnName,
            mLayout);

    return CQLUtils.getLocalityGroupInsert(
        mLayout,
        table,
        entityId,
        cassandraColumn,
        timestamp,
        valueBytes,
        ttl);
  }
}
