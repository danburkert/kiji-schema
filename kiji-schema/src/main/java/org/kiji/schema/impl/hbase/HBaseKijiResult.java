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

package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiResult;
import org.kiji.schema.impl.hbase.result.ColumnIterables;
import org.kiji.schema.impl.hbase.result.ResultDecoders;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.util.CloseableIterable;
import org.kiji.schema.util.DebugResourceTracker;

/**
 * Abstract implementation of the KijiResult API.
 *
 * <p>
 *   This class simplifies implementing the {@code KijiResult} API by translating simple-to-use
 *   client API calls into simple-to-implement abstract method calls.
 * </p>
 *
 * {@link org.kiji.schema.KijiResult}s by sharing common logic.
 * @param <T> type of {@code KijiCell} value returned by this {@code KijiRowView}.
 */
@ApiAudience.Private
public final class HBaseKijiResult<T> implements KijiResult<T> {

  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;
  private final SortedMap<KijiColumnName, List<KijiCell<T>>> mUnpagedColumns;
  private final SortedMap<KijiColumnName, Iterable<KijiCell<T>>> mPagedColumns;
  private final HBaseColumnNameTranslator mColumnTranslator;
  private final KijiTableLayout mLayout;
  private final CellDecoderProvider mDecoderProvider;
  private final HBaseKijiTable mTable;
  private final Closer mCloser;
  private final AtomicBoolean mIsClosed;

  /**
   * Initialize a new {@link HBaseKijiResult}.
   *
   * @param entityId EntityId of the row from which to read cells.
   * @param dataRequest KijiDataRequest defining the values to retrieve.
   * @param unpagedColumns materialized column cells.
   * @param columnTranslator for the table.
   * @param layout of the table.
   * @param decoderProvider for the table.
   * @param table being viewed.
   */
  private HBaseKijiResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final SortedMap<KijiColumnName, List<KijiCell<T>>> unpagedColumns,
      final HBaseColumnNameTranslator columnTranslator,
      final KijiTableLayout layout,
      final CellDecoderProvider decoderProvider,
      final HBaseKijiTable table
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mUnpagedColumns = unpagedColumns;
    mColumnTranslator = columnTranslator;
    mLayout = layout;
    mDecoderProvider = decoderProvider;
    mTable = table;
    mCloser = Closer.create();

    final ImmutableSortedMap.Builder<KijiColumnName, Iterable<KijiCell<T>>> pagedColumns =
        ImmutableSortedMap.naturalOrder();
    for (Column columnRequest : dataRequest.getColumns()) {
      if (columnRequest.isPagingEnabled()) {
        final CloseableIterable<KijiCell<T>> pagedColumn = ColumnIterables.getPagedColumnIterable(
            entityId,
            columnRequest,
            dataRequest.getMinTimestamp(),
            dataRequest.getMaxTimestamp(),
            columnTranslator,
            layout,
            decoderProvider,
            table);
        mCloser.register(pagedColumn);
        pagedColumns.put(columnRequest.getColumnName(), pagedColumn);
      }
    }
    mPagedColumns = pagedColumns.build();
    if (!mPagedColumns.isEmpty()) {
      DebugResourceTracker.get().registerResource(this);
    }
    mIsClosed = new AtomicBoolean(false);
  }

  /**
   * Create a new {@link HBaseKijiResult}.
   *
   * <p>
   *   This constructor function does processing of an HBase Result once, so that narrowed views
   *   can share the results.
   * </p>
   *
   * @param entityId EntityId of the row from which to read cells.
   * @param dataRequest KijiDataRequest defining the values to retrieve.
   * @param unpagedRawResult the result from a get of the unpaged columns.
   * @param layout of the table.
   * @param columnTranslator for the table.
   * @param decoderProvider for the table.
   * @param table being viewed.
   * @param <T> type of value in viewed {@code KijiCell}s.
   * @return an {@code HBaseKijiResult}.
   */
  public static <T> HBaseKijiResult<T> create(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final Result unpagedRawResult,
      final KijiTableLayout layout,
      final HBaseColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider,
      final HBaseKijiTable table
  ) {
    int maxVersions = 1;
    for (Column columnRequest : dataRequest.getColumns()) {
      maxVersions = Math.max(maxVersions, columnRequest.getMaxVersions());
    }

    final ImmutableSortedMap.Builder<KijiColumnName, List<KijiCell<T>>> unpagedResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : dataRequest.getColumns()) {
      final KijiColumnName column = columnRequest.getColumnName();
      if (!columnRequest.isPagingEnabled()) {
        List<KeyValue> rawColumnResults = ColumnIterables.getMaterializedColumnIterable(
            columnRequest,
            columnTranslator,
            maxVersions,
            unpagedRawResult);

        final List<KijiCell<T>> columnResults =
            Lists.transform(
                rawColumnResults,
                ResultDecoders.<T>getDecoderFunction(
                    column,
                    layout,
                    columnTranslator,
                    decoderProvider));

        unpagedResults.put(column, columnResults);
      }
    }

    return new HBaseKijiResult<T>(
        entityId,
        dataRequest,
        unpagedResults.build(),
        columnTranslator,
        layout,
        decoderProvider,
        table);
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<KijiCell<T>> iterator() {
    return
        Iterables.concat(
            Iterables.concat(mUnpagedColumns.values()),
            Iterables.concat(mPagedColumns.values())
        ).iterator();
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <U> KijiResult<U> narrowView(final KijiColumnName column) {
    final KijiDataRequest narrowRequest = narrowRequest(column, mDataRequest);
    if (narrowRequest.equals(mDataRequest)) {
      return (KijiResult<U>) this;
    }

    final ImmutableSortedMap.Builder<KijiColumnName, List<KijiCell<U>>> unpagedResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : narrowRequest.getColumns()) {
      final KijiColumnName requestColumnName = columnRequest.getColumnName();

      if (!columnRequest.isPagingEnabled()) {
        // (Object) cast is necessary. Might be: http://bugs.java.com/view_bug.do?bug_id=6548436
        final List<KijiCell<U>> exactColumn =
            (List<KijiCell<U>>) (Object) mUnpagedColumns.get(requestColumnName);
        if (exactColumn != null) {
          unpagedResults.put(requestColumnName, exactColumn);
        } else {
          // The column request is fully qualified, and the original view contains a request for the
          // column's entire family.
          final List<KijiCell<T>> familyColumn =
              mUnpagedColumns.get(KijiColumnName.create(requestColumnName.getFamily(), null));
          final List<KijiCell<T>> filteredColumnResults =
              getQualifiedColumnCells(requestColumnName, familyColumn);
          unpagedResults.put(requestColumnName, (List<KijiCell<U>>) (Object) filteredColumnResults);
        }
      }
    }

    final HBaseKijiResult<U> narrowView = new HBaseKijiResult<U>(
        mEntityId,
        narrowRequest,
        unpagedResults.build(),
        mColumnTranslator,
        mLayout,
        mDecoderProvider,
        mTable);
    mCloser.register(narrowView);
    return narrowView;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    if (mIsClosed.compareAndSet(false, true) && !mPagedColumns.isEmpty()) {
      DebugResourceTracker.get().unregisterResource(this);
    }
    mCloser.close();
  }

  // -----------------------------------------------------------------------------------------------
  // Helper methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Takes a list of {@code KijiCell}s from a Kiji family and a fully-qualified column name, and
   * returns the sublist of columns which are in the fully-qualified column.
   *
   * @param column to get {@code KijiCell}s for.
   * @param familyCells list of {@code KijiCell}s in the family. Must be sorted by column name.
   * @param <T> type of value in the {@code KijiCell}s.
   * @return a sublist containing the {@code KijiCell}s in the specified column.
   */
  public static <T> List<KijiCell<T>> getQualifiedColumnCells(
      final KijiColumnName column,
      final List<KijiCell<T>> familyCells
  ) {
    final KijiCell<T> start = KijiCell.create(column, Long.MAX_VALUE, null);
    final KijiCell<T> end = KijiCell.create(column, -1, null);

    return ColumnIterables.getSublist(familyCells, KijiCell.getKeyComparator(), start, end);
  }

  /**
   * Narrow a {@link KijiDataRequest} to a column.  Will return a new data request. The column may
   * be fully qualified or a family.
   *
   * @param column to narrow data request.
   * @param dataRequest to narrow.
   * @return a data request narrowed to the specified column.
   */
  public static KijiDataRequest narrowRequest(
      final KijiColumnName column,
      final KijiDataRequest dataRequest
  ) {
    final List<Column> columnRequests = getColumnRequests(column, dataRequest);

    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp());
    for (Column columnRequest : columnRequests) {
      builder.newColumnsDef(columnRequest);
    }

    return builder.build();
  }

  /**
   * Retrieve the column requests corresponding to a Kiji column in a {@code KijiDataRequest}.
   *
   * <p>
   * If the requested column is fully qualified, and the request contains a family request
   * containing the column, a new {@code Column} request will be created which corresponds to
   * the requested family narrowed to the qualifier.
   * </p>
   *
   * @param column a fully qualified {@link KijiColumnName}
   * @param dataRequest the data request to get column request from.
   * @return the column request.
   */
  public static List<Column> getColumnRequests(
      final KijiColumnName column,
      final KijiDataRequest dataRequest
  ) {
    final Column exactRequest = dataRequest.getColumn(column);
    if (exactRequest != null) {
      return ImmutableList.of(exactRequest);
    }

    if (column.isFullyQualified()) {
      // The column is fully qualified, but a request doesn't exist for the qualified column.
      // Check if the family is requested, and if so create a new qualified-column request from it.
      final Column familyRequest =
          dataRequest.getRequestForColumn(KijiColumnName.create(column.getFamily(), null));
      ColumnsDef columnDef = ColumnsDef
          .create()
          .withFilter(familyRequest.getFilter())
          .withPageSize(familyRequest.getPageSize())
          .withMaxVersions(familyRequest.getMaxVersions())
          .add(column.getFamily(), column.getQualifier(), familyRequest.getReaderSpec());

      return ImmutableList.of(
          KijiDataRequest.builder().addColumns(columnDef).build().getColumn(column));
    } else {
      // The column is a family, but a request doesn't exist for the entire family add all requests
      // for individual columns in the family.
      ImmutableList.Builder<Column> columnRequests = ImmutableList.builder();
      for (Column columnRequest : dataRequest.getColumns()) {
        if (columnRequest.getColumnName().getFamily().equals(column.getFamily())) {
          columnRequests.add(columnRequest);
        }
      }
      return columnRequests.build();
    }
  }
}
