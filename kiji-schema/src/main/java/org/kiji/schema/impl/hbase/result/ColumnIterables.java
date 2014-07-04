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

package org.kiji.schema.impl.hbase.result;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.filter.KijiColumnFilter;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.hbase.HBaseDataRequestAdapter.NameTranslatingFilterContext;
import org.kiji.schema.impl.hbase.HBaseKijiTable;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.util.CloseableIterable;

/**
 * Provides iterables over Kiji table columns.
 */
@ApiAudience.Private
public final class ColumnIterables {

  static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * Get the list of {@code KeyValue}s in a {@code Result} belonging to a column request.
   *
   * <p>
   *   This method will filter extra version from the result if the number requested versions
   *   ({@code requestMaxVersions}) is greater than the column's requested max versions.
   * </p>
   *
   * @param columnRequest of the column whose {@code KeyValues} to return.
   * @param translator for the table.
   * @param requestMaxVersions number of versions requested for the entire scan.
   * @param result the scan results.
   * @return the {@code KeyValue}s for the column.
   */
  public static List<KeyValue> getMaterializedColumnIterable(
      final Column columnRequest,
      final HBaseColumnNameTranslator translator,
      final int requestMaxVersions,
      final Result result
  ) {
    final KijiColumnName column = columnRequest.getColumnName();

    if (column.isFullyQualified()) {
      return getQualifiedColumnKeyValues(columnRequest, translator, requestMaxVersions, result);
    } else {
      return getFamilyKeyValues(columnRequest, translator, result);
    }
  }

  // CSOFF: ParameterNumberCheck
  /**
   * Returns an iterable which starts an HBase scan for each requested iterator.
   *
   * @param entityId of row.
   * @param columnRequest of column to scan.
   * @param minTimestamp of scan.
   * @param maxTimestamp of scan.
   * @param translator of table.
   * @param layout of table.
   * @param decoderProvider of table.
   * @param table to create scan with.
   * @param <T> type of value in column
   * @return an iterable which creates a scan over the column per iterator.
   */
  public static <T> CloseableIterable<KijiCell<T>> getPagedColumnIterable(
      final EntityId entityId,
      final Column columnRequest,
      final long minTimestamp,
      final long maxTimestamp,
      final HBaseColumnNameTranslator translator,
      final KijiTableLayout layout,
      final CellDecoderProvider decoderProvider,
      final HBaseKijiTable table
  ) {
    return new PagedColumnIterable<T>(
        entityId,
        columnRequest,
        minTimestamp,
        maxTimestamp,
        translator,
        layout,
        decoderProvider,
        table);
  }
  // CSON

  /**
   * Get the list of {@code KeyValue}s in the {@code Result} belonging to a fully-qualified column
   * request.
   *
   * <p>
   *   This method will filter extra versions from the result if the number of requested versions
   *   ({@code requestMaxVersions}) is greater than the column's requested max versions.
   * </p>
   *
   * @param columnRequest of the column whose {@code KeyValues} to return.
   * @param translator for the table.
   * @param requestMaxVersions number of versions requested for the entire scan.
   * @param result the scan results.
   * @return the {@code KeyValue}s for the qualified column.
   */
  private static List<KeyValue> getQualifiedColumnKeyValues(
      final Column columnRequest,
      final HBaseColumnNameTranslator translator,
      final int requestMaxVersions,
      final Result result
  ) {
    final KeyValue[] raw = result.raw();
    if (raw.length == 0) {
      return ImmutableList.of();
    }
    final byte[] rowkey = raw[0].getRow();
    final byte[] family;
    final byte[] qualifier;
    try {
      final HBaseColumnName hbaseColumn =
          translator.toHBaseColumnName(columnRequest.getColumnName());
      family = hbaseColumn.getFamily();
      qualifier = hbaseColumn.getQualifier();
    } catch (NoSuchColumnException e) {
      throw new IllegalArgumentException(e);
    }

    final KeyValue start = new KeyValue(rowkey, family, qualifier, Long.MAX_VALUE, EMPTY_BYTES);
    // HBase will never return a KeyValue with a negative timestamp, so -1 is fine for exclusive end
    final KeyValue end = new KeyValue(rowkey, family, qualifier, -1, EMPTY_BYTES);

    final List<KeyValue> familyKeyValues =
        getSublist(Arrays.asList(raw), KeyValue.COMPARATOR, start, end);

    final int maxVersions = Math.min(columnRequest.getMaxVersions(), requestMaxVersions);
    if (familyKeyValues.size() > maxVersions) {
      return familyKeyValues.subList(0, maxVersions);
    } else {
      return familyKeyValues;
    }
  }

  /**
   * Get the list of {@code KeyValue}s in a {@code Result} belonging to a family request.
   *
   * <p>
   *   This method will filter extra versions from each column if necessary.
   * </p>
   *
   * @param familyRequest of family whose {@code KeyValues} to return.
   * @param translator for the table.
   * @param result the scan results.
   * @return the {@code KeyValue}s for the specified family.
   */
  private static List<KeyValue> getFamilyKeyValues(
      final Column familyRequest,
      final HBaseColumnNameTranslator translator,
      final Result result
  ) {
    final KijiColumnName column = familyRequest.getColumnName();
    final List<KeyValue> raw = Arrays.asList(result.raw());
    if (raw.size() == 0) {
      return ImmutableList.of();
    }
    final byte[] rowkey = raw.get(0).getRow();
    final byte[] family;
    byte[] qualifier;
    try {
      final HBaseColumnName hbaseColumn =
          translator.toHBaseColumnName(familyRequest.getColumnName());
      family = hbaseColumn.getFamily();
      qualifier = hbaseColumn.getQualifier();
    } catch (NoSuchColumnException e) {
      throw new IllegalArgumentException(e);
    }

    List<KeyValue> keyValues = Lists.newArrayList();

    final KeyValue familyStartKV = new KeyValue(rowkey, family, qualifier, 0, EMPTY_BYTES);

    // Index of the current qualified column index
    int columnStart = getElementIndex(result.raw(), 0, familyStartKV);
    while (columnStart < raw.size()
      && column.getFamily().equals(getKeyValueColumnName(raw.get(columnStart), translator)
            .getFamily())) {

      final KeyValue start = raw.get(columnStart);
      final KeyValue end = new KeyValue(
          start.getRow(),
          start.getFamily(),
          Arrays.copyOf(start.getQualifier(),
          start.getQualifier().length + 1));
      final int columnEnd = getElementIndex(result.raw(), columnStart, end);

      final int length = Math.min(columnEnd - columnStart, familyRequest.getMaxVersions());

      keyValues.addAll(raw.subList(columnStart, columnStart + length));
      columnStart = columnEnd;
    }

    return keyValues;
  }

  /**
   * Get the KijiColumnName encoded in the Key of a given KeyValue.
   *
   * @param kv KeyValue from which to get the encoded KijiColumnName.
   * @param translator for table.
   * @return the KijiColumnName encoded in the Key of a given KeyValue.
   */
  private static KijiColumnName getKeyValueColumnName(
      final KeyValue kv,
      final HBaseColumnNameTranslator translator
  ) {
    final HBaseColumnName hBaseColumnName = new HBaseColumnName(kv.getFamily(), kv.getQualifier());
    try {
      return translator.toKijiColumnName(hBaseColumnName);
    } catch (NoSuchColumnException nsce) {
      // This should not happen since it's only called on data returned by HBase.
      throw new IllegalStateException(
          String.format("Unknown column name in KeyValue: %s.", kv));
    }
  }

  /**
   * Get the index of a {@code KeyValue} in an array.
   *
   * @param array to search for the {@code KeyValue}. Must be sorted.
   * @param start index to start search for the {@code KeyValue}.
   * @param element to search for.
   * @return the index that the element resides, or would reside if it were to be inserted into the
   *     sorted array.
   */
  public static int getElementIndex(
      final KeyValue[] array,
      final int start,
      final KeyValue element
  ) {
    final int index = Arrays.binarySearch(array, start, array.length, element, KeyValue.COMPARATOR);
    if (index < 0) {
      return -1 - index;
    } else {
      return index;
    }
  }

  /**
   * Get a sublist of a list starting at the {@code start} element, and extending to the {@code end}
   * element.
   *
   * @param list from which to take the sublist. Must be sorted.
   * @param comparator which the list is sorted with.
   * @param start element for the sublist (inclusive).
   * @param end element for the sublist (exclusive).
   * @param <T> type of list element.
   * @return the sublist from the provided sorted list which contains the {@code start} element and
   *    excludes the {@code end} element.
   */
  public static <T> List<T> getSublist(
      final List<T> list,
      final Comparator<? super T> comparator,
      final T start,
      final T end
  ) {
    int startIndex = Collections.binarySearch(list, start, comparator);
    if (startIndex < 0) {
      startIndex = -1 - startIndex;
    }
    int endIndex = Collections.binarySearch(list, end, comparator);
    if (endIndex < 0) {
      endIndex = -1 - endIndex;
    }

    return list.subList(startIndex, endIndex);
  }

  /**
   * An iterable which starts an HBase scan for each requested iterator.
   *
   * @param <T> type of value in column
   */
  public static final class PagedColumnIterable<T> implements CloseableIterable<KijiCell<T>> {
    private final KijiColumnName mColumn;
    private final Scan mScan;
    private final CellDecoderProvider mDecoderProvider;
    private final KijiTableLayout mLayout;
    private final HBaseColumnNameTranslator mTranslator;
    private final HBaseKijiTable mTable;
    private final Closer mCloser;

    // CSOFF: ParameterNumberCheck
    /**
     * Creates an iterable which starts an HBase scan for each requested iterator.
     *
     * @param entityId of row.
     * @param columnRequest of column to scan.
     * @param minTimestamp of scan.
     * @param maxTimestamp of scan.
     * @param translator of table.
     * @param layout of table.
     * @param decoderProvider of table.
     * @param table to create scan with.
     */
    private PagedColumnIterable(
        final EntityId entityId,
        final Column columnRequest,
        final long minTimestamp,
        final long maxTimestamp,
        final HBaseColumnNameTranslator translator,
        final KijiTableLayout layout,
        final CellDecoderProvider decoderProvider,
        final HBaseKijiTable table
    ) {
      mColumn = columnRequest.getColumnName();
      mDecoderProvider = decoderProvider;
      mLayout = layout;
      mTranslator = translator;
      mTable = table;
      mCloser = Closer.create();

      try {
        final KijiColumnFilter.Context filterContext =
            new NameTranslatingFilterContext(mTranslator);
        final KijiColumnFilter kijiFilter = columnRequest.getFilter();
        final Filter filter;
        if (kijiFilter != null) {
          filter = kijiFilter.toHBaseFilter(mColumn, filterContext);
        } else {
          filter = null;
        }

        final byte[] rowkey = entityId.getHBaseRowKey();
        mScan = new Scan(rowkey, Arrays.copyOf(rowkey, rowkey.length + 1));

        final HBaseColumnName hbaseColumn = mTranslator.toHBaseColumnName(mColumn);

        if (mColumn.isFullyQualified()) {
          mScan.addColumn(hbaseColumn.getFamily(), hbaseColumn.getQualifier());
          mScan.setFilter(filter);
        } else {
          if (Arrays.equals(hbaseColumn.getQualifier(), new byte[0])) {
            // This can happen with the native translator
            mScan.addFamily(hbaseColumn.getFamily());
            mScan.setFilter(filter);
          } else if (hbaseColumn.getQualifier().length == 0) {
            mScan.addFamily(hbaseColumn.getFamily());
            mScan.setFilter(filter);
          }
            mScan.addFamily(hbaseColumn.getFamily());

          final Filter prefixFilter = new ColumnPrefixFilter(hbaseColumn.getQualifier());
          if (filter != null) {
            final FilterList filters = new FilterList(Operator.MUST_PASS_ALL);
            filters.addFilter(prefixFilter);
            filters.addFilter(filter);
            mScan.setFilter(filters);
          } else {
            mScan.setFilter(prefixFilter);
          }
        }

        mScan.setMaxVersions(columnRequest.getMaxVersions());
        mScan.setTimeRange(minTimestamp, maxTimestamp);
        mScan.setBatch(columnRequest.getPageSize());
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
    // CSOFF

    /** {@inheritDoc} */
    @Override
    public Iterator<KijiCell<T>> iterator() {
      final HTableInterface htable;
      final ResultScanner scanner;
      try {
        htable = mTable.openHTableConnection();
        scanner = htable.getScanner(mScan);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
      mCloser.register(htable);
      mCloser.register(scanner);

      final Function<KeyValue, KijiCell<T>> decoder =
          ResultDecoders.getDecoderFunction(mColumn, mLayout, mTranslator, mDecoderProvider);

      return
          Iterators.concat(
              Iterators.transform(
                  scanner.iterator(),
                  new Function<Result, Iterator<KijiCell<T>>>() {
                    @Override
                    public Iterator<KijiCell<T>> apply(final Result result) {
                      return Iterators.transform(Iterators.forArray(result.raw()), decoder);
                    }
                  }));
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mCloser.close();
    }
  }

  /** private constructor for utility class. */
  private ColumnIterables() {
  }
}
