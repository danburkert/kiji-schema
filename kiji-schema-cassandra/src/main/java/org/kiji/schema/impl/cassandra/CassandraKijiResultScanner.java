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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.mortbay.io.RuntimeIOException;

import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiResultScanner;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.impl.cassandra.RowDecoders.TokenRowKeyComponents;
import org.kiji.schema.impl.cassandra.RowDecoders.TokenRowKeyComponentsComparator;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * {@inheritDoc}
 *
 * Cassandra implementation of {@code KijiResultScanner}.
 *
 * @param <T> type of {@code KijiCell} value returned by scanned {@code KijiResult}s.
 */
public class CassandraKijiResultScanner<T> implements KijiResultScanner<T> {
  private final Iterator<KijiResult<T>> mIterator;

 /*
  * ## Implementation Notes
  *
  * To perform a scan over a Kiji table, Cassandra Kiji creates an entityID scan over all locality
  * group tables in the scan, and then for each entity Id, creates a separate KijiResult. Creating
  * each Kiji result requires more requests to create the paged and non-paged columns.
  *
  * TODO: we could optimize this by using table scans to pull in the materialized rows as part of a
  * scan instead of issuing separate get requests for each row.  However, it is not clear that this
  * will be faster given the idiosyncrasies of Cassandra scans.
  */

  public CassandraKijiResultScanner(
      final KijiDataRequest request,
      final CassandraKijiTable table,
      final KijiTableLayout layout,
      final CellDecoderProvider decoderProvider,
      final CassandraColumnNameTranslator translator
  ) throws IOException {

    final Set<ColumnId> localityGroups = Sets.newHashSet();

    for (final Column columnRequest : request.getColumns()) {
      final ColumnId localityGroupId =
          layout.getFamilyMap().get(columnRequest.getFamily()).getLocalityGroup().getId();
      localityGroups.add(localityGroupId);
    }

    final List<ListenableFuture<Iterator<TokenRowKeyComponents>>> rowKeyStreamFutures =
        Lists.newArrayList();

    final KijiURI tableURI = table.getURI();
    for (final ColumnId localityGroup : localityGroups) {
      final CassandraTableName tableName =
          CassandraTableName.getLocalityGroupTableName(tableURI, localityGroup);

      final Statement scan = CQLUtils.getEntityIDScanStatement(layout, tableName);
      final Function<Row, TokenRowKeyComponents> rowKeyDecoder =
          RowDecoders.getRowKeyDecoderFunction(layout);

      final ListenableFuture<Iterator<TokenRowKeyComponents>> rowKeyStreamFuture =
          Futures.transform(
              table.getAdmin().executeAsync(scan),
              new Function<ResultSet, Iterator<TokenRowKeyComponents>>() {
                @Override
                public Iterator<TokenRowKeyComponents> apply(final ResultSet resultSet) {
                  return Iterators.transform(resultSet.iterator(), rowKeyDecoder);
                }
              });

      rowKeyStreamFutures.add(rowKeyStreamFuture);
    }

    final List<Iterator<TokenRowKeyComponents>> rowKeyStreams =
        Lists.newArrayListWithCapacity(rowKeyStreamFutures.size());

    for (final ListenableFuture<Iterator<TokenRowKeyComponents>> future : rowKeyStreamFutures) {
      rowKeyStreams.add(CassandraKijiResult.unwrapFuture(future));
    }

    final Iterator<TokenRowKeyComponents> rowkeys =
        new DeduplicatingIterator<TokenRowKeyComponents>(
            Iterators.mergeSorted(rowKeyStreams, TokenRowKeyComponentsComparator.INSTANCE));

    mIterator = Iterators.transform(
        rowkeys,
        new Function<TokenRowKeyComponents, KijiResult<T>>() {
          @Override
          public KijiResult<T> apply(final TokenRowKeyComponents rowkey) {
            try {
              return CassandraKijiResult.create(
                  rowkey.getComponents().getEntityIdForTable(table),
                  request,
                  table,
                  layout,
                  translator,
                  decoderProvider);
            } catch (IOException e) {
              throw new RuntimeIOException(e);
            }
          }
        });
  }


  @Override
  public void close() throws IOException {
  }

  @Override
  public boolean hasNext() {
    return mIterator.hasNext();
  }

  @Override
  public KijiResult<T> next() {
    return mIterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Wraps an existing iterator and removes consecutive duplicate elements.
   *
   * TODO: replace this with the version from kiji-commons
   *
   * @param iterator The iterator to deduplicate.
   * @param <T> The value type of the iterator.
   * @return An iterator which lazily deduplicates elements.
   */
  public static <T> Iterator<T> deduplicatingIterator(final Iterator<T> iterator) {
    return new DeduplicatingIterator<T>(iterator);
  }

  /**
   * A deduplicating iterator which removes consecutive duplicate elements from another iterator.
   *
   * TODO: replace this with the version from kiji-commons
   *
   * @param <T> The value type of elements in the iterator.
   */
  @NotThreadSafe
  private static final class DeduplicatingIterator<T> extends UnmodifiableIterator<T> {
    private final PeekingIterator<T> mIterator;

    /**
     * Create an iterator which will remove consecutive duplicate elements in an iterator.
     *
     * @param iterator The iterator to deduplicate.
     */
    private DeduplicatingIterator(final Iterator<T> iterator) {
      mIterator = Iterators.peekingIterator(iterator);
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return mIterator.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public T next() {
      final T next = mIterator.next();

      while (mIterator.hasNext() && next.equals(mIterator.peek())) {
        mIterator.next();
      }
      return next;
    }
  }
}
