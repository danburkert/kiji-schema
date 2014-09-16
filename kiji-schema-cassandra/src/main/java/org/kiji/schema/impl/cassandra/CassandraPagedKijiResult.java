package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiResult;
import org.kiji.schema.impl.DefaultKijiResult;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/**
 *
 */
public class CassandraPagedKijiResult<T> implements KijiResult<T> {
  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;
  private final CassandraKijiTable mTable;
  private final KijiTableLayout mLayout;
  private final CassandraColumnNameTranslator mColumnTranslator;
  private final CellDecoderProvider mDecoderProvider;
  private final SortedMap<KijiColumnName, Iterable<KijiCell<T>>> mColumnResults;

  public CassandraPagedKijiResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final CassandraKijiTable table,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mTable = table;
    mLayout = layout;
    mColumnTranslator = columnTranslator;
    mDecoderProvider = decoderProvider;

    final ImmutableSortedMap.Builder<KijiColumnName, Iterable<KijiCell<T>>> columnResults =
        ImmutableSortedMap.naturalOrder();

    for (Column columnRequest : mDataRequest.getColumns()) {
      final PagedColumnIterable columnIterable = new PagedColumnIterable(columnRequest);
      columnResults.put(columnRequest.getColumnName(), columnIterable);
    }

    mColumnResults = columnResults.build();
  }

  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  @Override
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  @Override
  public Iterator<KijiCell<T>> iterator() {
    return Iterables.concat(mColumnResults.values()).iterator();
  }

  @Override
  public <U extends T> KijiResult<U> narrowView(final KijiColumnName column) {
    final KijiDataRequest narrowRequest = DefaultKijiResult.narrowRequest(column, mDataRequest);

    return new CassandraPagedKijiResult<U>(
        mEntityId,
        narrowRequest,
        mTable,
        mLayout,
        mColumnTranslator,
        mDecoderProvider);
  }

  @Override
  public void close() throws IOException { }

  /**
   * An iterable which starts a Cassandra scan for each requested iterator.
   */
  private final class PagedColumnIterable implements Iterable<KijiCell<T>> {
    private final Column mColumnRequest;

    /**
     * Creates an iterable which starts an HBase scan for each requested iterator.
     *
     * @param columnRequest of column to scan.
     */
    private PagedColumnIterable(final Column columnRequest) {
      mColumnRequest = columnRequest;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<KijiCell<T>> iterator() {
      return CassandraKijiResult.unwrapFuture(
          CassandraKijiResult.<T>getColumn(
              mTable.getURI(),
              mEntityId,
              mColumnRequest,
              mDataRequest,
              mLayout,
              mColumnTranslator,
              mDecoderProvider,
              mTable.getAdmin()));
    }
  }
}
