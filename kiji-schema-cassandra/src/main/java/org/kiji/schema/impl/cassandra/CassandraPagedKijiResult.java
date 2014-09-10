package org.kiji.schema.impl.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;

import com.datastax.driver.core.Statement;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiResult;
import org.kiji.schema.cassandra.CassandraTableName;
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
  private final Closer mCloser;

  /**
   * This result does not need to be closed unless {@link #iterator()} is called, so we defer
   * registering with the debug resource tracker till that point. This variable keeps track of
   * whether we have registered yet. Does not need to be atomic, because this class is not thread
   * safe.
   */
  private boolean mDebugRegistered = false;

  public CassandraPagedKijiResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final CassandraKijiTable table,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider,
      final SortedMap<KijiColumnName, Iterable<KijiCell<T>>> columnResults
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mTable = table;
    mLayout = layout;
    mColumnTranslator = columnTranslator;
    mDecoderProvider = decoderProvider;
    mColumnResults = columnResults;
    mCloser = Closer.create();
  }

  @Override
  public EntityId getEntityId() {
    return null;
  }

  @Override
  public KijiDataRequest getDataRequest() {
    return null;
  }

  @Override
  public Iterator<KijiCell<T>> iterator() {
    return null;
  }

  @Override
  public <U extends T> KijiResult<U> narrowView(final KijiColumnName column) {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * An iterable which starts a Cassandra scan for each requested iterator.
   */
  private final class PagedColumnIterable implements Iterable<KijiCell<T>>, Closeable {
    private final Column mColumnRequest;

    /**
     * Creates an iterable which starts an HBase scan for each requested iterator.
     *
     * @param columnRequest of column to scan.
     */
    private PagedColumnIterable(final Column columnRequest) {
      mColumnRequest = columnRequest;
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mCloser.close();
    }
  }
}
