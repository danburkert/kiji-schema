package org.kiji.schema.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiResultIterator;

/**
 * A {@link KijiResult} implementation of a single, materialized column.  The column may be fully
 * qualified, or may be an unqualified column family.
 */
public final class MaterializedKijiResult implements KijiResult {
  private static final Logger LOG = LoggerFactory.getLogger(MaterializedKijiResult.class);

  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;
  private final KijiColumnName mColumnName;
  private final List<KijiCell<Object>> mCells;

  public MaterializedKijiResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final KijiColumnName columnName,
      final List<KijiCell<Object>> cells
  ) {
    mEntityId = Preconditions.checkNotNull(entityId);
    mDataRequest = Preconditions.checkNotNull(dataRequest);
    mColumnName = Preconditions.checkNotNull(columnName);
    mCells = Preconditions.checkNotNull(cells);
  }

  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  @Override
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /**
   * Returns the list of cells held by this KijiResult in the requested column.
   *
   * @param column being requested.
   * @return the list of cells held by this KijiResult in the column.
   */
  private List<KijiCell<Object>> getCells(final KijiColumnName column) {
    Preconditions.checkNotNull(column);
    if (mColumnName.isFullyQualified()) {
      if (!mColumnName.equals(column)) {
        // Throwing a NPE here is nonsensical, but the spec requires it
        throw new NullPointerException(
            String.format("Column %s is not included in the data request.", column));
      }
      // The request is for the qualified column this KijiResult holds.
      return mCells;
    } else if (column.isFullyQualified()) {
      // This KijiResult holds an unqualified column, but the request is for a qualified column.
      // Filter the cells in this KijiResult to just those in the requested column.

      int startIndex = Collections.binarySearch(
          mCells,
          KijiCell.create(column, 0, null),
          QualifierTimestampComparator.get());

      int endIndex = Collections.binarySearch(
          mCells,
          KijiCell.create(column, Long.MAX_VALUE, null),
          QualifierTimestampComparator.get());

      if (startIndex < 0) {
        startIndex = (-startIndex) + 1;
      }

      if (endIndex < 0) {
        endIndex = (-endIndex) + 1;
      }

      return mCells.subList(startIndex, endIndex + 1);
    } else {
      // The request is for the unqualified column this KijiResult holds.
      return mCells;
    }
  }

  @Override
  public <T> KijiCell<T> getMostRecentCell(final KijiColumnName column) {
    return (KijiCell<T>) Iterables.getFirst(getCells(column), null);
  }

  @Override
  public <T> KijiCell<T> getCell(final KijiColumnName column, final long timestamp) {
    if (!mColumnName.getFamily().equals(column.getFamily())
        || (mColumnName.isFullyQualified()
            && !mColumnName.getQualifier().equals(column.getQualifier()))) {
      // Throwing a NPE here is nonsensical, but the spec requires it
      throw new NullPointerException(
          String.format("Column %s is not included in the data request.", column));
    }

    final int index = Collections.binarySearch(
        mCells,
        KijiCell.create(column, timestamp, null),
        QualifierTimestampComparator.get());

    if (index > 0) {
      return (KijiCell<T>) mCells.get(index);
    } else {
      return null;
    }
  }

  @Override
  public <T> KijiResultIterator<T> iterator(final KijiColumnName column) {
    return new MaterializedKijiResultIterator(getCells(column).iterator());
  }

  @Override
  public KijiResultIterator<Object> iterator() {
    return new MaterializedKijiResultIterator<Object>(mCells.iterator());
  }

  /**
   * A KijiResultIterator which delegates to another Iterator.
   *
   * @param <T> type of KijiCell being iterated.
   */
  private static final class MaterializedKijiResultIterator<T> implements KijiResultIterator {

    private final Iterator<KijiCell<T>> mIterator;

    /**
     * Create a new delegating KijiResultIterator.
     *
     * @param iterator to delegate to.
     */
    private MaterializedKijiResultIterator(final Iterator<KijiCell<T>> iterator) {
      mIterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return mIterator.hasNext();
    }

    @Override
    public Object next() {
      return mIterator.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("KijiResultIterator does not support #remove().");
    }
  }

  /**
   * A comparator for KijiCells which compares on timestamp.  Should only be applied to a
   * stream of KijiCells which belongs to the same family, and is pre sorted by ascending qualifier
   * and descending timestamp.
   */
  private static final class QualifierTimestampComparator implements Comparator<KijiCell<?>> {
    private static final Comparator<KijiCell<?>> INSTANCE = new QualifierTimestampComparator();

    /**
     * Private constructor.  Use {@link #get()} to retrieve an instance.
     */
    private QualifierTimestampComparator() {
    }

    /**
     * Get a timestamp comparator for {@code KijiCell}s.
     *
     * @return a {@code KijiCell} timestamp comparator.
     */
    public static Comparator<KijiCell<?>> get() {
      return INSTANCE;
    }

    @Override
    public int compare(final KijiCell<?> o1, final KijiCell<?> o2) {
      return ComparisonChain.start()
          .compare(o1.getColumn().getQualifier(), o2.getColumn().getQualifier())
          // TS comparison is reversed to account for the natural descending sort of timestamps.
          .compare(o2.getTimestamp(), o1.getTimestamp())
          .result();
    }
  }
}
