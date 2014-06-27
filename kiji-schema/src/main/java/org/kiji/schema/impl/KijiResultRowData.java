package org.kiji.schema.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiColumnPagingNotEnabledException;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiResultIterator;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.util.TimestampComparator;

/**
 *
 */
public class KijiResultRowData implements KijiRowData {
  private static final Logger LOG = LoggerFactory.getLogger(KijiResultRowData.class);

  private final KijiResult mResult;

  /**
   * A {@code KijiRowData} implementation backed entirely by a {@code KijiResult}.
   *
   * @param result the {@code KijiResult} backing this {@code KijiRowData}.
   */
  public KijiResultRowData(final KijiResult result) {
    mResult = result;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mResult.getEntityId();
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsColumn(final String family, final String qualifier) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    validateColumnRequest(column);

    return mResult.getMostRecentCell(column) != null;
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsColumn(final String family) {
    for (Column column : mResult.getDataRequest().getColumns()) {
      if (column.getFamily().equals(family) && !column.isPagingEnabled()) {
        if (mResult.getMostRecentCell(column.getColumnName()) != null) {
          return true;
        }
      }
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsCell(final String family, final String qualifier, final long timestamp) {
    final KijiDataRequest dataRequest = mResult.getDataRequest();
    return
        (dataRequest.getColumn(KijiColumnName.create(family, qualifier)) != null
            || dataRequest.getColumn(KijiColumnName.create(family, null)) != null)
        && getCell(family, qualifier, timestamp) != null;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableSet<String> getQualifiers(final String family) {
    return getMostRecentValues(family).navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public NavigableSet<Long> getTimestamps(final String family, final String qualifier) {
    return getCells(family, qualifier).navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public Schema getReaderSchema(final String family, final String qualifier) {
    final Column columnRequest = validateColumnRequest(KijiColumnName.create(family, qualifier));
    final ColumnReaderSpec spec = columnRequest.getReaderSpec();
    return spec.getAvroReaderSchema();
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getValue(
      final String family,
      final String qualifier,
      final long timestamp
  ) {
    final KijiCell<T> cell = getCell(family, qualifier, timestamp);
    return cell.getData();
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getMostRecentValue(final String family, final String qualifier) {
    final KijiCell<T> cell = getMostRecentCell(family, qualifier);
    if (cell == null) {
      return null;
    } else {
      return cell.getData();
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, T> getMostRecentValues(final String family) {
    final Collection<Column> columns = mResult.getDataRequest().getColumns();
    final List<KijiColumnName> qualifiedMapColumns = Lists.newArrayListWithCapacity(columns.size());
    KijiColumnName unqualifiedFamily = null;
    for (Column column : columns) {
      if (column.getFamily().equals(family) && !column.isPagingEnabled()) {
        if (column.getQualifier() == null) {
          unqualifiedFamily = column.getColumnName();
          break;
        } else {
          qualifiedMapColumns.add(column.getColumnName());
        }
      }
    }

    final NavigableMap<String, T> qualifiers = Maps.newTreeMap();
    if (unqualifiedFamily == null) {
      // Retrieving a subset of the qualifiers in the family
      for (KijiColumnName column : qualifiedMapColumns) {
        final KijiCell<T> cell = mResult.getMostRecentCell(column);
        qualifiers.put(cell.getColumn().getQualifier(), cell.getData());
      }
    } else {
      // Retrieve all of the qualifiers in the family
      for (KijiColumnName column : qualifiedMapColumns) {
        final KijiResultIterator<T> itr = mResult.iterator(column);
        String lastSeenQualifier = null;
        while (itr.hasNext()) {
          final KijiCell<T> cell = itr.next();
          final String qualifier = cell.getColumn().getQualifier();
          if (!qualifier.equals(lastSeenQualifier)) {
            qualifiers.put(qualifier, cell.getData());
            lastSeenQualifier = qualifier;
          }
        }
      }
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, T>> getValues(
      final String family
  ) {
    final Collection<Column> columns = mResult.getDataRequest().getColumns();
    final List<KijiColumnName> qualifiedMapColumns = Lists.newArrayListWithCapacity(columns.size());
    KijiColumnName unqualifiedFamily = null;
    for (Column column : columns) {
      if (column.getFamily().equals(family) && !column.isPagingEnabled()) {
        if (column.getQualifier() == null) {
          unqualifiedFamily = column.getColumnName();
          break;
        } else {
          qualifiedMapColumns.add(column.getColumnName());
        }
      }
    }

    final NavigableMap<String, NavigableMap<Long, T>> qualifiers = Maps.newTreeMap();
    if (unqualifiedFamily == null) {
      // Retrieving a subset of the qualifiers in the family
      for (KijiColumnName column : qualifiedMapColumns) {
        final NavigableMap<Long, T> values = getValues(column);
        qualifiers.put(column.getQualifier(), values);
      }
    } else {
      // Retrieve all of the qualifiers in the family
      final KijiResultIterator<T> itr = mResult.iterator(unqualifiedFamily);
      while (itr.hasNext()) {
        final KijiCell<T> cell = itr.next();
        final String qualifier = cell.getColumn().getQualifier();
        NavigableMap<Long, T> values = qualifiers.get(qualifier);
        if (values == null) {
          values = Maps.newTreeMap(TimestampComparator.INSTANCE);
          qualifiers.put(qualifier, values);
        }
        values.put(cell.getTimestamp(), cell.getData());
      }
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, T> getValues(
      final String family,
      final String qualifier
  ) {
  return getValues(KijiColumnName.create(family, qualifier));
}

  /**
   * Gets all data stored within the specified column.
   *
   * @param column of the desired data.
   * @param <T> Type of the data stored at the specified coordinates.
   * @return A sorted map containing the data stored in the specified column.
   */
  private <T> NavigableMap<Long, T> getValues(KijiColumnName column) {
    final NavigableMap<Long, T> values = Maps.newTreeMap(TimestampComparator.INSTANCE);
    final KijiResultIterator<T> itr = mResult.iterator(column);
    while (itr.hasNext()) {
      final KijiCell<T> cell = itr.next();
      values.put(cell.getTimestamp(), cell.getData());
    }
    return values;
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getCell(
      final String family,
      final String qualifier,
      final long timestamp
  ) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    final Column columnRequest = mResult.getDataRequest().getColumn(family, qualifier);
    Preconditions.checkNotNull(
        columnRequest,
        "Column %s does not exist in the DataRequest.",
        column);
    Preconditions.checkArgument(
        !columnRequest.isPagingEnabled(),
        "Column%s is paged in the DataRequest.");

    return mResult.getCell(column, timestamp);
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getMostRecentCell(final String family, final String qualifier) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    validateColumnRequest(column);

    return mResult.getMostRecentCell(column);
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, KijiCell<T>> getMostRecentCells(
      final String family
  ) {
    final Collection<Column> columns = mResult.getDataRequest().getColumns();
    final List<KijiColumnName> qualifiedMapColumns = Lists.newArrayListWithCapacity(columns.size());
    KijiColumnName unqualifiedFamily = null;
    for (Column column : columns) {
      if (column.getFamily().equals(family) && !column.isPagingEnabled()) {
        if (column.getQualifier() == null) {
          unqualifiedFamily = column.getColumnName();
          break;
        } else {
          qualifiedMapColumns.add(column.getColumnName());
        }
      }
    }

    final NavigableMap<String, KijiCell<T>> qualifiers = Maps.newTreeMap();
    if (unqualifiedFamily == null) {
      // Retrieving a subset of the qualifiers in the family
      for (KijiColumnName column : qualifiedMapColumns) {
        final KijiCell<T> cell = mResult.getMostRecentCell(column);
        qualifiers.put(cell.getColumn().getQualifier(), cell);
      }
    } else {
      // Retrieve all of the qualifiers in the family
      for (KijiColumnName column : qualifiedMapColumns) {
        final KijiResultIterator<T> itr = mResult.iterator(column);
        String lastSeenQualifier = null;
        while (itr.hasNext()) {
          final KijiCell<T> cell = itr.next();
          final String qualifier = cell.getColumn().getQualifier();
          if (!qualifier.equals(lastSeenQualifier)) {
            qualifiers.put(qualifier, cell);
            lastSeenQualifier = qualifier;
          }
        }
      }
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, KijiCell<T>>> getCells(
      final String family
  ) {
    final Collection<Column> columns = mResult.getDataRequest().getColumns();
    final List<KijiColumnName> qualifiedMapColumns = Lists.newArrayListWithCapacity(columns.size());
    KijiColumnName unqualifiedFamily = null;
    for (Column column : columns) {
      if (column.getFamily().equals(family) && !column.isPagingEnabled()) {
        if (column.getQualifier() == null) {
          unqualifiedFamily = column.getColumnName();
          break;
        } else {
          qualifiedMapColumns.add(column.getColumnName());
        }
      }
    }

    final NavigableMap<String, NavigableMap<Long, KijiCell<T>>> qualifiers = Maps.newTreeMap();
    if (unqualifiedFamily == null) {
      // Retrieving a subset of the qualifiers in the family
      for (KijiColumnName column : qualifiedMapColumns) {
        final NavigableMap<Long, KijiCell<T>> cells = getCells(column);
        qualifiers.put(column.getQualifier(), cells);
      }
    } else {
      // Retrieve all of the qualifiers in the family
      final KijiResultIterator<T> itr = mResult.iterator(unqualifiedFamily);
      while (itr.hasNext()) {
        final KijiCell<T> cell = itr.next();
        final String qualifier = cell.getColumn().getQualifier();
        NavigableMap<Long, KijiCell<T>> cells = qualifiers.get(qualifier);
        if (cells == null) {
          cells = Maps.newTreeMap(TimestampComparator.INSTANCE);
          qualifiers.put(qualifier, cells);
        }
        cells.put(cell.getTimestamp(), cell);
      }
    }
    return qualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, KijiCell<T>> getCells(final String family, final String qualifier) {
    return getCells(KijiColumnName.create(family, qualifier));
  }

  /**
   * Gets all cells stored within the specified column.
   *
   * @param column of the desired cells.
   * @param <T> Type of the cells stored at the specified coordinates.
   * @return A sorted map containing the cells stored in the specified column.
   */
  private <T> NavigableMap<Long, KijiCell<T>> getCells(KijiColumnName column) {
    final NavigableMap<Long, KijiCell<T>> cells = Maps.newTreeMap(TimestampComparator.INSTANCE);
    final KijiResultIterator<T> itr = mResult.iterator(column);
    while (itr.hasNext()) {
      final KijiCell<T> cell = itr.next();
      cells.put(cell.getTimestamp(), cell);
    }
    return cells;
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(
      final String family,
      final String qualifier
  ) throws KijiColumnPagingNotEnabledException {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    Column columnRequest = mResult.getDataRequest().getRequestForColumn(column);

    if (columnRequest == null && qualifier != null) {
      // If it's a fully qualified request, try checking if the family is paged
      columnRequest =
          mResult.getDataRequest().getRequestForColumn(KijiColumnName.create(family, null));
    }

    Preconditions.checkNotNull(columnRequest,
        "Requested column %s is not included in the data request %s.",
        column, mResult.getDataRequest());

    if (!columnRequest.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
          String.format("Requested column %s dpes not have paging enabled in data request %s.",
          column, mResult.getDataRequest()));
    }

    Preconditions.checkArgument(columnRequest.isPagingEnabled());

    return new KijiResultPager(
        mResult.getEntityId(),
        mResult.getDataRequest(),
        column,
        mResult.iterator(column));
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(final String family) throws KijiColumnPagingNotEnabledException {
    final KijiColumnName column = KijiColumnName.create(family, null);
    Column columnRequest = mResult.getDataRequest().getRequestForColumn(column);

    Preconditions.checkNotNull(columnRequest,
        "Requested column %s is not included in the data request %s.",
        column, mResult.getDataRequest());

    if (!columnRequest.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
          String.format("Requested column %s dpes not have paging enabled in data request %s.",
              column, mResult.getDataRequest()));
    }

    return new KijiResultQualifierPager(
        mResult.getEntityId(),
        mResult.getDataRequest(),
        column,
        mResult.iterator(column));
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(final String family, final String qualifier) {
    final KijiColumnName column = KijiColumnName.create(family, qualifier);
    validateColumnRequest(column);

    return mResult.iterator(column);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(final String family) {
    return iterator(family, null);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(
      final String family,
      final String qualifier
  ) {
    final Iterator<KijiCell<T>> itr = iterator(family, qualifier);
    return ImmutableList.copyOf(itr);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(final String family) {
    final Iterator<KijiCell<T>> itr = iterator(family);
    return ImmutableList.copyOf(itr);
  }

  /**
   * Retrieves the column request from the {@code KijiDataRequest}, or throws an exception if the
   * request is paged.
   *
   * @param column of the request to retrieve.
   * @return the column's data request, or null if it does not exist.
   */
  private Column getColumnRequest(final KijiColumnName column) {
    final KijiDataRequest dataRequest = mResult.getDataRequest();
    final Column columnRequest = dataRequest.getColumn(column);

    if (columnRequest != null) {
      // The column is requested
      Preconditions.checkArgument(!columnRequest.isPagingEnabled(),
          "Paging is enabled for column %s in data request %s.", columnRequest, dataRequest);

      return columnRequest;
    } else if (column.isFullyQualified()) {
      // The requested column is a qualified column; check if the family is requested
      final Column familyRequest =
          dataRequest.getColumn(KijiColumnName.create(column.getFamily(), null));

      if (familyRequest != null) {
        Preconditions.checkArgument(!familyRequest.isPagingEnabled(),
            "Paging is enabled for column %s in data request %s.", familyRequest, dataRequest);
        return familyRequest;
      }
    }
    return null;
  }

  /**
   * Retrieves the column request from the {@code KijiDataRequest}, or throws an exception if the
   * request is paged or does not exist.
   *
   * @param column of the request to retrieve.
   * @return the column's data request.
   */
  private Column validateColumnRequest(final KijiColumnName column) {
    return Preconditions.checkNotNull(
        getColumnRequest(column),
        "Requested column %s is not included in the data request %s.",
        column, mResult.getDataRequest());
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (!obj.getClass().equals(this.getClass())) {
      return false;
    }

    final KijiResultRowData other = (KijiResultRowData) obj;
    return mResult.equals(other.mResult);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("KijiResult", mResult).toString();
  }
}
