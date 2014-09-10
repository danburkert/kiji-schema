package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import org.kiji.schema.DecodedCell;
import org.kiji.schema.KConstants;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/**
 * Provides decoding functions for Kiji columns.
 */
public class RowDecoders {

  /**
   * Get a new decoder function for a Cassandra {@link Row}.
   *
   * @param tableName The Cassandra table of the Row.
   * @param column The Kiji column of the Row.
   * @param layout The table layout of the Kiji table.
   * @param translator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The value type in the column.
   * @return A decoded cell.
   */
  public static <T> Function<Row, KijiCell<T>> getRowDecoderFunction(
      final CassandraTableName tableName,
      final KijiColumnName column,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) {
    try {
      if (tableName.isCounter()) {
        if (column.isFullyQualified()) {
          // Fully qualified counter column
          return new CounterColumnDecoder<T>(column);
        } else {
          // Counter family
          return new CounterFamilyDecoder<T>(translator.toCassandraColumnName(column), translator);
        }
      } else {
        if (column.isFullyQualified()) {
          return new QualifiedColumnDecoder<T>(column, decoderProvider.<T>getDecoder(column));
        } else if (layout.getFamilyMap().get(column.getFamily()).isMapType()) {
          // Map-type family
          return new MapFamilyDecoder<T>(
              translator.toCassandraColumnName(column),
              translator,
              decoderProvider.<T>getDecoder(column));
        } else {
          // Group-type family
          return new MapFamilyDecoder<T>(
              translator.toCassandraColumnName(column),
              translator,
              decoderProvider.<T>getDecoder(column));
        }
      }
    } catch (NoSuchColumnException e) {
      throw new IllegalStateException(String.format("Column %s does not exist in Kiji table %s.",
          column, layout.getName()));
    }
  }

  /**
   * Get a new decoder function for a Cassandra {@link ResultSet}.
   *
   * @param tableName The Cassandra table of the ResultSet.
   * @param column The Kiji column of the ResultSet.
   * @param layout The table layout of the Kiji table.
   * @param translator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The value type in the column.
   * @return A decoded list of cells.
   */
  public static <T> Function<ResultSet, Iterator<KijiCell<T>>> getResultSetDecoderFunction(
      final CassandraTableName tableName,
      final KijiColumnName column,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) {
    return new ResultSetDecoder<T>(tableName, column, layout, translator, decoderProvider);
  }


  /**
   * A function which will decode {@link Row}s from a map-type column.
   *
   * <p>
   *   This function may apply optimizations that make it only suitable to decode {@code Row}s
   *   from the specified group-type family, so do not use it over {@code Row}s from another
   *   family.
   * </p>
   */
  @NotThreadSafe
  private static final class MapFamilyDecoder<T> implements Function<Row, KijiCell<T>> {
    private final CassandraColumnName mFamilyColumn;
    private final KijiCellDecoder<T> mCellDecoder;
    private final CassandraColumnNameTranslator mColumnTranslator;

    private KijiColumnName mLastColumn = null;
    private ByteBuffer mLastQualifier = null;

    /**
     * Create a map-family column decoder.
     *
     * @param columnTranslator for the table.
     * @param decoder for the table.
     */
    public MapFamilyDecoder(
        final CassandraColumnName familyColumn,
        final CassandraColumnNameTranslator columnTranslator,
        final KijiCellDecoder<T> decoder
    ) {
      mFamilyColumn = familyColumn;
      mColumnTranslator = columnTranslator;
      mCellDecoder = decoder;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     *   We cache the previously-used {@code KijiColumnName}. This saves parsing and allocations of
     *   the column name for the common case of iterating through multiple versions of each column
     *   in the family.
     * </p>
     *
     * @param row to decode.
     * @return the decoded KijiCell.
     */
    @Override
    public KijiCell<T> apply(final Row row) {
      final ByteBuffer qualifier = row.getBytes(CQLUtils.QUALIFIER_COL);
      if (!qualifier.equals(mLastQualifier)) {
        mLastQualifier = qualifier;
        try {
          mLastColumn =
              mColumnTranslator.toKijiColumnName(
                  new CassandraColumnName(
                      mFamilyColumn.getLocalityGroup(),
                      mFamilyColumn.getFamily(),
                      CassandraByteUtil.byteBuffertoBytes(row.getBytes(CQLUtils.QUALIFIER_COL))));
        } catch (NoSuchColumnException e) {
          mLastQualifier = null;
          mLastColumn = null;
          // TODO(SCHEMA-962): Critical! Handle this. Will happen when reading a deleted column
          throw new IllegalArgumentException(e);
        }
      }

      try {
        final DecodedCell<T> decodedCell =
            mCellDecoder.decodeCell(
                CassandraByteUtil.byteBuffertoBytes(
                    row.getBytes(CQLUtils.VALUE_COL)));
        return KijiCell.create(mLastColumn, row.getLong(CQLUtils.VERSION_COL), decodedCell);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * A function which will decode {@link Row}s from a group-type family.
   *
   * <p>
   *   This function may use optimizations that make it only suitable to decode {@code Row}s
   *   from the specified group-type family, so do not use it over {@code Row}s from another
   *   family.
   * </p>
   */
  @NotThreadSafe
  private static final class GroupFamilyDecoder<T> implements Function<Row, KijiCell<T>> {
    private final CellDecoderProvider mDecoderProvider;
    private final CassandraColumnNameTranslator mColumnTranslator;
    private final CassandraColumnName mFamilyColumn;

    private KijiCellDecoder<T> mLastDecoder;
    private KijiColumnName mLastColumn;
    private ByteBuffer mLastQualifier;

    /**
     * Create a qualified column decoder for the provided column.
     *
     * @param columnTranslator for the table.
     * @param decoderProvider for the table.
     */
    public GroupFamilyDecoder(
        final CassandraColumnName familyColumn,
        final CassandraColumnNameTranslator columnTranslator,
        final CellDecoderProvider decoderProvider
    ) {
      mDecoderProvider = decoderProvider;
      mColumnTranslator = columnTranslator;
      mFamilyColumn = familyColumn;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     *   We cache the previously-used {@code KijiCellDecoder} and {@code KijiColumnName}. This saves
     *   lookups (of the decoder) and allocations (of the column name) for the common case of
     *   iterating through the versions of a column in the family.
     * </p>
     *
     * TODO: We know that all of the KijiCell's decoded from this function always have the same
     * Kiji family, so we should not decode it. Currently the CassandraColumnNameTranslator does not
     * support this.
     *
     * @param row The row to decode.
     * @return the decoded KijiCell.
     */
    @Override
    public KijiCell<T> apply(final Row row) {
      final ByteBuffer qualifier = row.getBytes(CQLUtils.QUALIFIER_COL);
      if (!qualifier.equals(mLastQualifier)) {
        try {
          mLastQualifier = qualifier;
          mLastColumn =
              mColumnTranslator.toKijiColumnName(
                  new CassandraColumnName(
                      mFamilyColumn.getLocalityGroup(),
                      mFamilyColumn.getFamily(),
                      CassandraByteUtil.byteBuffertoBytes(qualifier)));
          mLastDecoder = mDecoderProvider.getDecoder(mLastColumn);
        } catch (NoSuchColumnException e) {
          mLastDecoder = null;
          mLastColumn = null;
          mLastQualifier = null;
          // TODO(SCHEMA-962): Critical! Handle this. Will happen when reading a deleted column
          throw new IllegalArgumentException(e);
        }
      }

      try {
        final Long version = row.getLong(CQLUtils.VERSION_COL);
        final DecodedCell<T> decodedCell =
            mLastDecoder.decodeCell(
                CassandraByteUtil.byteBuffertoBytes(
                    row.getBytes(CQLUtils.VALUE_COL)));

        return KijiCell.create(mLastColumn, version, decodedCell);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * A function which will decode {@link Row}s from a qualified column.
   *
   * <p>
   *   The column may be from either a map-type or group-type family.
   * </p>
   *
   * <p>
   *   This function may apply optimizations that make it only suitable to decode {@code KeyValue}s
   *   from the specified column, so do not use it over {@code KeyValue}s from another column.
   * </p>
   *
   * @param <T> type of value in the column.
   */
  @Immutable
  private static final class QualifiedColumnDecoder<T> implements Function<Row, KijiCell<T>> {
    private final KijiCellDecoder<T> mCellDecoder;
    private final KijiColumnName mColumnName;

    /**
     * Create a qualified column decoder for the provided column.
     *
     * @param columnName of the column.
     * @param cellDecoder for the table.
     */
    public QualifiedColumnDecoder(
        final KijiColumnName columnName,
        final KijiCellDecoder<T> cellDecoder
    ) {
      mCellDecoder = cellDecoder;
      mColumnName = columnName;
    }

    /** {@inheritDoc} */
    @Override
    public KijiCell<T> apply(final Row row) {
      try {
        final DecodedCell<T> decodedCell =
            mCellDecoder.decodeCell(
                CassandraByteUtil.byteBuffertoBytes(
                    row.getBytes(CQLUtils.VALUE_COL)));
        return KijiCell.create(mColumnName, row.getLong(CQLUtils.VERSION_COL), decodedCell);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * A function which will decode counter {@link Row}s from a group-type family.
   *
   * <p>
   *   This function may use optimizations that make it only suitable to decode {@code Row}s
   *   from the specified group-type family, so do not use it over {@code Row}s from another
   *   family.
   * </p>
   *
   * @param <T> generic type of KijiCell.  Must always be {@link Long}.
   */
  @Immutable
  private static final class CounterFamilyDecoder<T> implements Function<Row, KijiCell<T>> {
    private final CassandraColumnNameTranslator mColumnTranslator;
    private final CassandraColumnName mFamilyColumn;

    /**
     * Create a counter decoder for the provided column family. The family may be either group-type
     * or map-type.
     *
     * @param familyColumn The family containing counters to decode.
     * @param columnTranslator A column name translator for the table.
     */
    public CounterFamilyDecoder(
        final CassandraColumnName familyColumn,
        final CassandraColumnNameTranslator columnTranslator
    ) {
      mColumnTranslator = columnTranslator;
      mFamilyColumn = familyColumn;
    }

    /** {@inheritDoc} */
    @Override
    public KijiCell<T> apply(final Row row) {
      // TODO: We know that all of the KijiCell's decoded from this function always have the same
      // Kiji family, so we should not decode it. Currently the CassandraColumnNameTranslator does
      // not support this.
      try {
        final KijiColumnName column =
            mColumnTranslator.toKijiColumnName(
                new CassandraColumnName(
                    mFamilyColumn.getLocalityGroup(),
                    mFamilyColumn.getFamily(),
                    CassandraByteUtil.byteBuffertoBytes(row.getBytes(CQLUtils.QUALIFIER_COL))));
        @SuppressWarnings("unchecked")
        final DecodedCell<T> decodedCell =
            new DecodedCell(DecodedCell.NO_SCHEMA, row.getLong(CQLUtils.VALUE_COL));
        return KijiCell.create(column, KConstants.CASSANDRA_COUNTER_TIMESTAMP, decodedCell);
      } catch (NoSuchColumnException e) {
        // TODO(SCHEMA-962): Critical! Handle this. Will happen when reading a deleted column
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * A function which will decode counter {@link Row}s from a qualified column.
   *
   * <p>
   *   The column may be from either a map-type or group-type family.
   * </p>
   *
   * <p>
   *   This function may apply optimizations that make it only suitable to decode counter
   *   {@code Row}s from the specified fully-qualified column, so do not apply it to {@code Row}s
   *   from another column.
   * </p>
   *
   * @param <T> generic type of KijiCell.  Must always be {@link Long}.
   */
  @Immutable
  private static final class CounterColumnDecoder<T> implements Function<Row, KijiCell<T>> {
    private final KijiColumnName mColumnName;

    /**
     * Create a qualified column decoder for the provided column.
     *
     * @param columnName of the column.
     */
    public CounterColumnDecoder(final KijiColumnName columnName) {
      mColumnName = columnName;
    }

    /** {@inheritDoc} */
    @Override
    public KijiCell<T> apply(final Row row) {
      @SuppressWarnings("unchecked")
      final DecodedCell<T> decodedCell =
          new DecodedCell(DecodedCell.NO_SCHEMA, row.getLong(CQLUtils.VALUE_COL));

      return KijiCell.create(
          mColumnName,
          KConstants.CASSANDRA_COUNTER_TIMESTAMP,
          decodedCell);
    }
  }

  /**
   * A function which will decode a Cassandra {@link ResultSet} from a column.
   *
   * @param <T> type of value in the column.
   */
  @ThreadSafe
  private static final class ResultSetDecoder<T>
      implements Function<ResultSet, Iterator<KijiCell<T>>> {

    final CassandraTableName mTableName;
    final KijiColumnName mColumn;
    final KijiTableLayout mLayout;
    final CassandraColumnNameTranslator mTranslator;
    final CellDecoderProvider mDecoderProvider;

    /**
     * Get a new decoder function for a Cassandra {@link Row}.
     *  @param tableName The Cassandra table of the Row.
     * @param column The Kiji column of the Row.
     * @param layout The table layout of the Kiji table.
     * @param translator A column name translator for the table.
     * @param decoderProvider A cell decoder provider for the table.
     */
    public ResultSetDecoder(
        final CassandraTableName tableName,
        final KijiColumnName column,
        final KijiTableLayout layout,
        final CassandraColumnNameTranslator translator,
        final CellDecoderProvider decoderProvider) {
      mTableName = tableName;
      mColumn = column;
      mLayout = layout;
      mTranslator = translator;
      mDecoderProvider = decoderProvider;
  }

    @Override
    public Iterator<KijiCell<T>> apply(final ResultSet resultSet) {
      final Function<Row, KijiCell<T>> rowDecoder = getRowDecoderFunction(
          mTableName,
          mColumn,
          mLayout,
          mTranslator,
          mDecoderProvider);

      return Iterators.transform(resultSet.iterator(), rowDecoder);
    }
  }

  /** private constructor for utility class. */
  private RowDecoders() { }
}
