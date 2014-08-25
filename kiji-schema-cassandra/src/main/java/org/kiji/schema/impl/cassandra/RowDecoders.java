package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import com.datastax.driver.core.Row;
import com.google.common.base.Function;

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

  public static <T> Function<Row, KijiCell<T>> getDecoderFunction(
      final CassandraTableName tableName,
      final KijiColumnName column,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) throws NoSuchColumnException {
    if (tableName.isCounter()) {
      if (column.isFullyQualified()) {
        // Fully qualified counter column
        return new CounterColumnDec

      } else {
        // Family of counter columns

      }

    } else {
      if (layout.getFamilyMap().get(column.getFamily()).isMapType()) {
        // Map-type family
      } else {
        // Group-type family
      }
    }

    throw new RuntimeException();
  }

  /**
   * A function which will decode {@link Row}s from a map-type column.
   *
   * <p>
   *   This function may apply optimizations that make it only suitable to decode {@code Row}s
   *   from the specified group-type family, so do not use it over {@code Row}s from another
   *   family.
   * </p>
   *
   * <p>
   *   This class is <em>not</em> threadsafe.
   * </p>
   */
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
          throw new IllegalArgumentException(e);
        }
      }

      try {
        final DecodedCell<T> decodedCell = mCellDecoder.decodeCell(row.getValue());
        return KijiCell.create(mLastColumn, row.getTimestamp(), decodedCell);
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
          throw new IllegalArgumentException(e);
        }
      }

      try {
        final Long version = row.getLong(CQLUtils.VERSION_COL);
        final DecodedCell<T> decodedCell =
            mLastDecoder.decodeCell(
                CassandraByteUtil.byteBuffertoBytes(row.getBytes(CQLUtils.VALUE_COL)));

        return KijiCell.create(mLastColumn, version, decodedCell);
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * A function which will decode {@link Row}s from a qualified counter column.
   *
   * <p>
   *   The column may be from either a map-type or group-type family.
   * </p>
   *
   * <p>
   *   This function may apply optimizations that make it only suitable to decode {@code Row}s
   *   from the specified column, so do not use it over {@code Row}s from another column.
   * </p>
   */
  @Immutable
  private static final class QualifiedCounterDecoder implements Function<Row, KijiCell<Long>> {
    private final KijiColumnName mColumnName;

    /**
     * Create a qualified column decoder for the provided column.
     *
     * @param columnName of the column.
     */
    public QualifiedCounterDecoder(
        final KijiColumnName columnName
    ) {
      mColumnName = columnName;
    }

    /** {@inheritDoc} */
    @Override
    public KijiCell<Long> apply(final Row row) {
      final DecodedCell<Long> decodedCell =
          new DecodedCell<Long>(DecodedCell.NO_SCHEMA, row.getLong(CQLUtils.VALUE_COL));
      return KijiCell.create(mColumnName, KConstants.CASSANDRA_COUNTER_TIMESTAMP, decodedCell);
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
   */
  @Immutable
  private static final class CounterFamilyDecoder implements Function<Row, KijiCell<Long>> {
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
    public KijiCell<Long> apply(final Row row) {
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
        final DecodedCell<Long> decodedCell =
            new DecodedCell<Long>(DecodedCell.NO_SCHEMA, row.getLong(CQLUtils.VALUE_COL));
        return KijiCell.create(column, KConstants.CASSANDRA_COUNTER_TIMESTAMP, decodedCell);
      } catch (NoSuchColumnException e) {
        // TODO: this should be handled. This could happen in the case of a dropped column.
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
   * @param <T> type of value in the column.
   */
  @Immutable
  private static final class CounterColumnDecoder<T> implements Function<Row, KijiCell<T>> {
    private final KijiCellDecoder<T> mCellDecoder;
    private final KijiColumnName mColumnName;

    /**
     * Create a qualified column decoder for the provided column.
     *
     * @param columnName of the column.
     * @param cellDecoder for the table.
     */
    public CounterColumnDecoder(
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

  /** private constructor for utility class. */
  private RowDecoders() { }
}
