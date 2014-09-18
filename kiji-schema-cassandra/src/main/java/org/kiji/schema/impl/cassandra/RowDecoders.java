package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.base.Objects;

import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiRowKeyComponents;
import org.kiji.schema.KijiTable;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.cassandra.CassandraColumnName;
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
   * @param column The Kiji column of the Row.
   * @param columnRequest The column request defining the query.
   * @param dataRequest The data request defining the query.
   * @param layout The table layout of the Kiji table.
   * @param translator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The value type in the column.
   * @return A decoded cell.
   */
  public static <T> Function<Row, KijiCell<T>> getColumnFamilyDecoderFunction(
      final KijiColumnName column,
      final Column columnRequest,
      final KijiDataRequest dataRequest,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) {
    try {
      if (layout.getFamilyMap().get(column.getFamily()).isMapType()) {
        // Map-type family
        return new MapFamilyDecoder<T>(
            translator.toCassandraColumnName(column),
            columnRequest,
            dataRequest,
            translator,
            decoderProvider.<T>getDecoder(column));
      } else {
        // Group-type family
        return new GroupFamilyDecoder<T>(
            translator.toCassandraColumnName(column),
            columnRequest,
            dataRequest,
            translator,
            decoderProvider);
      }
    } catch (NoSuchColumnException e) {
      throw new IllegalStateException(String.format("Column %s does not exist in Kiji table %s.",
          column, layout.getName()));
    }
  }

  /**
   * Get a new decoder function for a Cassandra {@link Row}.
   *
   * @param column The Kiji column of the Row.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The value type in the column.
   * @return A decoded cell.
   */
  public static <T> Function<Row, KijiCell<T>> getQualifiedColumnDecoderFunction(
      final KijiColumnName column,
      final CellDecoderProvider decoderProvider
  ) {
    return new QualifiedColumnDecoder<T>(column, decoderProvider.<T>getDecoder(column));
  }

  /**
   * Get a function for decoding row keys and tokens from Cassandra rows.
   *
   * @param layout The table layout.
   * @return A function to decode row keys and tokens for the table.
   */
  public static Function<Row, TokenRowKeyComponents> getRowKeyDecoderFunction(
      final KijiTableLayout layout
  ) {
    final RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();

    switch (keyFormat.getEncoding()) {
      case RAW: return new RawRowKeyDecoder(layout);
      case FORMATTED: return new FormattedRowKeyDecoder(layout);
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }

  /**
   * Get a function for converting {@link TokenRowKeyComponents} to {@link EntityId}s.
   *
   * @param table The Kiji table the row keys belong to.
   * @return A function for converting {@link TokenRowKeyComponents} to {@link EntityId}s.
   */
  public static Function<TokenRowKeyComponents, EntityId> getEntityIdFunction(
      final KijiTable table
  ) {
    return new RowKeyComponentsToEntityId(table);
  }

  /**
   * A function which will decode {@link Row}s from a map-type column. If data request specifies
   * a maximum number of versions for the columns in the family, then this function will filter
   * extra versions and return null.
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
    private final int mMaxVersions;
    private final long mMinTimestamp;
    private final long mMaxTimestamp;

    private KijiColumnName mLastColumn = null;
    private ByteBuffer mLastQualifier = null;
    private int mLastQualifierVersions = 0;

    /**
     * Create a map-family column decoder.
     *
     * @param familyColumn The Kiji column of the Row.
     * @param columnRequest The column request defining the query.
     * @param dataRequest The data request defining the query.
     * @param columnTranslator The column translator for the table.
     * @param decoder for the table.
     */
    public MapFamilyDecoder(
        final CassandraColumnName familyColumn,
        final Column columnRequest,
        final KijiDataRequest dataRequest,
        final CassandraColumnNameTranslator columnTranslator,
        final KijiCellDecoder<T> decoder
    ) {
      mFamilyColumn = familyColumn;
      mColumnTranslator = columnTranslator;
      mCellDecoder = decoder;
      mMaxVersions = columnRequest.getMaxVersions();
      mMinTimestamp = dataRequest.getMinTimestamp();
      mMaxTimestamp = dataRequest.getMaxTimestamp();
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
        mLastQualifierVersions = 0;
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
          // There should be no columns that we can't decode, so this signals a logic error
          throw new InternalKijiError(e);
        }
      }

      final long version = row.getLong(CQLUtils.VERSION_COL);

      if (mMaxVersions == 0 || mLastQualifierVersions < mMaxVersions
          && version < mMaxTimestamp && version >= mMinTimestamp) {
        try {
          mLastQualifierVersions += 1;
          final DecodedCell<T> decodedCell =
              mCellDecoder.decodeCell(
                  CassandraByteUtil.byteBuffertoBytes(row.getBytes(CQLUtils.VALUE_COL)));
          return KijiCell.create(mLastColumn, version, decodedCell);
        } catch (IOException e) {
          throw new KijiIOException(e);
        }
      } else {
        return null;
      }
    }
  }

  /**
   * A function which will decode {@link Row}s from a group-type family. If data request specifies
   * a maximum number of versions for the columns in the family, then this function will filter
   * extra versions and return null. If a column is read which has been dropped, then this function
   * will return null.
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
    private final int mMaxVersions;
    private final long mMaxTimestamp;
    private final long mMinTimestamp;

    private KijiCellDecoder<T> mLastDecoder;
    private KijiColumnName mLastColumn;
    private ByteBuffer mLastQualifier;
    private int mLastQualifierVersions = 0;

    /**
     * Create a qualified column decoder for the provided column.
     *
     * @param familyColumn The Kiji column of the Row.
     * @param columnRequest The column request defining the query.
     * @param dataRequest The data request defining the query.
     * @param columnTranslator The column translator for the table.
     * @param decoderProvider A cell decoder provider for the table.
     */
    public GroupFamilyDecoder(
        final CassandraColumnName familyColumn,
        final Column columnRequest,
        final KijiDataRequest dataRequest,
        final CassandraColumnNameTranslator columnTranslator,
        final CellDecoderProvider decoderProvider
    ) {
      mDecoderProvider = decoderProvider;
      mColumnTranslator = columnTranslator;
      mFamilyColumn = familyColumn;
      mMaxVersions = columnRequest.getMaxVersions();
      mMaxTimestamp = dataRequest.getMaxTimestamp();
      mMinTimestamp = dataRequest.getMinTimestamp();
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
          mLastQualifier = qualifier.duplicate();
          mLastQualifierVersions = 0;
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
          return null;
        }
      }

      final long version = row.getLong(CQLUtils.VERSION_COL);

      if ((mMaxVersions == 0 || mLastQualifierVersions < mMaxVersions)
          && version < mMaxTimestamp && version >= mMinTimestamp) {
        try {
          mLastQualifierVersions += 1;
          final DecodedCell<T> decodedCell =
              mLastDecoder.decodeCell(
                  CassandraByteUtil.byteBuffertoBytes(row.getBytes(CQLUtils.VALUE_COL)));
          return KijiCell.create(mLastColumn, version, decodedCell);
        } catch (IOException e) {
          throw new KijiIOException(e);
        }
      } else {
        return null;
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
   * A 2-tuple combining a Cassandra token and Kiji row key components.
   */
  @Immutable
  public static class TokenRowKeyComponents {
    private final long mToken;
    private final KijiRowKeyComponents mComponents;

    public TokenRowKeyComponents(final long token, final KijiRowKeyComponents components) {
      mToken = token;
      mComponents = components;
    }

    public long getToken() {
      return mToken;
    }

    public KijiRowKeyComponents getComponents() {
      return mComponents;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mToken, mComponents);
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final TokenRowKeyComponents other = (TokenRowKeyComponents) obj;
      return Objects.equal(this.mToken, other.mToken)
          && Objects.equal(this.mComponents, other.mComponents);
    }
  }

  /**
   * A comparator for {@link TokenRowKeyComponents}.
   */
  @Immutable
  public static class TokenRowKeyComponentsComparator implements Comparator<TokenRowKeyComponents> {
    public static final TokenRowKeyComponentsComparator INSTANCE =
        new TokenRowKeyComponentsComparator();

    /** Private constructor for non-instantiable class */
    private TokenRowKeyComponentsComparator() { }


    @Override
    public int compare(
        final TokenRowKeyComponents a,
        final TokenRowKeyComponents b
    ) {
      final long tokenCompare = a.getToken() - b.getToken();
      if (tokenCompare != 0) {
        return (int) tokenCompare;
      } else {
        return a.getComponents().compareTo(b.getComponents());
      }
    }
  }

  /**
   * Decodes a Cassandra row containing the token a raw row key olumn into a
   * {@link TokenRowKeyComponents}.
   */
  @Immutable
  private static final class RawRowKeyDecoder implements Function<Row, TokenRowKeyComponents> {
    final String mTokenColumn;

    private RawRowKeyDecoder(final KijiTableLayout layout) {
      mTokenColumn = CQLUtils.getTokenColumn(layout);
    }

    @Override
    public TokenRowKeyComponents apply(final Row row) {
      final int token = row.getInt(mTokenColumn);
      final Object[] components =
          new Object[] {
              CassandraByteUtil.byteBuffertoBytes(row.getBytes(CQLUtils.RAW_KEY_COL))
          };
      return new TokenRowKeyComponents(token, KijiRowKeyComponents.fromComponents(components));
    }
  }

  /**
   * Decodes a Cassandra row containing the token and row key component columns into a
   * {@link TokenRowKeyComponents}.
   */
  @Immutable
  private static final class FormattedRowKeyDecoder
      implements Function<Row, TokenRowKeyComponents> {

    private final RowKeyFormat2 mKeyFormat;
    private final String mTokenColumn;

    /**
     * Create a new {@code FormattedRowKeyDecoder}.
     *
     * @param layout The table layout.
     */
    private FormattedRowKeyDecoder(final KijiTableLayout layout) {
      mTokenColumn = CQLUtils.getTokenColumn(layout);
      mKeyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    }

    /** {@inheritDoc} */
    @Override
    public TokenRowKeyComponents apply(final Row row) {

      final List<RowKeyComponent> formatComponents = mKeyFormat.getComponents();
      final Object[] components = new Object[formatComponents.size()];

      for (int i = 0; i < formatComponents.size(); i++) {
        RowKeyComponent component = formatComponents.get(i);
        // TODO: investigate whether we can do this by position instead of creating a bunch of
        // garbage through column name translation
        final String columnName =
            CQLUtils.translateEntityIDComponentNameToColumnName(component.getName());
        switch (component.getType()) {
          case STRING: {
            components[i] = row.getString(columnName);
            break;
          }
          case INTEGER: {
            components[i] = row.getInt(columnName);
            break;
          }
          case LONG: {
            components[i] = row.getLong(columnName);
            break;
          }
          default: throw new IllegalArgumentException("Unknown row key component type.");
        }
      }

      return new TokenRowKeyComponents(
          row.getLong(mTokenColumn),
          KijiRowKeyComponents.fromComponents(components));
    }
  }

  /**
   * A function for converting {@link TokenRowKeyComponents} to {@link EntityId}s.
   */
  private static class RowKeyComponentsToEntityId
      implements Function<TokenRowKeyComponents, EntityId> {

    private final KijiTable mTable;

    /**
     * Create a new function for converting a {@link TokenRowKeyComponents} to an {@link EntityId}.
     *
     * @param table The table the row key belongs to.
     */
    public RowKeyComponentsToEntityId(final KijiTable table) {
      mTable = table;
    }

    /** {@inheritDoc} */
    @Override
    public EntityId apply(final TokenRowKeyComponents input) {
      return input.getComponents().getEntityIdForTable(mTable);
    }
  }

  /** private constructor for utility class. */
  private RowDecoders() { }
}
