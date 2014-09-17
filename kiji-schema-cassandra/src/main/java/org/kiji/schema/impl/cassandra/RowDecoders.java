package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.base.Objects;

import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiRowKeyComponents;
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
   * @param layout The table layout of the Kiji table.
   * @param translator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The value type in the column.
   * @return A decoded cell.
   */
  public static <T> Function<Row, KijiCell<T>> getRowDecoderFunction(
      final KijiColumnName column,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider
  ) {
    try {
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
        return new GroupFamilyDecoder<T>(
            translator.toCassandraColumnName(column),
            translator,
            decoderProvider);
      }
    } catch (NoSuchColumnException e) {
      throw new IllegalStateException(String.format("Column %s does not exist in Kiji table %s.",
          column, layout.getName()));
    }
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
                CassandraByteUtil.byteBuffertoBytes(row.getBytes(CQLUtils.VALUE_COL)));

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
   * A comparator for {@link TokenRowKeyComponents}
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
   * Decodes a Cassandra row containing token and entity ID columns into an
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

  @Immutable
  private static final class FormattedRowKeyDecoder
      implements Function<Row, TokenRowKeyComponents> {
    private final RowKeyFormat2 mKeyFormat;
    private final String mTokenColumn;

    private FormattedRowKeyDecoder(final KijiTableLayout layout) {
      mTokenColumn = CQLUtils.getTokenColumn(layout);
      mKeyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    }

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


  /** private constructor for utility class. */
  private RowDecoders() { }
}
