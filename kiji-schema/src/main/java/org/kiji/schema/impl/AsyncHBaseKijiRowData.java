package org.kiji.schema.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.hbase.async.Scanner;
import org.kiji.schema.*;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.impl.CellDecoderProvider;

import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;

public class AsyncHBaseKijiRowData implements KijiRowData {

  private final Scanner mScanner;
  private final AsyncHBaseKijiTable mTable;
  private final EntityId mEntityId;
  private final CellDecoderProvider mDecoderProvider;
  private final KijiDataRequest mDataRequest;

  public AsyncHBaseKijiRowData(Scanner scanner, AsyncHBaseKijiTable table, EntityId entityId, KijiDataRequest dataRequest)
      throws IOException {
    mScanner = scanner;
    mTable = table;
    mEntityId = entityId;
    mDataRequest = dataRequest;

    mDecoderProvider = new CellDecoderProvider(
        this.mTable.getLayout(),
        Maps.<KijiColumnName, BoundColumnReaderSpec>newHashMap(),
        Sets.<BoundColumnReaderSpec>newHashSet(),
        KijiTableReaderBuilder.DEFAULT_CACHE_MISS);
  }

  /**
   * Get the decoder for the given column from the {@link CellDecoderProvider}.
   *
   * @param column column for which to get a cell decoder.
   * @param <T> the type of the value encoded in the cell.
   * @return a cell decoder which can read the given column.
   * @throws IOException in case of an error getting the cell decoder.
   */
  private <T> KijiCellDecoder<T> getDecoder(KijiColumnName column) throws IOException {
    final KijiDataRequest.Column requestColumn = mDataRequest.getRequestForColumn(column);
    if (null != requestColumn) {
      final ColumnReaderSpec spec = requestColumn.getReaderSpec();
      if (null != spec) {
        // If there is a spec override in the data request, use it to get the decoder.
        return mDecoderProvider.getDecoder(BoundColumnReaderSpec.create(spec, column));
      }
    }
    // If the column is not in the request, or there is no spec override, get the decoder for the
    // column by name.
    return mDecoderProvider.getDecoder(column.getFamily(), column.getQualifier());
  }

  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  @Override
  public boolean containsColumn(String family, String qualifier) {
    return false;
  }

  @Override
  public boolean containsColumn(String family) {
    return false;
  }

  @Override
  public boolean containsCell(String family, String qualifier, long timestamp) {
    return false;
  }

  @Override
  public NavigableSet<String> getQualifiers(String family) {
    return null;
  }

  @Override
  public NavigableSet<Long> getTimestamps(String family, String qualifier) {
    return null;
  }

  @Override
  public Schema getReaderSchema(String family, String qualifier) throws IOException {
    return null;
  }

  @Override
  public <T> T getValue(String family, String qualifier, long timestamp) throws IOException {
    return null;
  }

  @Override
  public <T> T getMostRecentValue(String family, String qualifier) throws IOException {
    return null;
  }

  @Override
  public <T> NavigableMap<String, T> getMostRecentValues(String family) throws IOException {
    return null;
  }

  @Override
  public <T> NavigableMap<String, NavigableMap<Long, T>> getValues(String family) throws IOException {
    return null;
  }

  @Override
  public <T> NavigableMap<Long, T> getValues(String family, String qualifier) throws IOException {
    return null;
  }

  @Override
  public <T> KijiCell<T> getCell(String family, String qualifier, long timestamp) throws IOException {
    return null;
  }

  @Override
  public <T> KijiCell<T> getMostRecentCell(String family, String qualifier) throws IOException {
    return null;
  }

  @Override
  public <T> NavigableMap<String, KijiCell<T>> getMostRecentCells(String family) throws IOException {
    return null;
  }

  @Override
  public <T> NavigableMap<String, NavigableMap<Long, KijiCell<T>>> getCells(String family) throws IOException {
    return null;
  }

  @Override
  public <T> NavigableMap<Long, KijiCell<T>> getCells(String family, String qualifier) throws IOException {
    return null;
  }

  @Override
  public KijiPager getPager(String family, String qualifier) throws KijiColumnPagingNotEnabledException {
    return null;
  }

  @Override
  public KijiPager getPager(String family) throws KijiColumnPagingNotEnabledException {
    return null;
  }

  @Override
  public <T> Iterator<KijiCell<T>> iterator(String family, String qualifier) throws IOException {
    return null;
  }

  @Override
  public <T> Iterator<KijiCell<T>> iterator(String family) throws IOException {
    return null;
  }

  @Override
  public <T> Iterable<KijiCell<T>> asIterable(String family, String qualifier) throws IOException {
    return null;
  }

  @Override
  public <T> Iterable<KijiCell<T>> asIterable(String family) throws IOException {
    return null;
  }
}
