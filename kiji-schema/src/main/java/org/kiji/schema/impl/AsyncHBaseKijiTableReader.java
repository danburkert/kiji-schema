package org.kiji.schema.impl;

import java.io.IOException;
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.collect.*;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.kiji.schema.*;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.layout.impl.ColumnNameTranslator;

public class AsyncHBaseKijiTableReader implements KijiTableReader {

  private final HBaseClient mClient;
  private final AsyncHBaseKijiTable mTable;
  private final byte[] mTableName;

  public AsyncHBaseKijiTableReader(HBaseClient client, AsyncHBaseKijiTable table) {
    this.mClient = client;
    this.mTable = table;
    KijiURI uri = table.getURI();
    mTableName = KijiManagedHBaseTableName.getKijiTableName(uri.getInstance(), uri.getTable()).toBytes();

    mTable.retain();
  }

  // Only supports one locality group
  // Does not set rowkey
  private Scanner translateRequest(KijiDataRequest dataRequest) {

    ColumnNameTranslator translator = mTable.getColumnNameTranslator();

    // HBase family -> Set(Qualifiers)
    Map<String, Set<String>> columns = Maps.newHashMap();

    int maxVersions = 1;

    for (KijiDataRequest.Column column : dataRequest.getColumns()) {
      final HBaseColumnName columnName;
      try {
        columnName = translator.toHBaseColumnName(column.getColumnName());
      } catch (NoSuchColumnException e) {
        throw new RuntimeException(e);
      }
      String qualifier = columnName.getQualifierAsString();
      String family = columnName.getFamilyAsString();

      maxVersions = Math.max(column.getMaxVersions(), maxVersions);

      Set<String> qualifiers = columns.get(family);
      if (qualifiers == null) {
        qualifiers = Sets.newHashSet(qualifier);
        columns.put(family, qualifiers);
      } else {
        qualifiers.add(qualifier);
      }
    }

    byte[][] families = new byte[columns.size()][];
    byte[][][] qualifiers = new byte[columns.size()][][];

    int i = 0;
    for (Map.Entry<String, Set<String>> column : columns.entrySet()) {
      families[i] = Bytes.toBytes(column.getKey());
      qualifiers[i] = new byte[column.getValue().size()][];
      int j = 0;
      for (String qualifier : column.getValue()) {
        qualifiers[i][j] = Bytes.toBytes(qualifier);
        j++;
      }
      i++;
    }

    Scanner scanner = mClient.newScanner(mTableName);
    scanner.setFamilies(families, qualifiers);
    scanner.setMaxTimestamp(dataRequest.getMaxTimestamp());
    scanner.setMinTimestamp(dataRequest.getMinTimestamp());
    scanner.setMaxVersions(maxVersions);

    // TODO - filters

    return scanner;
  }

  private static byte[] incrementRowKey(byte[] rowkey) {
    return Arrays.copyOf(rowkey, rowkey.length + 1);
  }

  @Override
  public KijiRowData get(EntityId entityId, KijiDataRequest dataRequest) throws IOException {
    final byte[] startRowKey = entityId.getHBaseRowKey();
    Scanner scanner = translateRequest(dataRequest);
    scanner.setStartKey(startRowKey);
    scanner.setStopKey(incrementRowKey(startRowKey));
    scanner.setMaxNumRows(1);


    ArrayList<ArrayList<KeyValue>> batch;

    try {
      batch = scanner.nextRows().join();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Iterable<KeyValue> keyValues = Iterables.concat(batch);


    throw new AssertionError();
  }

  @Override
  public List<KijiRowData> bulkGet(List<EntityId> entityIds, KijiDataRequest dataRequest) throws IOException {
    throw new AssertionError();
  }

  @Override
  public KijiRowScanner getScanner(KijiDataRequest dataRequest) throws IOException {
    throw new AssertionError();
  }

  @Override
  public KijiRowScanner getScanner(KijiDataRequest dataRequest, KijiScannerOptions scannerOptions) throws IOException {
    throw new AssertionError();
  }

  @Override
  public void close() throws IOException {
    throw new AssertionError();
  }

  public Deferred<List<KijiKeyValue<?>>> flatAsyncGet(EntityId entityId, KijiDataRequest dataRequest) throws IOException {
    final byte[] startRowKey = entityId.getHBaseRowKey();
    Scanner scanner = translateRequest(dataRequest);
    scanner.setStartKey(startRowKey);
    scanner.setStopKey(incrementRowKey(startRowKey));
    scanner.setMaxNumRows(1);

    Deferred<ArrayList<ArrayList<KeyValue>>> batch;

    try {
      batch = scanner.nextRows();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    KijiTableLayout tableLayout = mTable.getLayout();

    FlattenKeyValuesCallBack flattenCallBack = new FlattenKeyValuesCallBack(
        EntityIdFactory.getFactory(tableLayout),
        mTable.getColumnNameTranslator(),
        new CellDecoderProvider(
            tableLayout,
            mTable.getKiji().getSchemaTable(),
            GenericCellDecoderFactory.get(),
            Maps.<KijiColumnName, CellSpec>newTreeMap()),
        dataRequest);

    return batch.addCallback(flattenCallBack);
  }

  public class KijiKeyValue<T> {
    private final EntityId mEntityId;
    private final String mFamily;
    private final String mQualifier;
    private final long mVersion;
    private final T mValue;

    KijiKeyValue(EntityId entityId, String family, String qualifier, long version, T value) {
      mEntityId = entityId;
      mFamily = family;
      mQualifier = qualifier;
      mVersion = version;
      mValue = value;
    }

    public EntityId getEntityId() {
      return mEntityId;
    }

    public String getColumnFamily() {
      return mFamily;
    }

    public String getQualifier() {
      return mQualifier;
    }

    public long getVersion() {
      return mVersion;
    }

    public T getValue() {
      return mValue;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("EntityId", mEntityId)
          .add("Family", mFamily)
          .add("Qualifier", mQualifier)
          .add("Version", mVersion)
          .add("Value", mValue)
          .toString();
    }
  }



  private class FlattenKeyValuesCallBack extends ConvertKeyValueCallBack {

    private FlattenKeyValuesCallBack(
        EntityIdFactory entityIdFactory,
        ColumnNameTranslator columnNameTranslator,
        CellDecoderProvider cellDecoderProvider,
        KijiDataRequest kijiDataRequest) {
      super(entityIdFactory, columnNameTranslator, cellDecoderProvider, kijiDataRequest);
    }

    @Override
    public List<KijiKeyValue<?>> call(ArrayList<ArrayList<KeyValue>> rows) throws Exception {
      final ArrayList<KijiKeyValue<?>> keyValues = Lists.newArrayList();
      for (ArrayList<KeyValue> row : rows) {
        for (KeyValue kv : row) {
          keyValues.add(convertKeyValue(kv));
        }
      }
      return keyValues;
    }
  }


  private abstract class ConvertKeyValueCallBack implements Callback<List<KijiKeyValue<?>>, ArrayList<ArrayList<KeyValue>>> {
    private final EntityIdFactory mEntityIdFactory;
    private final ColumnNameTranslator mColumnNameTranslator;
    private final CellDecoderProvider mCellDecoderProvider;
    private final KijiDataRequest mKijiDataRequest;

    private final Map<byte[], EntityId> entityIdCache = Maps.newHashMap();
    private final Table<byte[], byte[], KijiColumnName> columnCache = HashBasedTable.create();

    protected ConvertKeyValueCallBack(
        EntityIdFactory entityIdFactory,
        ColumnNameTranslator columnNameTranslator,
        CellDecoderProvider cellDecoderProvider,
        KijiDataRequest kijiDataRequest) {
      mEntityIdFactory = entityIdFactory;
      mColumnNameTranslator = columnNameTranslator;
      mCellDecoderProvider = cellDecoderProvider;
      mKijiDataRequest = kijiDataRequest;
    }

    private EntityId getEntityID(byte[] bytes) {
      EntityId eid = entityIdCache.get(bytes);
      if (eid == null) {
        eid = mEntityIdFactory.getEntityIdFromHBaseRowKey(bytes);
        entityIdCache.put(bytes, eid);
      }
      return eid;
    }

    private KijiColumnName getColumnName(byte[] family, byte[] qualifier) throws NoSuchColumnException {
      KijiColumnName name = columnCache.get(family, qualifier);
      if (name == null) {
        name = mColumnNameTranslator.toKijiColumnName(new HBaseColumnName(family, qualifier));
        columnCache.put(family, qualifier, name);
      }
      return name;
    }


    protected KijiKeyValue<Object> convertKeyValue(KeyValue keyValue) throws IOException {
      EntityId eid = getEntityID(keyValue.key());
      KijiColumnName columnName = getColumnName(keyValue.family(), keyValue.qualifier());
      ColumnReaderSpec spec = mKijiDataRequest.getRequestForColumn(columnName).getReaderSpec();

      Object value;
      if (spec == null) {
        value = mCellDecoderProvider
            .getDecoder(columnName.getFamily(), columnName.getQualifier())
            .decodeValue(keyValue.value());
      } else {
        value = mCellDecoderProvider
            .getDecoder(BoundColumnReaderSpec.create(spec, columnName))
            .decodeValue(keyValue.value());
      }

      return new KijiKeyValue<Object>(eid, columnName.getFamily(), columnName.getQualifier(), keyValue.timestamp(), value);
    }
  }
}
