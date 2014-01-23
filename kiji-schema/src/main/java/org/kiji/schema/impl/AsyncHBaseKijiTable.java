package org.kiji.schema.impl;

import com.google.common.base.Preconditions;
import org.hbase.async.HBaseClient;
import org.kiji.schema.*;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.mortbay.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncHBaseKijiTable implements KijiTable {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncHBaseKijiTable.class);

  private final HBaseClient mAsyncClient;
  private final AsyncHBaseKiji mKiji;
  private final KijiURI mURI;
  private final KijiTableLayout mTableLayout;
  private final EntityIdFactory mEntityIdFactory;
  private final ColumnNameTranslator mTranslator;
  private final KijiTable mHBaseTable;

  /** Retain counter. When decreased to 0, the AsyncHBase KijiTable may be closed and disposed of. */
  private final AtomicInteger mRetainCount = new AtomicInteger(1);

  public AsyncHBaseKijiTable(HBaseClient asyncClient, AsyncHBaseKiji kiji, KijiTable hbaseTable, KijiURI uri) throws IOException {
    mAsyncClient = asyncClient;
    mKiji = kiji;
    mHBaseTable = hbaseTable;
    mURI = uri;
    mTableLayout = hbaseTable.getLayout();
    mEntityIdFactory = EntityIdFactory.getFactory(mTableLayout);
    mTranslator = ((HBaseKijiTable) hbaseTable).getColumnNameTranslator();
    mKiji.retain();
  }

  @Override
  public Kiji getKiji() {
    return mKiji;
  }

  @Override
  public String getName() {
    return mURI.getTable();
  }

  @Override
  public KijiURI getURI() {
    return mURI;
  }

  @Override
  public KijiTableLayout getLayout() {
    try {
      return mKiji.getMetaTable().getTableLayout(getName());
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public EntityId getEntityId(Object... kijiRowKey) {
    return mEntityIdFactory.getEntityId(kijiRowKey);
  }

  @Override
  public AsyncHBaseKijiTableReader openTableReader() {
    return new AsyncHBaseKijiTableReader(mAsyncClient, this);
  }

  @Override
  public KijiReaderFactory getReaderFactory() throws IOException {
    throw new AssertionError();
  }

  @Override
  public KijiTableWriter openTableWriter() {
    return mHBaseTable.openTableWriter();
  }

  @Override
  public KijiWriterFactory getWriterFactory() throws IOException {
    throw new AssertionError();
  }

  @Override
  public List<KijiRegion> getRegions() throws IOException {
    throw new AssertionError();
  }

  @Override
  public KijiTableAnnotator openTableAnnotator() throws IOException {
    throw new AssertionError();
  }

  @Override
  public KijiTable retain() {
    final int counter = mRetainCount.getAndIncrement();
    Preconditions.checkState(counter >= 1,
        "Cannot retain a closed KijiTable %s: retain counter was %s.", mURI, counter);
    return this;
  }

  @Override
  public void release() throws IOException {
    final int counter = mRetainCount.decrementAndGet();
    Preconditions.checkState(counter >= 0,
        "Cannot release closed KijiTable %s: retain counter is now %s.", mURI, counter);
    if (counter == 0) {
      close();
    }
  }

  private void close() throws IOException {
    mKiji.release();
  }

  public ColumnNameTranslator getColumnNameTranslator() {
    return mTranslator;
  }
}
