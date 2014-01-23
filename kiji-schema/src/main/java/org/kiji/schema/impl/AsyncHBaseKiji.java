package org.kiji.schema.impl;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.hbase.async.HBaseClient;
import org.kiji.schema.*;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.security.KijiSecurityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

public class AsyncHBaseKiji implements Kiji {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncHBaseKiji.class);

  private final Kiji mHbaseKiji;
  private final HBaseClient mAsyncClient;

  public AsyncHBaseKiji(Kiji hbaseKiji) {
    mHbaseKiji = hbaseKiji;
    KijiURI uri = mHbaseKiji.getURI();
    mAsyncClient = new HBaseClient(
        Joiner
            .on(":" + uri.getZookeeperClientPort() + ",")
            .join(mHbaseKiji.getURI().getZookeeperQuorum())
        + ":" + uri.getZookeeperClientPort());
  }

  @Override
  public Configuration getConf() {
    throw new AssertionError();
  }

  @Override
  public KijiURI getURI() {
    return mHbaseKiji.getURI();
  }

  @Override
  public KijiSchemaTable getSchemaTable() throws IOException {
    return mHbaseKiji.getSchemaTable();
  }

  @Override
  public KijiSystemTable getSystemTable() throws IOException {
    return mHbaseKiji.getSystemTable();
  }

  @Override
  public KijiMetaTable getMetaTable() throws IOException {
    return mHbaseKiji.getMetaTable();
  }

  @Override
  public boolean isSecurityEnabled() throws IOException {
    return mHbaseKiji.isSecurityEnabled();
  }

  @Override
  public KijiSecurityManager getSecurityManager() throws IOException {
    return mHbaseKiji.getSecurityManager();
  }

  @Override
  public void createTable(String tableName, KijiTableLayout tableLayout) throws IOException {
    mHbaseKiji.createTable(tableName, tableLayout);
  }

  @Override
  public void createTable(String name, KijiTableLayout tableLayout, int numRegions) throws IOException {
    mHbaseKiji.createTable(name, tableLayout, numRegions);
  }

  @Override
  public void createTable(String name, KijiTableLayout tableLayout, byte[][] splitKeys) throws IOException {
    mHbaseKiji.createTable(name, tableLayout, splitKeys);
  }

  @Override
  public void createTable(TableLayoutDesc tableLayout) throws IOException {
    mHbaseKiji.createTable(tableLayout);
  }

  @Override
  public void createTable(TableLayoutDesc tableLayout, int numRegions) throws IOException {
    mHbaseKiji.createTable(tableLayout, numRegions);
  }

  @Override
  public void createTable(TableLayoutDesc tableLayout, byte[][] splitKeys) throws IOException {
    mHbaseKiji.createTable(tableLayout, splitKeys);
  }

  @Override
  public void deleteTable(String name) throws IOException {
    mHbaseKiji.deleteTable(name);
  }

  @Override
  public List<String> getTableNames() throws IOException {
    return mHbaseKiji.getTableNames();
  }

  @Override
  public KijiTableLayout modifyTableLayout(String name, TableLayoutDesc update) throws IOException {
    return mHbaseKiji.modifyTableLayout(name, update);
  }

  @Override
  public KijiTableLayout modifyTableLayout(TableLayoutDesc update) throws IOException {
    return mHbaseKiji.modifyTableLayout(update);
  }

  @Override
  public KijiTableLayout modifyTableLayout(String name, TableLayoutDesc update, boolean dryRun, PrintStream printStream) throws IOException {
    return mHbaseKiji.modifyTableLayout(name, update, dryRun, printStream);
  }

  @Override
  public KijiTableLayout modifyTableLayout(TableLayoutDesc update, boolean dryRun, PrintStream printStream) throws IOException {
    return mHbaseKiji.modifyTableLayout(update, dryRun, printStream);
  }

  @Override
  public AsyncHBaseKijiTable openTable(String tableName) throws IOException {
    KijiURI uri = KijiURI.newBuilder(mHbaseKiji.getURI()).withTableName(tableName).build();
    return new AsyncHBaseKijiTable(mAsyncClient, this,  mHbaseKiji.openTable(tableName), uri);
  }

  @Override
  public Kiji retain() {
    mHbaseKiji.retain();
    return this;
  }

  @Override
  public void release() throws IOException {
    mHbaseKiji.release();
  }
}
