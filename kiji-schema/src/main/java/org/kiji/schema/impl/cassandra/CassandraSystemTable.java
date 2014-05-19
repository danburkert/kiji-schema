/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.SystemTableBackup;
import org.kiji.schema.avro.SystemTableEntry;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.util.ByteUtils;
import org.kiji.schema.util.CloseableIterable;
import org.kiji.schema.util.ProtocolVersion;

/**
 * <p>The Kiji system table that is stored in Cassandra.</p>
 *
 * <p>The system table (a Kiji system table) is a simple key-value store for system-wide
 * properties of a Kiji installation.  There is a single column family "value".  For a
 * key-value property (K,V), the key K is stored as the row key in the Cassandra table,
 * and the value V is stored in the "value:" column.<p>
 */
@ApiAudience.Private
public final class CassandraSystemTable implements KijiSystemTable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSystemTable.class);

  /** The Cassandra column family that stores the value of the properties. */
  public static final String KEY_COLUMN = "key";
  public static final String VALUE_COLUMN = "value";

  /** The Cassandra row key that stores the installed Kiji data format version. */
  public static final String KEY_DATA_VERSION = "data-version";

  /** The Cassandra row key that stores the Kiji security version. */
  public static final String SECURITY_PROTOCOL_VERSION = "security-version";

  /**
   * The name of the file that stores the current system table defaults that are loaded
   * at installation time.
   */
  public static final String DEFAULTS_PROPERTIES_FILE =
      "org/kiji/schema/system-default.properties";

  /** URI of the Kiji instance this system table belongs to. */
  private final KijiURI mInstanceURI;

  /** The Cassandra table that stores the Kiji instance properties. */
  private final CassandraTableName mTable;

  private final PreparedStatement mPreparedStatementGetValue;

  private final PreparedStatement mPreparedStatementPutValue;

  private final CassandraAdmin mAdmin;

  /**
   * Wrap an existing Cassandra table that is assumed to be the table that stores the
   * Kiji instance properties.
   *
   * @param instanceURI URI of the Kiji instance this table belongs to.
   * @param admin Cassandra connection.
   */
  public CassandraSystemTable(KijiURI instanceURI, CassandraAdmin admin) {
    mInstanceURI = Preconditions.checkNotNull(instanceURI);
    mTable = CassandraTableName.getSystemTableName(mInstanceURI);
    mAdmin = Preconditions.checkNotNull(admin);

    if (!mAdmin.tableExists(mTable)) {
      throw new KijiNotInstalledException(
          String.format(
              "System table for Kiji instance %s is not installed.",
              mInstanceURI.getInstance()),
          mInstanceURI);
    }

    final String selectText =
        String.format("SELECT %s FROM %s WHERE %s=?;", VALUE_COLUMN, mTable, KEY_COLUMN);
    mPreparedStatementGetValue = mAdmin.getPreparedStatement(selectText);

    final String insertText =
        String.format("INSERT INTO %s (%s, %s) values (?, ?);", mTable, KEY_COLUMN, VALUE_COLUMN);
    mPreparedStatementPutValue = mAdmin.getPreparedStatement(insertText);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ProtocolVersion getDataVersion() throws IOException {
    byte[] result = getValue(KEY_DATA_VERSION);
    return result == null ? null : ProtocolVersion.parse(Bytes.toString(result));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void setDataVersion(ProtocolVersion version) throws IOException {
    putValue(KEY_DATA_VERSION, Bytes.toBytes(version.toString()));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ProtocolVersion getSecurityVersion() throws IOException {
    byte[] result = getValue(SECURITY_PROTOCOL_VERSION);
    return result == null
        ? Versions.UNINSTALLED_SECURITY_VERSION
        : ProtocolVersion.parse(Bytes.toString(result));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void setSecurityVersion(ProtocolVersion version) throws IOException {
    Preconditions.checkNotNull(version);
    Kiji.Factory.open(mInstanceURI).getSecurityManager().checkCurrentGrantAccess();
    putValue(SECURITY_PROTOCOL_VERSION, Bytes.toBytes(version.toString()));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    // No owned resources
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getValue(String key) throws IOException {
    LOG.info(String.format("Reading value for key = %s", key));
    ResultSet resultSet = mAdmin.execute(mPreparedStatementGetValue.bind(key));

    // Extra the value from the byte buffer, otherwise return this empty buffer
    // TODO: Some additional sanity checks here?
    List<Row> rows = resultSet.all();
    Preconditions.checkArgument(
        rows.size() <= 1,
        "Expected to get back exactly zero or one rows from query to get value from system table, "
          + "but got back " + rows
    );
    if (rows.size() == 1) {
      return ByteUtils.toBytes(rows.get(0).getBytes(VALUE_COLUMN));
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void putValue(String key, byte[] value) throws IOException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);

    // TODO: Check for success?
    ByteBuffer valBytes = ByteBuffer.wrap(value);
    mAdmin.execute(mPreparedStatementPutValue.bind(key, valBytes));
  }

  /** {@inheritDoc} */
  @Override
  public CloseableIterable<SimpleEntry<String, byte[]>> getAll() throws IOException {
    // TODO: Make this a prepared query.
    String query = String.format("Select * FROM %s;", mTable);
    ResultSet resultSet = mAdmin.execute(query);

    // Extra the value from the byte buffer, otherwise return this empty buffer
    // TODO: Some checks here?
    return new CassandraSystemTableIterable(resultSet);
  }

  /**
   * Loads a map of properties from the properties file named by resource.
   *
   * @param resource The name of the properties resource holding the defaults.
   * @return The properties in the file as a Map.
   * @throws java.io.IOException If there is an error.
   */
  public static Map<String, String> loadPropertiesFromFileToMap(String resource)
      throws IOException {
    final Properties defaults = new Properties();
    defaults.load(CassandraSystemTable.class.getClassLoader().getResourceAsStream(resource));
    return Maps.fromProperties(defaults);
  }

  /**
   * Load the system table with the key/value pairs specified in properties.  Default properties are
   * loaded for any not specified.
   *
   * @param properties The properties to load into the system table.
   * @throws java.io.IOException If there is an I/O error.
   */
  protected void loadSystemTableProperties(Map<String, String> properties) throws IOException {
    final Map<String, String> defaults = loadPropertiesFromFileToMap(DEFAULTS_PROPERTIES_FILE);
    final Map<String, String> newProperties = Maps.newHashMap(defaults);
    newProperties.putAll(properties);
    for (Map.Entry<String, String> entry : newProperties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      putValue(key, Bytes.toBytes(value));
    }
  }

  /**
   * Installs a Kiji system table into a running HBase instance.
   *
   * @param admin The HBase cluster to install into.
   * @param kijiURI The KijiURI.
   * @throws java.io.IOException If there is an error.
   */
  public static void install(
      CassandraAdmin admin,
      KijiURI kijiURI)
      throws IOException {
    install(admin, kijiURI, Collections.<String, String>emptyMap());
  }

  /**
   * Installs a Kiji system table into a running C* instance.
   *
   * @param admin The Cassandra cluster and keyspace install into.
   * @param kijiURI The KijiURI.
   * @param properties The initial system properties to be used in addition to the defaults.
   * @throws java.io.IOException If there is an error.
   */
  public static void install(CassandraAdmin admin, KijiURI kijiURI, Map<String, String> properties)
      throws IOException {
    // Install the table.  Sadly, we have to just use blobs and byte arrays here, so that we are
    // compliant with everything else in Kiji.  :(
    final CassandraTableName systemTableName = CassandraTableName.getSystemTableName(kijiURI);

    // The layout of this table is straightforward - just blob to blob!
    // TODO: Any check here first for whether the table exists?
    String tableLayout =
        String.format("CREATE TABLE %s (%s text PRIMARY KEY, %s blob);",
            systemTableName, KEY_COLUMN, VALUE_COLUMN);

    admin.createTable(systemTableName, tableLayout);
    CassandraSystemTable systemTable = new CassandraSystemTable(kijiURI, admin);
    try {
      systemTable.loadSystemTableProperties(properties);
    } finally {
      systemTable.close();
    }
  }

  /**
   * Disables and delete the system table from HBase.
   *
   * @param admin The HBase admin object.
   * @param kijiURI The URI for the kiji instance to remove.
   * @throws java.io.IOException If there is an error.
   */
  public static void uninstall(CassandraAdmin admin, KijiURI kijiURI)
      throws IOException {
    // TODO: Does this actually need to do anything beyond dropping the table?
    final CassandraTableName tableName = CassandraTableName.getSystemTableName(kijiURI);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public SystemTableBackup toBackup() throws IOException {
    ArrayList<SystemTableEntry> backupEntries = new ArrayList<SystemTableEntry>();
    CloseableIterable<SimpleEntry<String, byte[]>> entries = getAll();
    for (SimpleEntry<String, byte[]> entry : entries) {
      backupEntries.add(SystemTableEntry.newBuilder()
          .setKey(entry.getKey())
          .setValue(ByteBuffer.wrap(entry.getValue()))
          .build());
    }

    return SystemTableBackup.newBuilder().setEntries(backupEntries).build();
  }

  /** {@inheritDoc} */
  @Override
  public void fromBackup(SystemTableBackup backup) throws IOException {
    LOG.info(String.format("Restoring system table from backup with %d entries.",
        backup.getEntries().size()));
    for (SystemTableEntry entry : backup.getEntries()) {
      putValue(entry.getKey(), entry.getValue().array());
    }
  }

  /** Private class for providing a CloseableIterable over system table key, value pairs. */
  private static class CassandraSystemTableIterable
      implements CloseableIterable<SimpleEntry<String, byte[]>> {

    /** Iterator returned by iterator(). */
    private Iterator<SimpleEntry<String, byte[]>> mIterator;

    /**
     * Create a new CassandraSystemTableIterable across system table properties.
     *
     * @param resultSet scanner across the target cells.
     */
    public CassandraSystemTableIterable(ResultSet resultSet) {
      mIterator = new CassandraSystemTableIterator(resultSet.iterator());
      //mResultScanner = resultScanner;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<SimpleEntry<String, byte[]>> iterator() {
      return mIterator;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      // No owned resources
    }
  }

  /** Private class for providing an Iterator to HBaseSystemTableIterable. */
  private static class CassandraSystemTableIterator
      implements Iterator<SimpleEntry<String, byte[]>> {

    /**
     * Iterator across result scanner results.
     * Used to build next() for HBaseSystemTableIterator
     */
    private Iterator<Row> mRowIterator;

    /**
     * Create an HBaseSystemTableIterator across the results of a ResultScanner.
     *
     * @param rowIterator iterator across the scanned cells.
     */
    public CassandraSystemTableIterator(Iterator<Row> rowIterator) {
      mRowIterator = rowIterator;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return mRowIterator.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public SimpleEntry<String, byte[]> next() {
      Row next = mRowIterator.next();
      String key = next.getString(KEY_COLUMN);
      byte[] value = ByteUtils.toBytes(next.getBytes(VALUE_COLUMN));
      return new SimpleEntry<String, byte[]>(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraSystemTable.class)
        .add("uri", mInstanceURI)
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  public KijiURI getKijiURI() {
    return mInstanceURI;
  }
}
