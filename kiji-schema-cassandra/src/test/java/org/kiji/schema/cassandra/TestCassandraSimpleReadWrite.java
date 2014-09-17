package org.kiji.schema.cassandra;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.impl.cassandra.CassandraKiji;
import org.kiji.schema.impl.cassandra.CassandraKijiTable;
import org.kiji.schema.impl.cassandra.CassandraKijiTableReader;
import org.kiji.schema.layout.KijiTableLayouts;

/**
 *
 */
public class TestCassandraSimpleReadWrite {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraSimpleReadWrite.class);

  private static CassandraKiji mKiji;
  private static CassandraKijiTable mTable;
  private KijiTableWriter mWriter;
  private CassandraKijiTableReader mReader;
  private EntityId mEntityId;

  /** Use to create unique entity IDs for each test case. */
  private static final AtomicInteger TEST_ID_COUNTER = new AtomicInteger(0);
  private static final CassandraKijiClientTest CLIENT_TEST_DELEGATE = new CassandraKijiClientTest();

  @BeforeClass
  public static void initShared() throws Exception {
    CLIENT_TEST_DELEGATE.setupKijiTest();
    mKiji = CLIENT_TEST_DELEGATE.getKiji();

    mKiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_FORMATTED_EID));
    mTable = mKiji.openTable("table");
  }

  @Before
  public final void setupEnvironment() throws Exception {
    // Fill local variables.
    mReader = mTable.openTableReader();
    mWriter = mTable.openTableWriter();
    mEntityId = mTable.getEntityId("eid-" + TEST_ID_COUNTER.getAndIncrement());
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mReader.close();
    mWriter.close();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    mTable.release();
    CLIENT_TEST_DELEGATE.tearDownKijiTest();
  }


  @Test
  public void testBasicReadAndWrite() throws Exception {
    mWriter.put(mEntityId, "family", "column", 0L, "Value at timestamp 0.");
    mWriter.put(mEntityId, "family", "column", 1L, "Value at timestamp 1.");

    final KijiDataRequest dataRequest =
        KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "column"))
            .build();

    // Try this as a get.
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    String s = rowData.getValue("family", "column", 0L).toString();
    Assert.assertEquals(s, "Value at timestamp 0.");

    // Delete the cell and make sure that this value is missing.
    mWriter.deleteCell(mEntityId, "family", "column", 0L);

    rowData = mReader.get(mEntityId, dataRequest);
    Assert.assertFalse(rowData.containsCell("family", "column", 0L));
  }

  @Test
  public void testBasicKijiResult() throws Exception {
    mWriter.put(mEntityId, "family", "column", 0L, "Value at timestamp 0.");
    mWriter.put(mEntityId, "family", "column", 1L, "Value at timestamp 1.");

    final KijiDataRequest dataRequest =
        KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "column"))
            .build();

    // Try this as a get.
    KijiResult<String> kijiResult = mReader.getResult(mEntityId, dataRequest);

    final List<KijiCell<String>> cells1 = ImmutableList.copyOf(kijiResult.iterator());
    final List<KijiCell<String>> cells2 = ImmutableList.copyOf(kijiResult.iterator());

    Assert.assertEquals(2, cells1.size());
    Assert.assertEquals(2, cells2.size());
  }
}
