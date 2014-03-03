package org.kiji.schema.impl;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.junit.Test;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.HBaseKiji;
import org.kiji.schema.impl.HBaseTableLayoutUpdater;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.TableLayoutBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHBaseTableLayoutUpdater extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseTableLayoutUpdater.class);

  /** Tests behavior of Avro enums. */
  @Test
  public void testFoo() throws Exception {
    final HBaseKiji kiji = (HBaseKiji) getKiji();

    TableLayoutDesc layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.SIMPLE).getDesc();
    layout.setVersion("layout-1.4.0");
    kiji.createTable(layout);

    HBaseTableLayoutUpdater updater = new HBaseTableLayoutUpdater(kiji, kiji.getURI(), layout);
    updater.getNewLayout()
    updater.update();
    updater.close();
  }

}
