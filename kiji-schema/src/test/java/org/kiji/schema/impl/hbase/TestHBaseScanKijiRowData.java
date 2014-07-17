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

package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Iterator;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowDataTest;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;

/**
 * Test of {@link KijiRowData} for HBase Kiji scans.
 */
public class TestHBaseScanKijiRowData extends KijiRowDataTest {

  /** {@inheritDoc} */
  @Override
  public KijiRowData getRowData(
      final KijiTable table,
      final KijiTableReader reader,
      final EntityId eid,
      final KijiDataRequest dataRequest
  ) throws IOException {
    final KijiScannerOptions options = new KijiScannerOptions();
    options.setStartRow(eid);
    final KijiRowScanner scanner = reader.getScanner(dataRequest, options);
    try {
      final Iterator<KijiRowData> itr = scanner.iterator();
      if (itr.hasNext()) {
        final KijiRowData scanData = itr.next();
        if (scanData.getEntityId().equals(eid)) {
          return scanData;
        }
      }
      // Fall back to normal get (should be empty);
      return reader.get(eid, dataRequest);
    } finally {
      scanner.close();
    }
  }
}
