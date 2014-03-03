/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.schema.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.BlockingQueue;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.ZooKeeperMonitor;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.util.ProtocolVersion;

public class IntegrationTestTableLayoutUpdate extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestTableLayoutUpdate.class);

  private static void setup(
      final KijiURI uri,
      final String initialLayoutID,
      final ProtocolVersion version) throws Exception {
    final Kiji kiji = Kiji.Factory.open(uri);
    try {
      // Create the table with layoutId1
      final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
      layoutDesc.setLayoutId(initialLayoutID);
      kiji.createTable(layoutDesc);

      // Update the system version
      kiji.getSystemTable().setDataVersion(version);
    } finally {
      kiji.release();
    }
  }

  @Test
  public void testUpdateLayout() throws Exception {
    final String tableName = "foo";
    final KijiURI tableURI = KijiURI.newBuilder(getKijiURI()).withTableName(tableName).build();
    final String layoutId1 = "1";
    final String layoutId2 = "2";

    setup(tableURI, layoutId1, Versions.MIN_SYS_VER_FOR_LAYOUT_VALIDATION);


    final HBaseKiji kiji1 = (HBaseKiji) Kiji.Factory.open(tableURI);
    final HBaseKiji kiji2 = (HBaseKiji) Kiji.Factory.open(tableURI);

    final CuratorFramework zkClient = kiji1.getZKClient();

    try {
      ZooKeeperMonitor.setTableLayout(zkClient, tableURI, layoutId1);
      final BlockingQueue<Multimap<String, String>> queue = Queues.newSynchronousQueue();

      final ZooKeeperMonitor.TableUsersTracker tracker =
          ZooKeeperMonitor.newTableUsersTracker(zkClient, tableURI,
              new ZooKeeperMonitor.TableRegistrationHandler() {
                /** {@inheritDoc} */
                @Override
                public void update(Multimap<String, String> users) {
                  LOG.info("User map update: {}", users);
                  try {
                    queue.put(users);
                  } catch (InterruptedException ie) {
                    throw new RuntimeInterruptedException(ie);
                  }
                }
              });
      tracker.start();
      try {
        // Initial user map should be empty:
        assertTrue(queue.take().isEmpty());

        final KijiTable table1 = kiji1.openTable(tableName);
        try {
          {
            // We opened one table, user map must contain exactly one entry:
            final Multimap<String, String> umap = queue.take();
            assertEquals(1, umap.size());
            assertEquals(layoutId1, umap.values().iterator().next());
          }

          // Open another table on a second Kiji in order to have a second table user registered
          final KijiTable table2 = kiji2.openTable(tableName);
          try {
            // Push a layout update (a no-op, but with a new layout ID):
            final TableLayoutDesc newLayoutDesc =
                KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
            newLayoutDesc.setReferenceLayout(layoutId1);
            newLayoutDesc.setLayoutId(layoutId2);
            kiji1.modifyTableLayout(newLayoutDesc);

            // The new user map should eventually reflect the new layout ID.
            // There may be, but not always, a transition set where both layout IDs are visible.
            Multimap<String, String> userMap = ImmutableMultimap.of();
            while (true) {
              Multimap<String, String> head = queue.poll();
              if (head != null) {
                userMap = head;
              } else {
                break;
              }
            }
            assertEquals(2, userMap.size());
            assertEquals(layoutId2, userMap.values().iterator().next());
          } finally {
            table2.release();
          }
        } finally {
          table1.release();
        }
        // Table is now closed, the user map should become empty:
        assertTrue(queue.take().isEmpty());
      } finally {
        tracker.close();
      }
    } finally {
      kiji1.release();
      kiji2.release();
    }
  }
}
