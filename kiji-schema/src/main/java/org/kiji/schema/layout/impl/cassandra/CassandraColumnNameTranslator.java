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

package org.kiji.schema.layout.impl.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.layout.KijiTableLayout;

/**
 *
 */
public abstract class CassandraColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraColumnNameTranslator.class);

  /**
   * Creates a new <code>KijiColumnNameTranslator</code> instance.  Supports either
   * {@link org.kiji.schema.layout.impl.ShortColumnNameTranslator},
   * {@link org.kiji.schema.layout.impl.IdentityColumnNameTranslator},
   * {@link org.kiji.schema.layout.impl.HBaseNativeColumnNameTranslator} based on the table layout.
   *
   * @param layout The layout of the table to translate column names for.
   * @return KijiColumnNameTranslator of the appropriate type specified by the layout
   */
  public static CassandraColumnNameTranslator from(KijiTableLayout layout) {
    switch (layout.getDesc().getColumnNameTranslator()) {
      case SHORT:
        return new CassandraShortColumnNameTranslator(layout);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported ColumnNameTranslator: %s for table %s.",
                layout.getDesc().getColumnNameTranslator(), layout.getName()));
    }
  }

  /**
   * Translates a Cassandra column name to a Kiji column name.
   *
   * @param columnName The Cassandra column name.
   * @return The Kiji column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract KijiColumnName toKijiColumnName(CassandraColumnName columnName)
      throws NoSuchColumnException;

  /**
   * Translates a Kiji column name into a Cassandra column name.
   *
   * @param kijiColumnName The Kiji column name.
   * @return The translated Cassandra column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract CassandraColumnName toCassandraColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException;
}
