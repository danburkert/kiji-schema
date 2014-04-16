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

package org.kiji.schema.layout;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.impl.hbase.IdentityColumnNameTranslator;
import org.kiji.schema.layout.impl.hbase.NativeColumnNameTranslator;
import org.kiji.schema.layout.impl.hbase.ShortColumnNameTranslator;

/**
 * Translates between HBase and Kiji column names.
 *
 * <p>This abstract class defines an interface for the mapping between HBase  families/qualifiers
 * and Kiji locality-group/family/qualifiers.</p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
public abstract class HBaseColumnNameTranslator {
  /**
   * Creates a new {@link HBaseColumnNameTranslator} instance.  Supports either
   * {@link org.kiji.schema.layout.impl.hbase.ShortColumnNameTranslator},
   * {@link org.kiji.schema.layout.impl.hbase.IdentityColumnNameTranslator},
   * {@link org.kiji.schema.layout.impl.hbase.NativeColumnNameTranslator} based on the table layout.
   *
   * @param layout The layout of the table to translate column names for.
   * @return KijiColumnNameTranslator of the appropriate type specified by the layout
   */
  public static HBaseColumnNameTranslator from(KijiTableLayout layout) {
    switch (layout.getDesc().getColumnNameTranslator()) {
      case SHORT:
        return new ShortColumnNameTranslator(layout);
      case IDENTITY:
        return new IdentityColumnNameTranslator(layout);
      case HBASE_NATIVE:
        return new NativeColumnNameTranslator(layout);
      default:
         throw new UnsupportedOperationException(
             String.format("Unsupported column name translator type %s for table %s.",
                 layout.getDesc().getColumnNameTranslator(), layout.getName()));
    }
  }

  /**
   * Translate an HBase column name to a Kiji column name.
   *
   * @param hbaseColumnName The HBase column name.
   * @return The Kiji column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract KijiColumnName toKijiColumnName(HBaseColumnName hbaseColumnName)
      throws NoSuchColumnException;

  /**
   * Translates a Kiji column name to an HBase column name.
   *
   * @param kijiColumnName The Kiji column name.
   * @return The HBase column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract HBaseColumnName toHBaseColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException;

  /**
   * Translates a Kiji LocalityGroup to an HBase family name.
   *
   * @param localityGroup The Kiji locality group.
   * @return The HBase column name.
   */
  public abstract byte[] toHBaseFamilyName(LocalityGroupLayout localityGroup);
}
