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
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.impl.hbase.HBaseNativeColumnNameTranslator;
import org.kiji.schema.layout.impl.hbase.IdentityColumnNameTranslator;
import org.kiji.schema.layout.impl.hbase.ShortColumnNameTranslator;

/**
 * Translates {@link KijiColumnName}s to {@link TranslatedColumnName}s and vice-versa.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public abstract class KijiColumnNameTranslator {
  /**
   * Creates a new {@link KijiColumnNameTranslator} instance. Supports either
   * {@link ShortColumnNameTranslator}, {@link IdentityColumnNameTranslator}, or
   * {@link HBaseNativeColumnNameTranslator} based on the table layout.
   *
   * @param tableLayout The layout of the table to translate column names for.
   * @return KijiColumnNameTranslator of the appropriate type specified by the layout.
   */
  public static KijiColumnNameTranslator from(KijiTableLayout tableLayout) {
    switch (tableLayout.getDesc().getColumnNameTranslator()) {
    case SHORT:
      return new ShortColumnNameTranslator(tableLayout);
    case IDENTITY:
      return new IdentityColumnNameTranslator(tableLayout);
    case HBASE_NATIVE:
      return new HBaseNativeColumnNameTranslator(tableLayout);
    default:
       throw new UnsupportedOperationException(
           String.format("Unsupported ColumnNameTranslator: %s for column: %s.",
               tableLayout.getDesc().getColumnNameTranslator(),
               tableLayout.getName()));
    }
  }

  /**
   * Translate a {@link TranslatedColumnName} to a {@link KijiColumnName}.
   *
   * @param translatedColumnName The translated column name.
   * @return The Kiji column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract KijiColumnName toKijiColumnName(TranslatedColumnName translatedColumnName)
      throws NoSuchColumnException;

  /**
   * Translates a {@link KijiColumnName} to a {@link TranslatedColumnName}.
   *
   * @param kijiColumnName The Kiji column name.
   * @return The translated column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public abstract TranslatedColumnName toTranslatedColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException;

  /**
   * Translates a Kiji locality group to the translated name.
   *
   * <p>In the case of HBase, this translated name corresponds to the HBase column family.</p>
   *
   * @param localityGroup The Kiji locality group.
   * @return The translated locality group.
   */
  public abstract byte[] translateLocalityGroup(LocalityGroupLayout localityGroup);
}
