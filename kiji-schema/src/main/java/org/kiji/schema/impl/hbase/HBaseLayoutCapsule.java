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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;

public class HBaseLayoutCapsule implements LayoutCapsule {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseLayoutCapsule.class);

  @Override
  public KijiTableLayout getLayout() {
    return null;
  }

  @Override
  public KijiColumnNameTranslator getKijiColumnNameTranslator() {
    return null;
  }

  /**
   * Get the ColumnNameTranslator for the associated layout.
   * @return the ColumnNameTranslator for the associated layout.
   */
  KijiColumnNameTranslator getKijiColumnNameTranslator();
}
