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
package org.kiji.schema.layout.impl;

import org.kiji.schema.layout.KijiTableLayout;

/**
 * Container class encapsulating the KijiTableLayout and related objects which must all reflect
 * layout updates atomically.  This object represents a snapshot of the table layout at a moment
 * in time which is valuable for maintaining consistency within a short-lived operation.  Because
 * this object represents a snapshot it should not be cached.
 * Does not include CellDecoderProvider or CellEncoderProvider because
 * readers and writers need to be able to override CellSpecs.  Does not include EntityIdFactory
 * because currently there are no valid table layout updates that modify the row key encoding.
 */
public final class LayoutCapsule {
  private final KijiTableLayout mLayout;
  private final ColumnNameTranslator mTranslator;

  /**
   * Default constructor.
   *
   * @param layout the layout of the table.
   * @param translator the ColumnNameTranslator for the given layout.
   */
  public LayoutCapsule(final KijiTableLayout layout, final ColumnNameTranslator translator) {
    mLayout = layout;
    mTranslator = translator;
  }

  /**
   * Get the KijiTableLayout for the associated layout.
   * @return the KijiTableLayout for the associated layout.
   */
  public KijiTableLayout getLayout() {
    return mLayout;
  }

  /**
   * Get the ColumnNameTranslator for the associated layout.
   * @return the ColumnNameTranslator for the associated layout.
   */
  public ColumnNameTranslator getColumnNameTranslator() {
    return mTranslator;
  }
}
