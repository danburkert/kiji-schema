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

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * An interface for container classes which hold a {@link KijiTableLayout} and a column name
 * translator. Implementations of {@code LayoutCapsule} are purely data containers, and are
 * immutable. {@code LayoutCapsule}s and the layouts and column name translators they hold should
 * *not* be cached users; instead, a new {@code LayoutCapsule} should be requested from a
 * {@link TableLayoutMonitor} each time a layout or column name translator is needed.
 * Alternatively, a user can cache a {@code LayoutCapsule} if they register a callback with the
 * {@link TableLayoutMonitor} which invalidates the cached copy upon table layout change.
 *
 * The {@code LayoutCapsule} does not include a {@link CellDecoderProvider} or
 * {@link CellEncoderProvider} because readers and writers need the flexibility to override
 * {@link org.kiji.schema.layout.CellSpec}s.
 *
 * The {@code LayoutCapsule} does not include an {@link org.kiji.schema.EntityIdFactory}, because
 * currently there are no valid table layout updates which modify the row key encoding.  Therefore,
 * {@link org.kiji.schema.EntityIdFactory} instances can be cached on a per-table basis (unlike
 * {@link KijiTableLayout}s, {@code ColumnNameTranslator}s, {@link CellDecoderProvider}s,
 * and {@link CellEncoderProvider}s).
 *
 * @param <T> type of column name translator held by this layout capsule.
 */
@ApiAudience.Private
public interface LayoutCapsule<T> {

  /**
   * Get the Kiji table layout held by this layout capsule.
   *
   * @return the Kiji table layout held by this layout capsule.
   */
  KijiTableLayout getLayout();

  /**
   * Get the column name translator held by this layout capsule.
   *
   * @return the column name translator held by this layout capsule.
   */
  T getColumnNameTranslator();
}
