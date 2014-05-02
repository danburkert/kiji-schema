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

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.layout.impl.LayoutCapsule;

/**
 * Interface for classes which hold table layout references which must be updated in response to a
 * table layout update.
 *
 * @param <T> concrete type of LayoutCapsule which the LayoutConsumer handles.
 */
@ApiAudience.Private
public interface LayoutConsumer<T extends LayoutCapsule<?>> {

  /**
   * Replace existing layout dependent state in this object with state from the given
   * {@link LayoutCapsule}. The table for which this layout consumer was opened is responsible for
   * calling this method in response to an update to the table layout before the table should report
   * that its update was successful.
   *
   * @param capsule a {@link LayoutCapsule} containing the most recent layout of the table.
   * @throws IOException in case of an error updating.
   */
  void update(T capsule) throws IOException;
}
