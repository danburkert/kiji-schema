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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.LayoutCapsule;

/**
 * A {@link LayoutCapsule} which holds a Kiji table layout, and an
 * {@link CassandraColumnNameTranslator}. All of the warnings about caching which are outlined in
 * {@link LayoutCapsule}'s JavaDoc apply to this class, as well.
 */
@ApiAudience.Private
public final class CassandraLayoutCapsule implements LayoutCapsule<CassandraColumnNameTranslator> {
  private final KijiTableLayout mLayout;
  private final CassandraColumnNameTranslator mTranslator;

  /**
   * Default constructor.
   *
   * @param layout the layout of the table.
   */
  public CassandraLayoutCapsule(final KijiTableLayout layout) {
    Preconditions.checkNotNull(layout);
    mLayout = layout;
    mTranslator = CassandraColumnNameTranslator.from(layout);
  }

  /** {@inheritDoc}. */
  @Override
  public KijiTableLayout getLayout() {
    return mLayout;
  }

  /** {@inheritDoc}. */
  @Override
  public CassandraColumnNameTranslator getColumnNameTranslator() {
    return mTranslator;
  }

  /**
   * A factory function for creating {@link CassandraLayoutCapsule} instances. Useful for handing to
   * callbacks which need to construct a {@link LayoutCapsule} without knowing the specific concrete
   * type.
   */
  public static final class CassandraLayoutCapsuleFactory
      implements Function<KijiTableLayout, CassandraLayoutCapsule> {

    private static final CassandraLayoutCapsuleFactory INSTANCE =
        new CassandraLayoutCapsuleFactory();

    /**
     * Private constructor for singleton object.
     */
    private CassandraLayoutCapsuleFactory() {
    }

    /** {@inheritDoc}. */
    @Override
    public CassandraLayoutCapsule apply(KijiTableLayout layout) {
      return new CassandraLayoutCapsule(layout);
    }

    /**
     * Get an instance of an {@link CassandraLayoutCapsuleFactory}.
     * @return an {@link CassandraLayoutCapsuleFactory}.
     */
    public static CassandraLayoutCapsuleFactory get() {
      return INSTANCE;
    }
  }
}
