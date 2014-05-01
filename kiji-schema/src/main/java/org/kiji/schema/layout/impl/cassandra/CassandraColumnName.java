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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * A Cassandra column name object that corresponds to a single Kiji column, but in a format suitable
 * for persisting into Cassandra.
 *
 */
@ApiAudience.Private
public class CassandraColumnName {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraColumnName.class);

  /** The column's locality group. */
  private final String mLocalityGroup;

  /** The Cassandra column family. */
  private final String mFamily;

  /** The Cassandra column qualifier. */
  private final String mQualifier;

  /**
   * Create a CassandraColumnName with the provided locality group, family, and qualifier.
   *
   * @param localityGroup of column.
   * @param family of column.
   * @param qualifier of column, or null if unqualified column.
   */
  public CassandraColumnName(String localityGroup, String family, String qualifier) {
    Preconditions.checkNotNull(localityGroup, "Must specify a locality group.");
    Preconditions.checkNotNull(family, "Must specify a family.");
    mLocalityGroup = localityGroup;
    mFamily = family;
    mQualifier = qualifier;
  }

  /**
   * Gets the locality group name of this column.
   *
   * Note: If the locality group is translated by the
   * {@link org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator}, this will be
   * the translated version.
   *
   * @return the locality group name of this column.
   */
  public String getLocalityGroup() {
    return mLocalityGroup;
  }

  /**
   * Gets the Cassandra column family of this column.
   *
   * Note: If the family is translated by the
   * {@link org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator}, this will be
   * the translated version.
   *
   * @return the Cassandra column family of this column.
   */
  public String getFamily() {
    return mFamily;
  }

  /**
   * Gets the Cassandra column qualifier of this column.
   *
   * Note: If the qualifier is translated by the
   * {@link org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator}, this will be
   * the translated version.
   *
   * @return the Cassandra column qualifier of this column, or null if unqualified column.
   */
  public String getQualifier() {
    return mQualifier;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLocalityGroup, mFamily, mQualifier);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CassandraColumnName)) {
      return false;
    }
    CassandraColumnName other = (CassandraColumnName) obj;
    return Objects.equal(mLocalityGroup, other.mLocalityGroup)
        && Objects.equal(mFamily, other.mFamily)
        && Objects.equal(mQualifier, other.mQualifier);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
        .add("Locality Group", mLocalityGroup)
        .add("Family", mFamily)
        .add("Qualifier", mQualifier)
        .toString();
  }
}
