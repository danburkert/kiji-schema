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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.cassandra.CassandraTableName;

/**
 * A Cassandra column name object that corresponds to a single Kiji column, but in a format suitable
 * for persisting into
 *
 */
@ApiAudience.Private
public class CassandraColumnName {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraColumnName.class);

  /** The Cassandra table name. */
  private final CassandraTableName mTable;

  /** The Cassandra column family. */
  private final String mFamily;

  /** The Cassandra column qualifier. */
  private final String mQualifier;

  /**
   *
   * @param table
   * @param family
   * @param qualifier
   */
  public CassandraColumnName(CassandraTableName table, String family, String qualifier) {
    mTable = table;
    mFamily = family;
    mQualifier = qualifier;
  }

  /**
   * Gets the Cassandra table name of this column.
   *
   * @return the Cassandra table name of this column.
   */
  public CassandraTableName getTable() {
    return mTable;
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
   * @return the Cassandra column qualifier of this column.
   */
  public String getQualifier() {
    return mQualifier;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mTable, mFamily, mQualifier);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CassandraColumnName)) {
      return false;
    }
    CassandraColumnName other = (CassandraColumnName) obj;
    return mTable.equals(other.mTable)
        && mFamily.equals(other.mFamily)
        && mQualifier.equals(other.mQualifier);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
        .add("Table", mTable)
        .add("Family", mFamily)
        .add("Qualifier", mQualifier)
        .toString();
  }
}
