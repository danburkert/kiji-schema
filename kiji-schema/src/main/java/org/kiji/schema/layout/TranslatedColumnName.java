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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * A translated column name.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class TranslatedColumnName {
  /** The untranslated locality group. This should only be used in Cassandra-specific code. */
  private final String mLocalityGroup;

  /** The translated column family. */
  private final byte[] mFamily;

  /** The translated column qualifier. */
  private final byte[] mQualifier;

  /**
   * Creates a new {@code TranslatedColumnName} instance.
   *
   * @param family translated column family.
   * @param qualifier translated column qualifier.
   */
  public TranslatedColumnName(byte[] family, byte[] qualifier) {
    mLocalityGroup = null;
    mFamily = Preconditions.checkNotNull(family);
    mQualifier = Preconditions.checkNotNull(qualifier);
  }

  /**
   * Creates a new {@link TranslatedColumnName} instance with the provided Kiji locality group. The
   * locality group is used by Cassandra to know which Cassandra table the
   * {@link TranslatedColumnName} belongs to. The locality group should match the name of the
   * locality group in the Kiji table layout exactly (no translation or shortening).
   *
   * @param localityGroup Kiji locality group.
   * @param family translated column family.
   * @param qualifier translated column qualifier.
   */
  public TranslatedColumnName(String localityGroup, byte[] family, byte[] qualifier) {
    mLocalityGroup = Preconditions.checkNotNull(localityGroup);
    mFamily = Preconditions.checkNotNull(family);
    mQualifier = Preconditions.checkNotNull(qualifier);
  }

  /**
   * Gets the translated column family. Do *not* modify the returned array.
   *
   * @return The family.
   */
  public byte[] getFamily() {
    return mFamily;
  }

  /**
   * Gets the translated column qualifier. Do *not* modify the returned array.
   *
   * @return The qualifier.
   */
  public byte[] getQualifier() {
    return mQualifier;
  }

  /**
   * Gets the Kiji locality group.
   *
   * @return the Kiji locality group.
   */
  public String getLocalityGroup() {
    return mLocalityGroup;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("locality_group", mLocalityGroup)
        .add("family", Bytes.toStringBinary(mFamily))
        .add("qualifier", Bytes.toStringBinary(mQualifier))
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(mLocalityGroup)
        .append(mFamily)
        .append(mQualifier)
        .toHashCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TranslatedColumnName)) {
      return false;
    }

    TranslatedColumnName other = (TranslatedColumnName) obj;
    return new EqualsBuilder()
        .append(mLocalityGroup, other.mLocalityGroup)
        .append(mFamily, other.mFamily)
        .append(mQualifier, other.mQualifier)
        .isEquals();
  }
}
