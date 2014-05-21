/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.schema.hbase;

import com.google.common.base.Objects;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * An HBase column name.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class HBaseColumnName {
  /** The HBase column family. */
  private final byte[] mFamily;

  /** The HBase column qualifier. */
  private final byte[] mQualifier;

  /**
   * Creates a new <code>HBaseColumnName</code> instance.
   *
   * @param family HBase column family.
   * @param qualifier HBase column qualifier.
   */
  public HBaseColumnName(byte[] family, byte[] qualifier) {
    mFamily = family;
    mQualifier = qualifier;
  }

  /**
   * Gets the HBase column family.
   *
   * @return The family.
   */
  public byte[] getFamily() {
    return mFamily.clone();
  }

  /**
   * Gets the HBase column qualifier.
   *
   * @return The qualifier.
   */
  public byte[] getQualifier() {
    return mQualifier.clone();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("family", Bytes.toStringBinary(mFamily))
        .add("qualifier", Bytes.toStringBinary(mQualifier))
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(mFamily)
        .append(mQualifier)
        .toHashCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HBaseColumnName)) {
      return false;
    }

    HBaseColumnName other = (HBaseColumnName) obj;
    return new EqualsBuilder()
        .append(mFamily, other.mFamily)
        .append(mQualifier, other.mQualifier)
        .isEquals();
  }
}
