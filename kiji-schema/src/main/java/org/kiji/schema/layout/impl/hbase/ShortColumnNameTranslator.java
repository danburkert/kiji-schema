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

package org.kiji.schema.layout.impl.hbase;

import com.google.common.base.Objects;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.TranslatedColumnName;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * {@inheritDoc}
 *
 * <p>This class defines a mapping between names of HBase families/qualifiers and
 * Kiji locality group/family/qualifiers using a custom base-64 encoding based on the ids of each
 * individual column.</p>
 */
@ApiAudience.Private
public final class ShortColumnNameTranslator extends KijiColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(ShortColumnNameTranslator.class);

  /** Used to separate the Kiji family from the Kiji qualifier in an translated qualifier. */
  private static final byte SEPARATOR = ':';

  /** The table to translate names for. */
  private final KijiTableLayout mLayout;

  /**
   * Creates a new {@link ShortColumnNameTranslator} instance.
   *
   * @param tableLayout The layout of the table to translate column names for.
   */
  public ShortColumnNameTranslator(KijiTableLayout tableLayout) {
    mLayout = tableLayout;
  }

  /** {@inheritDoc} */
  @Override
  public KijiColumnName toKijiColumnName(TranslatedColumnName translatedColumnName)
      throws NoSuchColumnException {
    LOG.debug("Translating column name '{}' to Kiji column name...", translatedColumnName);
    final ColumnId localityGroupID = ColumnId.fromByteArray(translatedColumnName.getFamily());
    final LocalityGroupLayout localityGroup =
        mLayout.getLocalityGroupMap().get(mLayout.getLocalityGroupIdNameMap().get(localityGroupID));
    if (localityGroup == null) {
      throw new NoSuchColumnException(String.format("No locality group with ID %s in table %s.",
          localityGroupID.getId(), mLayout.getName()));
    }

    // Parse the translated qualifier as a byte[] in order to save a String instantiation
    final byte[] translatedQualifier = translatedColumnName.getQualifier();
    final int index = ArrayUtils.indexOf(translatedQualifier, SEPARATOR);
    if (index == -1) {
      throw new NoSuchColumnException(String.format(
          "Missing separator in column %s.", translatedColumnName));
    }
    final ColumnId familyID = ColumnId.fromString(Bytes.toString(translatedQualifier, 0, index));
    final String rawQualifier =
        Bytes.toString(translatedQualifier, index + 1, translatedQualifier.length - index - 1);

    final FamilyLayout family =
        localityGroup.getFamilyMap().get(localityGroup.getFamilyIdNameMap().get(familyID));
    if (family == null) {
      throw new NoSuchColumnException(String.format(
          "No family with ID %s in locality group %s of table %s.",
          familyID.getId(), localityGroup.getName(), mLayout.getName()));
    }

    if (family.isGroupType()) {
      // Group type family.
      final ColumnId qualifierID = ColumnId.fromString(rawQualifier);
      final ColumnLayout qualifier =
          family.getColumnMap().get(family.getColumnIdNameMap().get(qualifierID));
      if (qualifier == null) {
        throw new NoSuchColumnException(String.format(
            "No column with ID %s in family %s of table %s.",
            qualifierID.getId(), family.getName(), mLayout.getName()));
      }
      final KijiColumnName kijiColumnName =
          new KijiColumnName(family.getName(), qualifier.getName());
      LOG.debug("Translated to Kiji group column {}.", kijiColumnName);
      return kijiColumnName;
    } else {
      // Map type family.
      assert family.isMapType();
      final KijiColumnName kijiColumnName = new KijiColumnName(family.getName(), rawQualifier);
      LOG.debug("Translated to Kiji map column '{}'.", kijiColumnName);
      return kijiColumnName;
    }
  }

  /** {@inheritDoc} */
  @Override
  public TranslatedColumnName toTranslatedColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException {
    final String familyName = kijiColumnName.getFamily();
    final String qualifierName = kijiColumnName.getQualifier();

    final FamilyLayout family = mLayout.getFamilyMap().get(familyName);
    if (family == null) {
      throw new NoSuchColumnException(kijiColumnName.toString());
    }

    final ColumnId localityGroupID = family.getLocalityGroup().getId();
    final ColumnId familyID = family.getId();

    final byte[] localityGroupBytes = Bytes.toBytes(localityGroupID.toString());
    final byte[] familyBytes = Bytes.toBytes(familyID.toString());

    if (qualifierName == null) {
      // Unqualified column
      return new TranslatedColumnName(localityGroupBytes,
          concatWithSeparator(SEPARATOR, familyBytes, new byte[]{}));
    } else if (family.isGroupType()) {
      // Group type family.
      final ColumnId qualifierID = family.getColumnIdNameMap().inverse().get(qualifierName);
      final byte[] qualifierBytes = Bytes.toBytes(qualifierID.toString());

      return new TranslatedColumnName(localityGroupBytes,
          concatWithSeparator(SEPARATOR, familyBytes, qualifierBytes));
    } else {
      // Map type family.
      assert family.isMapType();
      final byte[] qualifierBytes = Bytes.toBytes(qualifierName);

      return new TranslatedColumnName(
          localityGroupBytes,
          concatWithSeparator(SEPARATOR, familyBytes, qualifierBytes));
    }
  }

  /** {@inheritDoc} */
  @Override
  public byte[] translateLocalityGroup(LocalityGroupLayout localityGroup) {
    return Bytes.toBytes(localityGroup.getId().toString());
  }

  /**
   * Append the byte arrays with a single byte separator.
   *
   * @param separator to insert between the two arrays.
   * @param a first byte array.
   * @param b second byte array.
   * @return a concatenated with b with a separator byte in between.
   */
  static byte[] concatWithSeparator(byte separator, byte[] a, byte[] b) {
    final byte[] ret = new byte[a.length + b.length + 1];
    System.arraycopy(
        a, 0,      // src, src position
        ret, 0,    // dst, dst position
        a.length); // length
    ret[a.length] = separator;
    System.arraycopy(
        b, 0,              // src, src position
        ret, a.length + 1, // dst, dst position
        b.length);         // length
    return ret;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("table", mLayout.getName())
        .add("layout", mLayout)
        .toString();
  }
}
