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

    import com.google.common.base.Charsets;
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
    import org.kiji.schema.util.ByteUtils;

/**
 * Translates Kiji column names into shorter families and qualifiers.
 *
 * TODO: translation could be probably benefit from an LRU cache
 */
@ApiAudience.Private
public final class ShortColumnNameTranslator extends KijiColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(ShortColumnNameTranslator.class);

  private final KijiTableLayout mLayout;

  /**
   * Creates a new <code>TranslatedColumnNameTranslator</code> instance.
   *
   * @param layout The layout of the table to translate column names for.
   */
  public ShortColumnNameTranslator(KijiTableLayout layout) {
    mLayout = layout;
  }

  /** {@inheritDoc}. */
  @Override
  public KijiColumnName toKijiColumnName(TranslatedColumnName columnName)
      throws NoSuchColumnException {
    LOG.debug("Translating Cassandra column {} to Kiji column name.", columnName);

    final ColumnId familyID = ColumnId.fromByteArray(columnName.getFamily());

    final LocalityGroupLayout localityGroup =
        mLayout.getLocalityGroupMap().get(columnName.getLocalityGroup());
    if (localityGroup == null) {
      throw new NoSuchColumnException(String.format(
          "No locality group %s in table %s.", columnName.getLocalityGroup(), mLayout.getName()));
    }

    final FamilyLayout family =
        localityGroup.getFamilyMap().get(localityGroup.getFamilyIdNameMap().get(familyID));
    if (family == null) {
      throw new NoSuchColumnException(String.format(
          "No column family with ID %s in locality group %s of table %s.",
          familyID, localityGroup.getName(), mLayout.getName()));
    }

    if (family.isGroupType()) {
      // Group type family.
      final ColumnId qualifierID = ColumnId.fromByteArray(columnName.getQualifier());
      final ColumnLayout qualifier =
          family.getColumnMap().get(family.getColumnIdNameMap().get(qualifierID));
      if (qualifier == null) {
        throw new NoSuchColumnException(String.format(
            "No qualifier with ID %s in family %s of locality group %s of table %s.",
            qualifierID, family.getName(), localityGroup.getName(), mLayout.getName()));
      }
      final KijiColumnName column = new KijiColumnName(family.getName(), qualifier.getName());
      LOG.debug("Translated to Kiji group type column {}.", column);
      return column;
    } else {
      // Map type family.
      assert(family.isMapType());
      final KijiColumnName column =
          new KijiColumnName(family.getName(), ByteUtils.toString(columnName.getQualifier()));
      LOG.debug("Translated to Kiji map type column {}.", column);
      return column;
    }
  }

  /** {@inheritDoc}. */
  @Override
  public TranslatedColumnName toTranslatedColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException {

    final String tableName = mLayout.getName();
    final String familyName = kijiColumnName.getFamily();
    final String qualifierName = kijiColumnName.getQualifier();

    final FamilyLayout family = mLayout.getFamilyMap().get(familyName);
    if (family == null) {
      throw new NoSuchColumnException(String.format(
          "No column %s in table %s.", kijiColumnName, tableName));
    }

    final String translatedFamily = family.getId().toString();
    final String translatedQualifier;

    if (family.isGroupType()) {
      // Group type family.
      if (qualifierName == null) {
        // An unqualified group type family
        translatedQualifier = null;
      } else {
        ColumnLayout qualifier = family.getColumnMap().get(qualifierName);
        if (qualifier == null) {
          throw new NoSuchColumnException(String.format(
              "No qualifier %s in family %s of table %s.", qualifierName, familyName, tableName));
        }
        translatedQualifier =
            family.getColumnMap().get(qualifierName).getId().toString();
      }
    } else {
      // Map type family.
      assert(family.isMapType());
      translatedQualifier = kijiColumnName.getQualifier();
    }

    final byte[] familyBytes = translatedFamily.getBytes(Charsets.UTF_8);
    final byte[] qualifierBytes =
        translatedQualifier == null ? null : translatedQualifier.getBytes(Charsets.UTF_8);

    return new TranslatedColumnName(familyBytes, qualifierBytes);
  }

  @Override
  public byte[] translateLocalityGroup(LocalityGroupLayout localityGroup) {
    throw new UnsupportedOperationException("Cannot translate name of Cassandra locality group.");
  }
}
