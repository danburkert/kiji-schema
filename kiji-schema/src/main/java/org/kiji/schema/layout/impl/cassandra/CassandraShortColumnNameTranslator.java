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

package org.kiji.schema.layout.impl.cassandra;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * Translates Kiji column names into shorter families and qualifiers.
 *
 * TODO: Update Cassandra column name translators with short, identity, native.
 *
 */
@ApiAudience.Private
public final class CassandraShortColumnNameTranslator extends CassandraColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraShortColumnNameTranslator.class);

  private final KijiURI mTableURI;

  private final KijiTableLayout mLayout;

  private final Map<ColumnId, LocalityGroupLayout> mLocalityGroups;

  /**
   * Creates a new <code>CassandraColumnNameTranslator</code> instance.
   *
   * @param layout The layout of the table to translate column names for.
   */
  public CassandraShortColumnNameTranslator(KijiURI tableURI, KijiTableLayout layout) {
    mTableURI = tableURI;
    mLayout = layout;

    // Index the locality groups by their ColumnId
    ImmutableMap.Builder<ColumnId, LocalityGroupLayout> localityGroups = ImmutableMap.builder();
    for (Map.Entry<ColumnId, String> entry : mLayout.getLocalityGroupIdNameMap().entrySet()) {
      localityGroups.put(entry.getKey(), mLayout.getLocalityGroupMap().get(entry.getValue()));
    }
    mLocalityGroups = localityGroups.build();
  }

  /** {@inheritDoc}. */
  @Override
  public KijiColumnName toKijiColumnName(CassandraColumnName columnName)
      throws NoSuchColumnException {
    LOG.debug("Translating Cassandra column {} to Kiji column name.", columnName);
    final ColumnId lgId =
        ColumnId.fromString(columnName.getTable().getKijiLocalityGroup());
    final LocalityGroupLayout localityGroup = mLocalityGroups.get(lgId);
    if (localityGroup == null) {
      throw new NoSuchColumnException(String.format(
          "No locality group with ID %s in table %s.",
          columnName.getFamily(), mLayout.getName()));
    }

    final ColumnId familyID = ColumnId.fromString(columnName.getFamily());
    final FamilyLayout family = getKijiFamilyById(localityGroup, familyID);
    if (family == null) {
      throw new NoSuchColumnException(String.format(
          "No column family with ID %s in locality group %s of table %s.",
          familyID, localityGroup.getName(), mLayout.getName()));
    }

    if (family.isGroupType()) {
      // Group type family.
      final ColumnId qualifierID = ColumnId.fromString(columnName.getQualifier());
      final ColumnLayout qualifier = getKijiColumnById(family, qualifierID);
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
      final KijiColumnName column = new KijiColumnName(family.getName(), columnName.getQualifier());
      LOG.debug("Translated to Kiji map type column {}.", column);
      return column;
    }
  }

  /** {@inheritDoc}. */
  @Override
  public CassandraColumnName toCassandraColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException {
    final String familyName = kijiColumnName.getFamily();
    final String qualifierName = kijiColumnName.getQualifier();
    final String tableName = mLayout.getName();
    final FamilyLayout family = mLayout.getFamilyMap().get(kijiColumnName.getFamily());
    if (family == null) {
      throw new NoSuchColumnException(String.format(
          "No column %s in table %s.", kijiColumnName, tableName));
    }
    final CassandraTableName cassandraTableName;
    if (mLayout.getCellSchema(kijiColumnName).getType() == SchemaType.COUNTER) {
      // Counter column
      cassandraTableName = CassandraTableName.getKijiCounterTableName(mTableURI);
    } else {
      // Non-counter column
      cassandraTableName =
          CassandraTableName.getKijiLocalityGroupTableName(
              mTableURI,
              family.getLocalityGroup().getId().toString()); // Translated locality group
    }

    final String translatedFamily = family.getId().toString();
    if (family.isGroupType()) {
      // Group type family.
      final String translatedQualifier;
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
      return new CassandraColumnName(cassandraTableName, translatedFamily, translatedQualifier);
    } else {
      // Map type family.
      assert(family.isMapType());
      return new CassandraColumnName(cassandraTableName, translatedFamily, kijiColumnName.getQualifier());
    }
  }

  /**
   * Gets a Kiji column family from within a locality group by ID.
   *
   * @param localityGroup The locality group to look in.
   * @param familyId The ColumnId of the family to look for.
   * @return The family, or null if no family with the ID can be found within the locality group.
   */
  protected FamilyLayout getKijiFamilyById(LocalityGroupLayout localityGroup, ColumnId familyId) {
    final String familyName = localityGroup.getFamilyIdNameMap().get(familyId);
    return localityGroup.getFamilyMap().get(familyName);
  }

  /**
   * Gets a Kiji column from within a group-type family by ID.
   *
   * @param family The group-type family to look in.
   * @param columnId The ColumnId of the column to look for.
   * @return The column, or null if no column with the ID can be found within the family.
   */
  protected ColumnLayout getKijiColumnById(FamilyLayout family, ColumnId columnId) {
    final String columnName = family.getColumnIdNameMap().get(columnId);
    return family.getColumnMap().get(columnName);
  }
}
