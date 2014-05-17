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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.TranslatedColumnName;

public class TestIdentityColumnNameTranslator {
  private KijiTableLayout mTableLayout;

  @Before
  public void readLayout() throws Exception {
    mTableLayout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(
                KijiTableLayouts.FULL_FEATURED_IDENTITY));
  }

  @Test
  public void testTranslateFromKijiToHBase() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    TranslatedColumnName
        infoName = translator.toTranslatedColumnName(new KijiColumnName("info:name"));
    assertEquals("default", Bytes.toString(infoName.getFamily()));
    assertEquals("info:name", Bytes.toString(infoName.getQualifier()));

    TranslatedColumnName infoEmail = translator.toTranslatedColumnName(
        new KijiColumnName(
            "info:email"));
    assertEquals("default", Bytes.toString(infoEmail.getFamily()));
    assertEquals("info:email", Bytes.toString(infoEmail.getQualifier()));

    TranslatedColumnName recommendationsProduct = translator.toTranslatedColumnName(
        new KijiColumnName("recommendations:product"));
    assertEquals("inMemory", Bytes.toString(recommendationsProduct.getFamily()));
    assertEquals("recommendations:product", Bytes.toString(recommendationsProduct.getQualifier()));

    TranslatedColumnName purchases = translator.toTranslatedColumnName(
        new KijiColumnName(
            "purchases:foo"));
    assertEquals("inMemory", Bytes.toString(purchases.getFamily()));
    assertEquals("purchases:foo", Bytes.toString(purchases.getQualifier()));
  }

  @Test
  public void testTranslateFromHBaseToKiji() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    KijiColumnName infoName =
        translator.toKijiColumnName(getTranslatedColumnName("default", "info:name"));
    assertEquals("info:name", infoName.toString());

    KijiColumnName infoEmail =
        translator.toKijiColumnName(getTranslatedColumnName("default", "info:email"));
    assertEquals("info:email", infoEmail.toString());

    KijiColumnName recommendationsProduct = translator.toKijiColumnName(
        getTranslatedColumnName("inMemory", "recommendations:product"));
    assertEquals("recommendations:product", recommendationsProduct.toString());

    KijiColumnName purchases =
        translator.toKijiColumnName(getTranslatedColumnName("inMemory", "purchases:foo"));
    assertEquals("purchases:foo", purchases.toString());
  }

  /**
   * Tests that an exception is thrown when the HBase family doesn't match a Kiji locality group.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchKijiLocalityGroup() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toKijiColumnName(
        getTranslatedColumnName("fakeLocGroup", "fakeFamily:fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when the first part of the HBase qualifier doesn't
   * match a Kiji family.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchKijiFamily() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toKijiColumnName(getTranslatedColumnName("inMemory", "fakeFamily:fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when the second part of the HBase qualifier doesn't
   * match a Kiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchKijiColumn() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toKijiColumnName(
        getTranslatedColumnName("inMemory", "recommendations:fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when the HBase qualifier is corrupt (no separator).
   */
  @Test(expected = NoSuchColumnException.class)
  public void testCorruptQualifier() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toKijiColumnName(getTranslatedColumnName("inMemory", "fakeFamilyfakeQualifier"));
  }

  /**
   * Tests translation of HBase qualifiers have multiple separators (the Kiji qualifier contains
   * the separator).
   */
  @Test
  public void testMultipleSeparators() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    KijiColumnName kijiColumnName =
        translator.toKijiColumnName(getTranslatedColumnName("inMemory", "purchases:left:right"));
    assertEquals("purchases", kijiColumnName.getFamily());
    assertEquals("left:right", kijiColumnName.getQualifier());
  }

  /**
   * Tests that an exception is thrown when trying to translate a non-existent Kiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchHBaseColumn() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toTranslatedColumnName(new KijiColumnName("doesnt:exist"));
  }

  /**
   * Turns a family:qualifier string into an TranslatedColumnName.
   *
   * @param family The HBase family.
   * @param qualifier the HBase qualifier.
   * @return An TranslatedColumnName instance.
   */
  private static TranslatedColumnName getTranslatedColumnName(String family, String qualifier) {
    return new TranslatedColumnName(Bytes.toBytes(family), Bytes.toBytes(qualifier));
  }
}
