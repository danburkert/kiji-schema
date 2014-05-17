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

public class TestNativeColumnNameTranslator {
  private KijiTableLayout mTableLayout;

  @Before
  public void readLayout() throws Exception {
    mTableLayout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(
                KijiTableLayouts.FULL_FEATURED_NATIVE));
  }

  @Test
  public void testTranslateFromKijiToHBase() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    TranslatedColumnName infoName =
        translator.toTranslatedColumnName(new KijiColumnName("info:name"));
    assertEquals("info", Bytes.toString(infoName.getFamily()));
    assertEquals("name", Bytes.toString(infoName.getQualifier()));

    TranslatedColumnName infoEmail = translator.toTranslatedColumnName(
        new KijiColumnName(
            "info:email"));
    assertEquals("info", Bytes.toString(infoEmail.getFamily()));
    assertEquals("email", Bytes.toString(infoEmail.getQualifier()));

    TranslatedColumnName recommendationsProduct = translator.toTranslatedColumnName(
        new KijiColumnName("recommendations:product"));
    assertEquals("recommendations", Bytes.toString(recommendationsProduct.getFamily()));
    assertEquals("product", Bytes.toString(recommendationsProduct.getQualifier()));

    TranslatedColumnName purchases = translator.toTranslatedColumnName(
        new KijiColumnName("recommendations:product"));
    assertEquals("recommendations", Bytes.toString(purchases.getFamily()));
    assertEquals("product", Bytes.toString(purchases.getQualifier()));
  }

  @Test
  public void testTranslateFromHBaseToKiji() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    KijiColumnName infoName =
        translator.toKijiColumnName(getTranslatedColumnName("info", "name"));
    assertEquals("info:name", infoName.toString());

    KijiColumnName infoEmail =
        translator.toKijiColumnName(getTranslatedColumnName("info", "email"));
    assertEquals("info:email", infoEmail.toString());

    KijiColumnName recommendationsProduct = translator.toKijiColumnName(
        getTranslatedColumnName("recommendations", "product"));
    assertEquals("recommendations:product", recommendationsProduct.toString());

    KijiColumnName purchases =
        translator.toKijiColumnName(getTranslatedColumnName("recommendations", "product"));
    assertEquals("recommendations:product", purchases.toString());
  }

  /**
   * Tests that an exception is thrown when the HBase family doesn't match a Kiji locality group.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchKijiLocalityGroup() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toKijiColumnName(getTranslatedColumnName("fakeFamily", "fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when the second part of the HBase qualifier doesn't
   * match a Kiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchKijiColumn() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toKijiColumnName(getTranslatedColumnName("recommendations", "fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when trying to translate a non-existent Kiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchHBaseColumn() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toTranslatedColumnName(new KijiColumnName("doesnot:exist"));
  }

  /**
   * Tests that an exception is thrown when trying to instantiate a NativeKijiColumnTranslator
   * when the layout contains column families that don't match the locality group.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNativeLayout() throws Exception {
    KijiTableLayout invalidLayout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.INVALID_NATIVE));
    KijiColumnNameTranslator.from(invalidLayout);
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
