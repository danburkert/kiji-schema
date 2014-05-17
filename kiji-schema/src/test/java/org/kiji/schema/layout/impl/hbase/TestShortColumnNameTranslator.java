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

public class TestShortColumnNameTranslator {
  private KijiTableLayout mTableLayout;

  @Before
  public void readLayout() throws Exception {
    mTableLayout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED));
    System.out.println(mTableLayout);
  }

  @Test
  public void testTranslateFromKijiToHBase() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    TranslatedColumnName infoName =
        translator.toTranslatedColumnName(new KijiColumnName("info:name"));
    assertEquals("B", Bytes.toString(infoName.getFamily()));
    assertEquals("B:B", Bytes.toString(infoName.getQualifier()));

    TranslatedColumnName infoEmail =
        translator.toTranslatedColumnName(new KijiColumnName("info:email"));
    assertEquals("B", Bytes.toString(infoEmail.getFamily()));
    assertEquals("B:C", Bytes.toString(infoEmail.getQualifier()));

    TranslatedColumnName recommendationsProduct =
        translator.toTranslatedColumnName(new KijiColumnName("recommendations:product"));
    assertEquals("C", Bytes.toString(recommendationsProduct.getFamily()));
    assertEquals("B:B", Bytes.toString(recommendationsProduct.getQualifier()));

    TranslatedColumnName purchases = translator.toTranslatedColumnName(
        new KijiColumnName(
            "purchases:foo"));
    assertEquals("C", Bytes.toString(purchases.getFamily()));
    assertEquals("C:foo", Bytes.toString(purchases.getQualifier()));
  }

  @Test
  public void testTranslateFromHBaseToKiji() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    KijiColumnName infoName = translator.toKijiColumnName(getTranslatedColumnName("B", "B:B"));
    assertEquals("info:name", infoName.toString());

    KijiColumnName infoEmail = translator.toKijiColumnName(getTranslatedColumnName("B", "B:C"));
    assertEquals("info:email", infoEmail.toString());

    KijiColumnName recommendationsProduct =
        translator.toKijiColumnName(getTranslatedColumnName("C", "B:B"));
    assertEquals("recommendations:product", recommendationsProduct.toString());

    KijiColumnName purchases = translator.toKijiColumnName(getTranslatedColumnName("C", "C:foo"));
    assertEquals("purchases:foo", purchases.toString());
  }

  /**
   * Tests that an exception is thrown when the HBase family doesn't match a Kiji locality group.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchKijiLocalityGroup() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toKijiColumnName(getTranslatedColumnName("D", "E:E"));
  }

  /**
   * Tests that an exception is thrown when the first part of the HBase qualifier doesn't
   * match a Kiji family.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchKijiFamily() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toKijiColumnName(getTranslatedColumnName("C", "E:E"));
  }

  /**
   * Tests that an exception is thrown when the second part of the HBase qualifier doesn't
   * match a Kiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchKijiColumn() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toKijiColumnName(getTranslatedColumnName("C", "B:E"));
  }

  /**
   * Tests that an exception is thrown when the HBase qualifier is corrupt (no separator).
   */
  @Test(expected = NoSuchColumnException.class)
  public void testCorruptQualifier() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    translator.toKijiColumnName(getTranslatedColumnName("C", "BE"));
  }

  /**
   * Tests that an exception is thrown when trying to translate a non-existed Kiji column.
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
