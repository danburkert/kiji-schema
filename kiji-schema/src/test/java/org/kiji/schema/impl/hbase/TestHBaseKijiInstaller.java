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

package org.kiji.schema.impl.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiURI;

/** Tests for KijiInstaller. */
public class TestHBaseKijiInstaller {
  @Test
  public void testInstallThenUninstall() throws Exception {
    final KijiURI uri = KijiURI.newBuilder("kiji://.fake.kiji-installer/test").build();
    HBaseKijiInstaller.get().install(uri, ImmutableMap.<String, String>of());
    HBaseKijiInstaller.get().uninstall(uri);
  }

  @Test
  public void testInstallNullInstance() throws Exception {
    final KijiURI uri = KijiURI.newBuilder("kiji://.fake.kiji-installer/").build();
    try {
      HBaseKijiInstaller.get().install(uri, ImmutableMap.<String, String>of());
      fail("An exception should have been thrown.");
    } catch (KijiInvalidNameException kine) {
      assertEquals(
          "Kiji URI 'kiji://.fake.kiji-installer:2181/' does not specify a Kiji instance name",
          kine.getMessage());
    }
  }

  @Test
  public void testUninstallNullInstance() throws Exception {
    final KijiURI uri = KijiURI.newBuilder("kiji://.fake.kiji-installer/").build();
    try {
      HBaseKijiInstaller.get().uninstall(uri);
      fail("An exception should have been thrown.");
    } catch (KijiInvalidNameException kine) {
      assertEquals(
          "Kiji URI 'kiji://.fake.kiji-installer:2181/' does not specify a Kiji instance name",
          kine.getMessage());
    }
  }

  @Test
  public void testUninstallMissingInstance() throws Exception {
    final KijiURI uri =
        KijiURI.newBuilder("kiji://.fake.kiji-installer/anInstanceThatNeverExisted").build();
    try {
      HBaseKijiInstaller.get().uninstall(uri);
      fail("An exception should have been thrown.");
    } catch (KijiNotInstalledException knie) {
      assertTrue(Pattern.matches(
          "Kiji instance kiji://.*/anInstanceThatNeverExisted/ is not installed\\.",
          knie.getMessage()));
    }
  }
}
