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

package org.kiji.schema;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.delegation.Lookups;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.impl.KijiInstallerProvider;

/** Installs or uninstalls Kiji instances from an HBase cluster. */
@ApiAudience.Public
@ApiStability.Evolving
public final class KijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(KijiInstaller.class);

  /** Singleton KijiInstaller. **/
  private static final KijiInstaller SINGLETON = new KijiInstaller();

  /** Constructs a KijiInstaller. */
  private KijiInstaller() {
  }

  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the Kiji instance name is invalid or already exists.
   * @deprecated use {@link #install(KijiURI, java.util.Map)} instead.
   */
  @Deprecated
  public void install(KijiURI uri, Configuration conf) throws IOException {
    install(uri, null, ImmutableMap.<String, String>of(), conf);
  }

  /**
   * Uninstalls the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to uninstall.
   * @param conf unused.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid or already exists.
   * @deprecated use {@link #uninstall(KijiURI)} instead.
   */
  @Deprecated
  public void uninstall(KijiURI uri, Configuration conf) throws IOException {
    uninstall(uri);
  }

  /**
   * Installs a Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param hbaseFactory unused.
   * @param properties Map of the initial system properties for installation, to be used in addition
   *     to the defaults.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid or already exists.
   * @deprecated use {@link #install(KijiURI, java.util.Map)} instead.
   */
  @Deprecated
  public void install(
      KijiURI uri,
      HBaseFactory hbaseFactory,
      Map<String, String> properties,
      Configuration conf)
      throws IOException {
    Map<String, String> propertiesCopy = Maps.newHashMap(properties);
    for (Map.Entry<String, String> entry : conf) {
      propertiesCopy.put(entry.getKey(), entry.getValue());
    }
    install(uri, propertiesCopy);
  }

  /**
   * Removes a kiji instance from the HBase cluster including any user tables.
   *
   * @param uri URI of the Kiji instance to install.
   * @param hbaseFactory unused.
   * @param conf unused.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid.
   * @throws KijiNotInstalledException if the specified instance does not exist.
   * @deprecated use {@link #uninstall(KijiURI)} instead.
   */
  @Deprecated
  public void uninstall(
      KijiURI uri,
      HBaseFactory hbaseFactory,
      Configuration conf
  ) throws IOException {
    uninstall(uri);
  }

  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the Kiji instance name is invalid or already exists.
   */
  public void install(KijiURI uri) throws IOException {
    install(uri, ImmutableMap.<String, String>of());
  }

  /**
   * Installs the specified Kiji instance with the specified properties.
   *
   * @param uri URI of the Kiji instance to install.
   * @param properties Map of the initial system properties for installation, to be used in addition
   *     to the defaults.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the Kiji instance name is invalid or already exists.
   */
  public void install(KijiURI uri, Map<String, String> properties) throws IOException {
    Lookups
        .getPriority(KijiInstallerProvider.class)
        .lookup(ImmutableMap.of(Kiji.KIJI_TYPE_KEY, uri.getKijiType().toString()))
        .install(uri, properties);
  }


  /**
   * Uninstalls the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to uninstall.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid.
   */
  public void uninstall(KijiURI uri) throws IOException {
    Lookups
        .getPriority(KijiInstallerProvider.class)
        .lookup(ImmutableMap.of(Kiji.KIJI_TYPE_KEY, uri.getKijiType().toString()))
        .uninstall(uri);
  }

  /**
   * Gets an instance of a KijiInstaller.
   *
   * @return An instance of a KijiInstaller.
   */
  public static KijiInstaller get() {
    return SINGLETON;
  }
}
