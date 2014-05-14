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

package org.kiji.schema.impl;

import java.io.IOException;
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.delegation.PriorityProvider;
import org.kiji.schema.KijiURI;

/**
 * Interface implemented by providers of concrete Kiji installer instances. This interface is used
 * by the {@link org.kiji.schema.KijiInstaller} to find a concrete instances at runtime which
 * can install and uninstall Kiji instance of the appropriate type  (HBase or Cassandra).
 */
@ApiAudience.Private
@ApiStability.Evolving
public interface KijiInstallerProvider extends PriorityProvider {
  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param properties Map of the initial system properties for installation, to be used in addition
   *     to the defaults.
   * @throws IOException on I/O error.
   * @throws org.kiji.schema.KijiInvalidNameException if the Kiji instance name is invalid or
   *     already exists.
   */
  void install(KijiURI uri, Map<String, String> properties) throws IOException;

  /**
   * Uninstalls the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to uninstall.
   * @throws IOException on I/O error.
   * @throws org.kiji.schema.KijiInvalidNameException if the instance name is invalid or already
   *     exists.
   */
  void uninstall(KijiURI uri) throws IOException;
}
