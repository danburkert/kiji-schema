/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.delegation.Priority;
import org.kiji.schema.CassandraKijiURI;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiFactory;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraFactory;

/** Factory for constructing instances of CassandraKiji. */
@ApiAudience.Private
public final class CassandraKijiFactory implements KijiFactory {

  /** Singleton C* Kiji factory. */
  private static CassandraKijiFactory singleton = null;

  /**
   * Getting for singleton instance.
   * @return The singleton CassandraKijiFactory.
   */
  public static CassandraKijiFactory get() {
    if (null == singleton) {
      singleton = new CassandraKijiFactory();
    }
    return singleton;
  }

  /** {@inheritDoc} */
  @Override
  public Kiji open(KijiURI uri) throws IOException {
    CassandraFactory cassandraFactory = CassandraFactory.Provider.get();
    CassandraAdminFactory adminFactory = cassandraFactory.getCassandraAdminFactory(uri);
    CassandraAdmin admin = adminFactory.create(uri);
    return new CassandraKiji(uri, admin, cassandraFactory.getLockFactory(uri));
  }

  /** {@inheritDoc} */
  @Override
  public Kiji open(KijiURI uri, Configuration conf) throws IOException {
    return open(uri);
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    Preconditions.checkArgument(runtimeHints.containsKey(Kiji.KIJI_TYPE_KEY));
    if (runtimeHints.get(Kiji.KIJI_TYPE_KEY).equals(CassandraKijiURI.TYPE_CASSANDRA)) {
      // Default priority; should be used unless overridden by tests.
      return Priority.NORMAL;
    } else {
      return Priority.DISABLED;
    }
  }
}
