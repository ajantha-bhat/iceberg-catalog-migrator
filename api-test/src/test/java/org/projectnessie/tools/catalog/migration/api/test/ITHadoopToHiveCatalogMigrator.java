/*
 * Copyright (C) 2023 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.tools.catalog.migration.api.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

public class ITHadoopToHiveCatalogMigrator extends AbstractTestCatalogMigrator {

  @BeforeAll
  protected static void setup() throws Exception {
    HiveMetaStoreRunner.startMetastore();

    catalog1 = createHadoopCatalog(warehouse2.toAbsolutePath().toString(), "hadoop");
    catalog2 = HiveMetaStoreRunner.hiveCatalog();

    createNamespaces();
  }

  @AfterAll
  protected static void tearDown() throws Exception {
    dropNamespaces();
    HiveMetaStoreRunner.stopMetastore();
  }

  // disable large table test for IT to save CI time. It will be executed only for UT.
  @Override
  @Disabled
  public void testRegisterLargeNumberOfTables(boolean deleteSourceTables) throws Exception {
    super.testRegisterLargeNumberOfTables(deleteSourceTables);
  }
}
