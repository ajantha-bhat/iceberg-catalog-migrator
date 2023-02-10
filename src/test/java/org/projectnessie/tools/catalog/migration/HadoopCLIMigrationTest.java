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
package org.projectnessie.tools.catalog.migration;

import static org.projectnessie.tools.catalog.migration.CatalogMigrator.DRY_RUN_FILE;
import static org.projectnessie.tools.catalog.migration.CatalogMigrator.FAILED_IDENTIFIERS_FILE;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class HadoopCLIMigrationTest extends AbstractCLIMigrationTest {

  @BeforeAll
  protected static void setup() {
    dryRunFile = outputDir.getAbsolutePath() + "/" + DRY_RUN_FILE;
    failedIdentifiersFile = outputDir.getAbsolutePath() + "/" + FAILED_IDENTIFIERS_FILE;
    String warehousePath1 = String.format("file://%s", warehouse1.getAbsolutePath());
    String warehousePath2 = String.format("file://%s", warehouse2.getAbsolutePath());
    sourceCatalogProperties = "warehouse=" + warehousePath1 + ",type=hadoop";
    targetCatalogProperties = "warehouse=" + warehousePath2 + ",type=hadoop";

    catalog1 = createHadoopCatalog(warehousePath1, "catalog1");
    catalog2 = createHadoopCatalog(warehousePath2, "catalog2");

    sourceCatalogType = catalogType(catalog1);
    targetCatalogType = catalogType(catalog2);

    createNamespaces();
  }

  @AfterAll
  protected static void tearDown() {
    dropNamespaces();
  }
}
