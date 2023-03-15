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
package org.projectnessie.tools.catalog.migration.api;

import java.util.Collections;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.tools.catalog.migration.api.test.HiveMetaStoreRunner;

public class ITHadoopToHiveCatalogMigrator extends AbstractTestCatalogMigrator {

  @BeforeAll
  protected static void setup() throws Exception {
    HiveMetaStoreRunner.startMetastore();

    catalog1 = createHadoopCatalog(warehouse1.toAbsolutePath().toString(), "hadoop");
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

  @Order(8)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithNewNestedNamespace(boolean deleteSourceTables) {
    // catalog2 doesn't have a namespace "a.b.c"
    Namespace namespace = Namespace.of("a.b.c");
    String tableName = "tbl5_" + deleteSourceTables;
    TableIdentifier tableIdentifier = TableIdentifier.parse("a.b.c." + tableName);
    ((SupportsNamespaces) catalog1).createNamespace(namespace);
    catalog1.createTable(tableIdentifier, schema);

    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables)
            .registerTables(Collections.singletonList(tableIdentifier))
            .result();

    // hive catalog doesn't support multipart namespace. Hence, table should fail to register.
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers())
        .containsExactly(tableIdentifier);
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    catalog1.dropTable(tableIdentifier);
    ((SupportsNamespaces) catalog1).dropNamespace(namespace);
  }
}
