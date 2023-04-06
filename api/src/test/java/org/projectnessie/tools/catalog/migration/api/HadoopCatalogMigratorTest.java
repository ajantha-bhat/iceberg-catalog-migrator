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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class HadoopCatalogMigratorTest extends AbstractTestCatalogMigrator {

  @BeforeAll
  protected static void setup() {
    initializeSourceCatalog(CatalogMigrationUtil.CatalogType.HADOOP, Collections.emptyMap());
    initializeTargetCatalog(CatalogMigrationUtil.CatalogType.HADOOP, Collections.emptyMap());

    createNamespaces();
  }

  @AfterAll
  protected static void tearDown() {
    dropNamespaces();
  }

  @Test
  public void testRegisterWithNewNestedNamespaces() {
    List<Namespace> namespaceList = Arrays.asList(NS1, NS2, NS3, NS1_NS2, NS1_NS3, NS1_NS2_NS3);

    List<TableIdentifier> identifiers =
        Arrays.asList(TBL, NS1_TBL, NS2_TBL, NS3_TBL, NS1_NS2_TBL, NS1_NS3_TBL, NS1_NS2_NS3_TBL);

    namespaceList.forEach(((SupportsNamespaces) sourceCatalog)::createNamespace);
    identifiers.forEach(identifier -> sourceCatalog.createTable(identifier, schema));

    CatalogMigrator catalogMigrator = catalogMigratorWithDefaultArgs(false);
    Set<TableIdentifier> matchingTableIdentifiers =
        catalogMigrator.getMatchingTableIdentifiers(null);
    // HadoopCatalog supports implicit namespaces.
    // Hence, No concept of default namespace too. So, cannot list the tables from default
    // namespaces.
    // Can only load tables in default namespace using identifiers.
    Assertions.assertThat(matchingTableIdentifiers)
        .containsAll(identifiers.subList(1, 7)); // without "tblz"
    Assertions.assertThat(matchingTableIdentifiers).doesNotContain(identifiers.get(0));

    CatalogMigrationResult result =
        catalogMigrator.registerTables(matchingTableIdentifiers).result();
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsAll(identifiers.subList(1, 7)); // without "tblz"
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    // manually register the table from default namespace
    catalogMigrator = catalogMigratorWithDefaultArgs(false);
    result = catalogMigrator.registerTables(Collections.singletonList(TBL)).result();
    Assertions.assertThat(result.registeredTableIdentifiers()).containsExactly(TBL);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Collections.reverse(namespaceList);
    identifiers.forEach(sourceCatalog::dropTable);
    namespaceList.forEach(((SupportsNamespaces) sourceCatalog)::dropNamespace);
    identifiers.forEach(targetCatalog::dropTable);
    namespaceList.forEach(((SupportsNamespaces) targetCatalog)::dropNamespace);
  }

  @Test
  public void testCreateAndListNamespaces() {
    ImmutableCatalogMigrator catalogMigrator =
        ImmutableCatalogMigrator.builder()
            .sourceCatalog(sourceCatalog)
            .targetCatalog(targetCatalog)
            .deleteEntriesFromSourceCatalog(false)
            .build();

    List<Namespace> namespaceList =
        Arrays.asList(NS_A, NS_A_B, NS_A_B_C, NS_A_B_C_D, NS_A_B_C_D_E, NS_A_C);
    catalogMigrator.createNamespacesIfNotExistOnTargetCatalog(
        namespaceList.get(4)); // try creating "a.b.c.d.e"
    catalogMigrator.createNamespacesIfNotExistOnTargetCatalog(
        namespaceList.get(5)); // try creating "a.c"
    // should create all the levels of missing namespaces on target catalog
    Assertions.assertThat(((SupportsNamespaces) targetCatalog).listNamespaces())
        .contains(namespaceList.get(0))
        .doesNotContainAnyElementsOf(namespaceList.subList(1, 6));
    Assertions.assertThat(((SupportsNamespaces) targetCatalog).listNamespaces(namespaceList.get(0)))
        .containsExactlyInAnyOrder(namespaceList.get(1), namespaceList.get(5));
    Assertions.assertThat(((SupportsNamespaces) targetCatalog).listNamespaces(namespaceList.get(1)))
        .containsExactly(namespaceList.get(2));
    Assertions.assertThat(((SupportsNamespaces) targetCatalog).listNamespaces(namespaceList.get(2)))
        .containsExactly(namespaceList.get(3));
    Assertions.assertThat(((SupportsNamespaces) targetCatalog).listNamespaces(namespaceList.get(3)))
        .containsExactly(namespaceList.get(4));
    Assertions.assertThat(((SupportsNamespaces) targetCatalog).listNamespaces(namespaceList.get(4)))
        .isEmpty();
    Assertions.assertThat(((SupportsNamespaces) targetCatalog).listNamespaces(namespaceList.get(5)))
        .isEmpty();

    namespaceList.forEach(
        namespace -> ((SupportsNamespaces) sourceCatalog).createNamespace(namespace));
    Set<Namespace> listedNamespaces = new HashSet<>();
    // collect all the namespaces from all levels
    catalogMigrator.getAllNamespacesFromSourceCatalog(Namespace.empty(), listedNamespaces);
    Assertions.assertThat(listedNamespaces).containsAll(namespaceList);

    Collections.reverse(namespaceList);
    namespaceList.forEach(((SupportsNamespaces) sourceCatalog)::dropNamespace);
    namespaceList.forEach(((SupportsNamespaces) targetCatalog)::dropNamespace);
  }
}
