/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tool;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import picocli.CommandLine;

@CommandLine.Command(
    name = "register",
    mixinStandardHelpOptions = true,
    versionProvider = CLIVersionProvider.class,
    // As both source and target catalog has similar configurations,
    // documentation is easy to read if the target and source property is one after another instead
    // of sorted.
    sortOptions = false,
    description =
        "\nBulk register the iceberg tables from source catalog to target catalog without data copy.\n",
    subcommands = {SourceCatalog.class, TargetCatalog.class})
public class CatalogMigrationCLI implements Callable<Integer> {
  @CommandLine.Spec CommandLine.Model.CommandSpec commandSpec;

  @CommandLine.Option(
      names = {"-I", "--identifiers"},
      split = ",",
      description =
          "optional selective list of identifiers to register. If not specified, all the tables will be registered")
  List<String> identifiers = new ArrayList<>();

  @CommandLine.Option(
      names = {"-T", "--thread-pool-size"},
      defaultValue = "0",
      description =
          "optional size of the thread pool used for register tables. Tables are migrated sequentially if "
              + "not specified.")
  int maxThreadPoolSize;

  @CommandLine.Option(
      names = {"--delete-source-tables"},
      description =
          "Optional configuration to delete the tables entry from source catalog after successfully registering it "
              + "to target catalog.")
  private boolean deleteSourceCatalogTables;

  public static void main(String... args) {
    CommandLine commandLine = new CommandLine(new CatalogMigrationCLI());
    commandLine.setUsageHelpWidth(150);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    SourceCatalog from = commandSpec.subcommands().get("from").getCommand();
    if (from.catalog() == null) {
      throw new IllegalArgumentException(
          "Source catalog is not initialized. Please executes the 'from' command to build the source catalog.");
    }
    TargetCatalog to = commandSpec.subcommands().get("to").getCommand();
    if (to.catalog() == null) {
      throw new IllegalArgumentException(
          "Target catalog is not initialized. Please executes the 'to' command to build the target catalog.");
    }

    Catalog sourceCatalog = from.catalog();
    Catalog targetCatalog = to.catalog();
    List<TableIdentifier> tableIdentifiers =
        identifiers == null
            ? null
            : identifiers.stream().map(TableIdentifier::parse).collect(Collectors.toList());
    Collection<TableIdentifier> result;
    if (deleteSourceCatalogTables) {
      result =
          CatalogMigrateUtil.migrateTables(
              tableIdentifiers, sourceCatalog, targetCatalog, maxThreadPoolSize);
    } else {
      result =
          CatalogMigrateUtil.registerTables(
              tableIdentifiers, sourceCatalog, targetCatalog, maxThreadPoolSize);
    }

    PrintWriter printWriter = commandSpec.commandLine().getOut();
    printWriter.println("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ");
    printWriter.printf(
        "Successfully %s %d tables from %s catalog to %s catalog: \n",
        deleteSourceCatalogTables ? "migrated" : "registered",
        result.size(),
        sourceCatalog.name(),
        targetCatalog.name());
    result.forEach(printWriter::println);

    return 0;
  }

  public enum CatalogType {
    CUSTOM,
    DYNAMODB,
    ECS,
    GLUE,
    HADOOP,
    HIVE,
    JDBC,
    NESSIE,
    REST
  }
}
