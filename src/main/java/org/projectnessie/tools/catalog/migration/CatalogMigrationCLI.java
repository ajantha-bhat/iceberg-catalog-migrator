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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.dell.ecs.EcsCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import picocli.CommandLine;

@CommandLine.Command(
    name = "migrate",
    mixinStandardHelpOptions = true,
    versionProvider = CLIVersionProvider.class,
    // As both source and target catalog has similar configurations,
    // documentation is easy to read if the target and source property is one after another instead
    // of sorted.
    sortOptions = false,
    description =
        "\nBulk migrate the iceberg tables from source catalog to target catalog without data copy.\n")
public class CatalogMigrationCLI implements Callable<Integer> {
  @CommandLine.Spec CommandLine.Model.CommandSpec commandSpec;

  @CommandLine.Option(
      names = "--source-catalog-type",
      required = true,
      description =
          "source catalog type. "
              + "Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]")
  private CatalogType sourceCatalogType;

  @CommandLine.Option(
      names = "--source-catalog-properties",
      required = true,
      split = ",",
      description = "source catalog properties")
  private Map<String, String> sourceCatalogProperties;

  @CommandLine.Option(
      names = "--source-catalog-hadoop-conf",
      split = ",",
      description =
          "optional source catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when "
              + "using an Iceberg FileIO.")
  Map<String, String> sourceHadoopConf = new HashMap<>();

  @CommandLine.Option(
      names = {"--source-custom-catalog-impl"},
      description =
          "optional fully qualified class name of the custom catalog implementation of the source catalog. Required "
              + "when the catalog type is CUSTOM.")
  String sourceCustomCatalogImpl;

  @CommandLine.Option(
      names = "--target-catalog-type",
      required = true,
      description =
          "target catalog type. "
              + "Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]")
  private CatalogType targetCatalogType;

  @CommandLine.Option(
      names = "--target-catalog-properties",
      required = true,
      split = ",",
      description = "target catalog properties")
  private Map<String, String> targetCatalogProperties;

  @CommandLine.Option(
      names = "--target-catalog-hadoop-conf",
      split = ",",
      description =
          "optional target catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when "
              + "using an Iceberg FileIO.")
  Map<String, String> targetHadoopConf = new HashMap<>();

  @CommandLine.Option(
      names = {"--target-custom-catalog-impl"},
      description =
          "optional fully qualified class name of the custom catalog implementation of the target catalog. Required "
              + "when the catalog type is CUSTOM.")
  String targetCustomCatalogImpl;

  @CommandLine.Option(
      names = {"-I", "--identifiers"},
      split = ",",
      description =
          "optional selective list of identifiers to migrate. If not specified, all the tables will be migrated. "
              + "Use this when there are few identifiers that need to be migrated. For a large number of identifiers, "
              + "use the `--identifiers-from-file` or `--identifiers-regex` option.")
  List<String> identifiers = new ArrayList<>();

  @CommandLine.Option(
      names = {"--identifiers-from-file"},
      description =
          "optional text file path that contains a list of table identifiers (one per line) to migrate. Should not be "
              + "used with `--identifiers` or `--identifiers-regex` option.")
  String identifiersFromFile;

  @CommandLine.Option(
      names = {"--identifiers-regex"},
      description =
          "optional regular expression pattern used to migrate only the tables whose identifiers match this pattern. "
              + "Should not be used with `--identifiers` or '--identifiers-from-file' option.")
  String identifiersRegEx;

  @CommandLine.Option(
      names = {"-T", "--thread-pool-size"},
      defaultValue = "0",
      description =
          "optional size of the thread pool used for migrating tables. Tables are migrated sequentially if "
              + "not specified.")
  int maxThreadPoolSize;

  @CommandLine.Option(
      names = {"--dry-run"},
      description =
          "Optional configuration to simulate the migration without actually migrating. Can know a list of the tables "
              + "that will be migrated by running this.")
  private boolean isDryRun;

  public static void main(String... args) {
    CommandLine commandLine = new CommandLine(new CatalogMigrationCLI());
    commandLine.setUsageHelpWidth(150);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    if (identifiersFromFile != null && !identifiers.isEmpty()) {
      throw new IllegalArgumentException(
          "Both `--identifiers` and `--identifiers-from-file` options are configured. Please use only one of them.");
    } else if (identifiersFromFile != null) {
      if (!Files.exists(Paths.get(identifiersFromFile))) {
        throw new IllegalArgumentException(
            "File specified in `--identifiers-from-file` option does not exist.");
      }
    }

    PrintWriter printWriter = commandSpec.commandLine().getOut();
    Configuration sourceCatalogConf = new Configuration();
    if (sourceHadoopConf != null && !sourceHadoopConf.isEmpty()) {
      sourceHadoopConf.forEach(sourceCatalogConf::set);
    }
    Catalog sourceCatalog =
        CatalogUtil.loadCatalog(
            Objects.requireNonNull(catalogImpl(sourceCatalogType, sourceCustomCatalogImpl)),
            "sourceCatalog",
            sourceCatalogProperties,
            sourceCatalogConf);
    printWriter.printf("\nConfigured source catalog: %s\n", sourceCatalogType.name());

    Configuration targetCatalogConf = new Configuration();
    if (targetHadoopConf != null && !targetHadoopConf.isEmpty()) {
      targetHadoopConf.forEach(targetCatalogConf::set);
    }
    Catalog targetCatalog =
        CatalogUtil.loadCatalog(
            Objects.requireNonNull(catalogImpl(targetCatalogType, targetCustomCatalogImpl)),
            "targetCatalog",
            targetCatalogProperties,
            targetCatalogConf);
    printWriter.printf("\nConfigured target catalog: %s\n", targetCatalogType.name());

    List<TableIdentifier> tableIdentifiers = null;
    if (identifiersFromFile != null) {
      try {
        printWriter.printf("\nCollecting identifiers from the file %s...\n", identifiersFromFile);
        tableIdentifiers =
            Files.readAllLines(Paths.get(identifiersFromFile)).stream()
                .map(TableIdentifier::parse)
                .collect(Collectors.toList());
      } catch (IOException e) {
        throw new RuntimeException("Failed to read the file:", e);
      }
    } else if (!identifiers.isEmpty()) {
      tableIdentifiers =
          identifiers.stream().map(TableIdentifier::parse).collect(Collectors.toList());
    }
    // TODO: identifier regex validation and passing

    // TODO: pass and handle dry-run

    ImmutablePair<Collection<TableIdentifier>, Collection<TableIdentifier>> result;
    result =
        CatalogMigrateUtil.migrateTables(
            tableIdentifiers, sourceCatalog, targetCatalog, maxThreadPoolSize, printWriter);
    if (sourceCatalogType == CatalogType.HADOOP) {
      printWriter.println(
          "\n[WARNING]: Source catalog type is HADOOP and it doesn't support dropping tables just from "
              + "catalog. \nAvoid operating the migrated tables from the source catalog after migration. "
              + "Use the tables from target catalog.");
    }

    printWriter.println("\nSummary: ");
    if (!result.left.isEmpty()) {
      printWriter.printf(
          "- Successfully migrated %d tables from %s catalog to %s catalog. \n",
          result.left.size(), sourceCatalogType.name(), targetCatalogType.name());
    }
    if (!result.right.isEmpty()) {
      List<String> failedIdentifiers =
          result.right.stream().map(TableIdentifier::toString).collect(Collectors.toList());
      try {
        Files.write(Paths.get("failed_identifiers.txt"), failedIdentifiers);
      } catch (IOException e) {
        throw new RuntimeException("Failed to write the file:", e);
      }
      printWriter.printf(
          "- Failed to migrate %d tables from %s catalog to %s catalog. "
              + "Please check the `catalog_migration.log` file for the failure reason. "
              + "\n Failed Identifiers are written to `failed_identifiers.txt`. "
              + "Retry with that file using `--identifiers-from-file` option "
              + "if the failure is because of network/connection timeouts.\n",
          result.right.size(), sourceCatalogType.name(), targetCatalogType.name());
    }
    printWriter.println("\nDetails: ");
    if (!result.left.isEmpty()) {
      printWriter.println("- Successfully migrated these tables:\n");
      printWriter.println(result.left);
    }
    if (!result.right.isEmpty()) {
      printWriter.printf("- Failed to migrate these tables: \n");
      printWriter.println(result.right);
    }

    return 0;
  }

  private String catalogImpl(CatalogType type, String customCatalogImpl) {
    switch (type) {
      case CUSTOM:
        if (customCatalogImpl == null || customCatalogImpl.isEmpty()) {
          throw new IllegalArgumentException(
              "Need to specify the fully qualified class name of the custom catalog " + "impl");
        }
        return customCatalogImpl;
      case DYNAMODB:
        return DynamoDbCatalog.class.getName();
      case ECS:
        return EcsCatalog.class.getName();
      case GLUE:
        return GlueCatalog.class.getName();
      case HADOOP:
        return HadoopCatalog.class.getName();
      case HIVE:
        return HiveCatalog.class.getName();
      case JDBC:
        return JdbcCatalog.class.getName();
      case NESSIE:
        return NessieCatalog.class.getName();
      case REST:
        return RESTCatalog.class.getName();
      default:
        throw new IllegalArgumentException("Unsupported type: " + type.name());
    }
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
