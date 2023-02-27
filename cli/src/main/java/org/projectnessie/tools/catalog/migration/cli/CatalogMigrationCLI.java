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
package org.projectnessie.tools.catalog.migration.cli;

import static org.projectnessie.tools.catalog.migration.api.CatalogMigrator.DRY_RUN_FILE;
import static org.projectnessie.tools.catalog.migration.api.CatalogMigrator.FAILED_IDENTIFIERS_FILE;
import static org.projectnessie.tools.catalog.migration.api.CatalogMigrator.FAILED_TO_DELETE_AT_SOURCE_FILE;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
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
import org.projectnessie.tools.catalog.migration.api.CatalogMigrationResult;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrator;
import org.projectnessie.tools.catalog.migration.api.ImmutableCatalogMigrationResult;
import org.projectnessie.tools.catalog.migration.api.ImmutableCatalogMigratorParams;
import picocli.CommandLine;

@CommandLine.Command(
    name = "register",
    mixinStandardHelpOptions = true,
    versionProvider = CLIVersionProvider.class,
    // As both source and target catalog has similar configurations,
    // documentation is easy to read if the target and source property is one after another instead
    // of sorted order.
    sortOptions = false,
    description =
        "Bulk register the iceberg tables from source catalog to target catalog without data copy.")
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
      description = "source catalog properties (like uri, warehouse, etc)")
  private Map<String, String> sourceCatalogProperties;

  @CommandLine.Option(
      names = "--source-catalog-hadoop-conf",
      split = ",",
      description =
          "optional source catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when "
              + "using an Iceberg FileIO.")
  private Map<String, String> sourceHadoopConf = new HashMap<>();

  @CommandLine.Option(
      names = {"--source-custom-catalog-impl"},
      description =
          "optional fully qualified class name of the custom catalog implementation of the source catalog. Required "
              + "when the catalog type is CUSTOM.")
  private String sourceCustomCatalogImpl;

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
      description = "target catalog properties (like uri, warehouse, etc)")
  private Map<String, String> targetCatalogProperties;

  @CommandLine.Option(
      names = "--target-catalog-hadoop-conf",
      split = ",",
      description =
          "optional target catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when "
              + "using an Iceberg FileIO.")
  private Map<String, String> targetHadoopConf = new HashMap<>();

  @CommandLine.Option(
      names = {"--target-custom-catalog-impl"},
      description =
          "optional fully qualified class name of the custom catalog implementation of the target catalog. Required "
              + "when the catalog type is CUSTOM.")
  private String targetCustomCatalogImpl;

  @CommandLine.Option(
      names = {"--identifiers"},
      split = ",",
      description =
          "optional selective list of identifiers to register. If not specified, all the tables will be registered. "
              + "Use this when there are few identifiers that need to be registered. For a large number of identifiers, "
              + "use the `--identifiers-from-file` or `--identifiers-regex` option.")
  private List<String> identifiers = new ArrayList<>();

  @CommandLine.Option(
      names = {"--identifiers-from-file"},
      description =
          "optional text file path that contains a list of table identifiers (one per line) to register. Should not be "
              + "used with `--identifiers` or `--identifiers-regex` option.")
  private String identifiersFromFile;

  @CommandLine.Option(
      names = {"--identifiers-regex"},
      description =
          "optional regular expression pattern used to register only the tables whose identifiers match this pattern. "
              + "Should not be used with `--identifiers` or '--identifiers-from-file' option.")
  private String identifiersRegEx;

  @CommandLine.Option(
      names = {"--dry-run"},
      description =
          "optional configuration to simulate the registration without actually registering. Can learn about a list "
              + "of the tables that will be registered by running this.")
  private boolean isDryRun;

  @CommandLine.Option(
      names = {"--delete-source-tables"},
      description =
          "optional configuration to delete the table entry from source catalog after successfully registering it "
              + "to target catalog.")
  private boolean deleteSourceCatalogTables;

  @CommandLine.Option(
      names = {"--output-dir"},
      description =
          "optional local output directory path to write CLI output files like `failed_identifiers.txt`, "
              + "`failed_to_delete_at_source.txt`, `dry_run_identifiers.txt`. "
              + "Uses the present working directory if not specified.")
  private Path outputDirPath;

  private boolean disablePrompts;

  private static final int BATCH_SIZE = 100;

  public CatalogMigrationCLI() {}

  public static void main(String... args) {
    CommandLine commandLine = new CommandLine(new CatalogMigrationCLI());
    commandLine.setUsageHelpWidth(150);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  public void disablePrompts() {
    this.disablePrompts = true;
  }

  @Override
  public Integer call() {
    validateIdentifierOptions();

    PrintWriter printWriter = commandSpec.commandLine().getOut();
    Configuration sourceCatalogConf = new Configuration();
    if (sourceHadoopConf != null && !sourceHadoopConf.isEmpty()) {
      sourceHadoopConf.forEach(sourceCatalogConf::set);
    }
    Catalog sourceCatalog =
        CatalogUtil.loadCatalog(
            Objects.requireNonNull(catalogImpl(sourceCatalogType, sourceCustomCatalogImpl)),
            sourceCatalogType.name(),
            sourceCatalogProperties,
            sourceCatalogConf);
    printWriter.println(String.format("%nConfigured source catalog: %s", sourceCatalogType.name()));

    Configuration targetCatalogConf = new Configuration();
    if (targetHadoopConf != null && !targetHadoopConf.isEmpty()) {
      targetHadoopConf.forEach(targetCatalogConf::set);
    }
    Catalog targetCatalog =
        CatalogUtil.loadCatalog(
            Objects.requireNonNull(catalogImpl(targetCatalogType, targetCustomCatalogImpl)),
            targetCatalogType.name(),
            targetCatalogProperties,
            targetCatalogConf);
    printWriter.println(String.format("%nConfigured target catalog: %s", targetCatalogType.name()));

    List<TableIdentifier> tableIdentifiers = processIdentifiersInput(printWriter);

    if (!canProceed(printWriter, sourceCatalog)) {
      return 0;
    }

    ImmutableCatalogMigratorParams params =
        ImmutableCatalogMigratorParams.builder()
            .sourceCatalog(sourceCatalog)
            .targetCatalog(targetCatalog)
            .deleteEntriesFromSourceCatalog(deleteSourceCatalogTables)
            .build();
    CatalogMigrator catalogMigrator = new CatalogMigrator(params);

    List<TableIdentifier> identifiers;
    if (tableIdentifiers.isEmpty()) {
      if (identifiersRegEx == null) {
        printWriter.println(
            String.format(
                "%nUser has not specified the table identifiers."
                    + " Selecting all the tables from all the namespaces from the source catalog."));
      } else {
        printWriter.println(
            String.format(
                "%nUser has not specified the table identifiers."
                    + " Selecting all the tables from all the namespaces from the source catalog "
                    + "which matches the regex pattern:"
                    + identifiersRegEx));
      }
      identifiers = catalogMigrator.getMatchingTableIdentifiers(identifiersRegEx);
    } else {
      identifiers = tableIdentifiers;
    }

    String operation = deleteSourceCatalogTables ? "migration" : "registration";
    printWriter.println(
        String.format("%nIdentified %d tables for %s.", identifiers.size(), operation));

    ImmutableCatalogMigrationResult.Builder resultBuilder =
        ImmutableCatalogMigrationResult.builder();
    if (isDryRun) {
      CatalogMigrationResult result =
          resultBuilder.addAllRegisteredTableIdentifiers(identifiers).build();
      writeToFile(pathWithOutputDir(DRY_RUN_FILE), result.registeredTableIdentifiers());
      printWriter.println("Dry run is completed.");
      printDryRunResults(result, printWriter);
      return 0;
    }

    printWriter.println(String.format("%nStarted %s ...", operation));

    List<List<TableIdentifier>> IdentifierBatches = Lists.partition(identifiers, BATCH_SIZE);
    AtomicInteger counter = new AtomicInteger();
    IdentifierBatches.forEach(
        identifierBatch -> {
          catalogMigrator.registerTables(identifierBatch, resultBuilder);
          printWriter.println(
              String.format(
                  "%nAttempted %s for %d tables out of %d tables.",
                  operation, counter.incrementAndGet() * BATCH_SIZE, identifiers.size()));
        });

    CatalogMigrationResult result = resultBuilder.build();
    writeToFile(
        pathWithOutputDir(FAILED_IDENTIFIERS_FILE), result.failedToRegisterTableIdentifiers());
    writeToFile(
        pathWithOutputDir(FAILED_TO_DELETE_AT_SOURCE_FILE),
        result.failedToDeleteTableIdentifiers());

    printWriter.println(String.format("%nFinished %s ...", operation));
    printSummary(result, printWriter, sourceCatalog.name(), targetCatalog.name());
    printDetails(result, printWriter);
    return 0;
  }

  private boolean canProceed(PrintWriter printWriter, Catalog sourceCatalog) {
    if (isDryRun || disablePrompts) {
      return true;
    }
    if (deleteSourceCatalogTables) {
      if (sourceCatalog instanceof HadoopCatalog) {
        printWriter.println(
            String.format(
                "[WARNING]: Source catalog type is HADOOP and it doesn't support dropping tables just from "
                    + "catalog. %nAvoid operating the migrated tables from the source catalog after migration. "
                    + "Use the tables from target catalog."));
      }
      return PromptUtil.proceedForMigration(printWriter);
    } else {
      return PromptUtil.proceedForRegistration(printWriter);
    }
  }

  private List<TableIdentifier> processIdentifiersInput(PrintWriter printWriter) {
    List<TableIdentifier> tableIdentifiers;
    if (identifiersFromFile != null) {
      try {
        printWriter.println(
            String.format("Collecting identifiers from the file %s...", identifiersFromFile));
        printWriter.println();
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
    } else {
      tableIdentifiers = Collections.emptyList();
    }
    return tableIdentifiers;
  }

  private void validateIdentifierOptions() {
    if (identifiersFromFile != null && !identifiers.isEmpty() && identifiersRegEx != null) {
      throw new IllegalArgumentException(
          "All the three identifier options (`--identifiers`, `--identifiers-from-file`, "
              + "`--identifiers-regex`) are configured. Please use only one of them.");
    } else if (identifiersFromFile != null) {
      if (!identifiers.isEmpty()) {
        throw new IllegalArgumentException(
            "Both `--identifiers` and `--identifiers-from-file` options are configured. Please use only one of them.");
      } else if (identifiersRegEx != null) {
        throw new IllegalArgumentException(
            "Both `--identifiers-regex` and `--identifiers-from-file` options are configured. Please use only one of them.");
      } else {
        if (!Files.exists(Paths.get(identifiersFromFile))) {
          throw new IllegalArgumentException(
              "File specified in `--identifiers-from-file` option does not exist.");
        }
      }
    } else if (!identifiers.isEmpty()) {
      if (identifiersRegEx != null) {
        throw new IllegalArgumentException(
            "Both `--identifiers-regex` and `--identifiers` options are configured. Please use only one of them.");
      }
    }
  }

  private void printSummary(
      CatalogMigrationResult result,
      PrintWriter printWriter,
      String sourceCatalogType,
      String targetCatalogType) {
    printWriter.println(String.format("%nSummary: "));
    if (!result.registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Successfully %s %d tables from %s catalog to %s catalog.",
              deleteSourceCatalogTables ? "migrated" : "registered",
              result.registeredTableIdentifiers().size(),
              sourceCatalogType,
              targetCatalogType));
    }
    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Failed to %s %d tables from %s catalog to %s catalog. "
                  + "Please check the `catalog_migration.log` file for the failure reason. "
                  + "%nFailed identifiers are written into `%s`. "
                  + "Retry with that file using `--identifiers-from-file` option "
                  + "if the failure is because of network/connection timeouts.",
              deleteSourceCatalogTables ? "migrate" : "register",
              result.failedToRegisterTableIdentifiers().size(),
              sourceCatalogType,
              targetCatalogType,
              FAILED_IDENTIFIERS_FILE));
    }
    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Failed to delete %d tables from %s catalog. "
                  + "Please check the `catalog_migration.log` file for the reason. "
                  + "%nFailed to delete identifiers are written into `%s`. ",
              result.failedToDeleteTableIdentifiers().size(),
              sourceCatalogType,
              FAILED_TO_DELETE_AT_SOURCE_FILE));
    }
  }

  private void printDetails(CatalogMigrationResult result, PrintWriter printWriter) {
    printWriter.println(String.format("%nDetails: "));
    if (!result.registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Successfully %s these tables:",
              deleteSourceCatalogTables ? "migrated" : "registered"));
      printWriter.println(result.registeredTableIdentifiers());
    }

    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Failed to %s these tables:", deleteSourceCatalogTables ? "migrate" : "register"));
      printWriter.println(result.failedToRegisterTableIdentifiers());
    }

    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      printWriter.println("- [WARNING] Failed to delete these tables from source catalog:");
      printWriter.println(result.failedToDeleteTableIdentifiers());
    }
  }

  private void printDryRunResults(CatalogMigrationResult result, PrintWriter printWriter) {
    printWriter.println(String.format("%nSummary: "));
    if (result.registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- No tables are identified for %s. Please check logs for more info.",
              deleteSourceCatalogTables ? "migration" : "registration"));
      return;
    }
    printWriter.println(
        String.format(
            "- Identified %d tables for %s by dry-run. These identifiers are also written into %s. "
                + "You can use this file with `--identifiers-from-file` option.",
            result.registeredTableIdentifiers().size(),
            deleteSourceCatalogTables ? "migration" : "registration",
            DRY_RUN_FILE));

    printWriter.println(String.format("%nDetails: "));
    printWriter.println(
        String.format(
            "- Identified these tables for %s by dry-run:",
            deleteSourceCatalogTables ? "migration" : "registration"));
    printWriter.println(result.registeredTableIdentifiers());
  }

  private static void writeToFile(Path filePath, List<TableIdentifier> identifiers) {
    List<String> identifiersString =
        identifiers.stream().map(TableIdentifier::toString).collect(Collectors.toList());
    try {
      Files.write(filePath, identifiersString);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write the file:" + filePath, e);
    }
  }

  private Path pathWithOutputDir(String fileName) {
    if (outputDirPath == null) {
      return Paths.get(fileName);
    }
    return outputDirPath.resolve(fileName);
  }

  private static String catalogImpl(CatalogType type, String customCatalogImpl) {
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