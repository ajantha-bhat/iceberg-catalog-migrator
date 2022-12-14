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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.dell.ecs.EcsCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import picocli.CommandLine;

public abstract class BaseCatalogCommand extends BaseCommand {

  @CommandLine.Parameters(
      index = "0",
      description =
          "catalog type. Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]")
  private CatalogMigrationCLI.CatalogType catalogType;

  @CommandLine.Option(
      names = {"--custom-impl"},
      description =
          "optional fully qualified class name of the custom catalog implementation of the catalog. Required "
              + "when the catalog type is CUSTOM.")
  String customCatalogImpl;

  @CommandLine.Option(
      names = {"-P", "--properties"},
      split = ",",
      required = true,
      description = "catalog properties.")
  Map<String, String> properties = new HashMap<>();

  @CommandLine.Option(
      names = {"-C", "--hadoop-conf"},
      split = ",",
      description = "optional Hadoop configuration, required when using an Iceberg FileIO.")
  Map<String, String> hadoopConf = new HashMap<>();

  private Catalog catalog;

  @Override
  public Integer call() throws Exception {
    Configuration catalogConf = new Configuration();
    if (hadoopConf != null && !hadoopConf.isEmpty()) {
      hadoopConf.forEach(catalogConf::set);
    }
    catalog =
        CatalogUtil.loadCatalog(
            Objects.requireNonNull(catalogImpl(catalogType, customCatalogImpl)),
            catalogType.name(),
            properties,
            catalogConf);
    return 0;
  }

  public Catalog catalog() {
    return catalog;
  }

  @Override
  protected Integer executeCommand() throws Exception {
    return null;
  }

  private String catalogImpl(CatalogMigrationCLI.CatalogType type, String customCatalogImpl) {
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
    }
    return null;
  }
}
