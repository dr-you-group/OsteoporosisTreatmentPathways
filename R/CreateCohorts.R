# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of ODTP4TMU
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.createCohorts <- function(connection,
                           cdmDatabaseSchema,
                           vocabularyDatabaseSchema = cdmDatabaseSchema,
                           cohortDatabaseSchema,
                           cohortTable,
                           oracleTempSchema,
                           outputFolder) {
  
  # Create study cohort table structure:
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CreateCohortTable.sql",
                                           packageName = "ODTP4TMU",
                                           dbms = attr(connection, "dbms"),
                                           oracleTempSchema = oracleTempSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  
  # Create study DRUG table structure:
  drugOutcomeTable <- paste0(cohortTable,"_DRUG")
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CreateDrugOutcomeTable.sql",
                                           packageName = "ODTP4TMU",
                                           dbms = attr(connection, "dbms"),
                                           oracleTempSchema = oracleTempSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = drugOutcomeTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  # Instantiate cohorts:
  pathToCsv <- system.file("settings", "CohortsToCreate.csv", package = "ODTP4TMU")
  cohortsToCreate <- read.csv(pathToCsv)
  
  # Instantiate cohorts (TARGET):
  cohortsToCreate_P <- cohortsToCreate[cohortsToCreate$cohortType =="P_Target",]
  for (i in 1:nrow(cohortsToCreate_P)) {
    writeLines(paste("Creating", cohortsToCreate_P$cohortType[i], "cohort:", cohortsToCreate_P$atlasName[i]))
    if (i==1){
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste0(cohortsToCreate_P$name[i], ".sql"),
                                               packageName = "ODTP4TMU",
                                               dbms = attr(connection, "dbms"),
                                               oracleTempSchema = oracleTempSchema,
                                               cdm_database_schema = cdmDatabaseSchema,
                                               vocabulary_database_schema = vocabularyDatabaseSchema,
                                               target_database_schema = cohortDatabaseSchema,
                                               target_cohort_table = cohortTable,
                                               target_cohort_id = cohortsToCreate_P$cohortId[i])
      DatabaseConnector::executeSql(connection, sql, progressBar = TRUE, reportOverallTime = TRUE)
    }
    else{
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste0(cohortsToCreate_P$name[i], ".sql"),
                                               packageName = "ODTP4TMU",
                                               dbms = attr(connection, "dbms"),
                                               oracleTempSchema = oracleTempSchema,
                                               cdm_database_schema = cdmDatabaseSchema,
                                               vocabulary_database_schema = vocabularyDatabaseSchema,
                                               target_database_schema = cohortDatabaseSchema,
                                               target_cohort_table = cohortTable,
                                               target_cohort_id = cohortsToCreate_P$cohortId[i],
                                               StartYear = cohortsToCreate_P$StartYear[i])
      DatabaseConnector::executeSql(connection, sql, progressBar = TRUE, reportOverallTime = TRUE)
    }
  }
  # Instantiate cohorts(Outcome):
  cohortsToCreate_O <- cohortsToCreate[cohortsToCreate$cohortType == "P_Outcome",]
  for (i in 1:nrow(cohortsToCreate_O)) {
    writeLines(paste("Creating", cohortsToCreate_O$cohortType[i], "cohort:", cohortsToCreate_O$atlasName[i]))
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste0(cohortsToCreate_O$name[i], ".sql"),
                                             packageName = "ODTP4TMU",
                                             dbms = attr(connection, "dbms"),
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             vocabulary_database_schema = vocabularyDatabaseSchema,
                                             target_database_schema = cohortDatabaseSchema,
                                             target_cohort_table = cohortTable,
                                             target_cohort_id = cohortsToCreate_O$cohortId[i])
    DatabaseConnector::executeSql(connection, sql, progressBar = TRUE, reportOverallTime = TRUE)
  }
  
  cohortsToCreate_E <- cohortsToCreate[cohortsToCreate$cohortType %in% c("E_Target", "E_Outcome"),]
  for (i in 1:nrow(cohortsToCreate_E)) {
    writeLines(paste("Creating cohort:", cohortsToCreate_E$name[i]))
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste0(cohortsToCreate_E$name[i], ".sql"),
                                             packageName = "ODTP4TMU",
                                             dbms = attr(connection, "dbms"),
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             vocabulary_database_schema = vocabularyDatabaseSchema,
                                             target_database_schema = cohortDatabaseSchema,
                                             target_cohort_table = cohortTable,
                                             target_cohort_id = cohortsToCreate_E$cohortId[i])
    DatabaseConnector::executeSql(connection, sql)
  }
  
  # Instantiate cohorts(DRUG):
  cohortsToCreate_D <- cohortsToCreate[cohortsToCreate$cohortType =="DRUG",]
  for (i in 1:nrow(cohortsToCreate_D)) {
    writeLines(paste("Creating", cohortsToCreate_D$cohortType[i], "cohort:", cohortsToCreate_D$atlasName[i]))
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste0(cohortsToCreate_D$name[i], ".sql"),
                                             packageName = "ODTP4TMU",
                                             dbms = attr(connection, "dbms"),
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             vocabulary_database_schema = vocabularyDatabaseSchema,
                                             target_database_schema = cohortDatabaseSchema,
                                             target_cohort_table = drugOutcomeTable,
                                             target_cohort_id = cohortsToCreate_D$cohortId[i])
    DatabaseConnector::executeSql(connection, sql, progressBar = TRUE, reportOverallTime = TRUE)
  }
  
  # Fetch cohort counts:
  pathToCsv <- system.file("settings", "CohortsToCreate.csv", package = "ODTP4TMU")
  cohortsToCreate <- read.csv(pathToCsv)
  sql <- "SELECT cohort_definition_id, COUNT(*) AS count FROM @cohort_database_schema.@cohort_table GROUP BY cohort_definition_id union all SELECT cohort_definition_id, COUNT(*) AS count FROM @cohort_database_schema.@cohort_table_DRUG GROUP BY cohort_definition_id"
  sql <- SqlRender::render(sql,
                              cohort_database_schema = cohortDatabaseSchema,
                              cohort_table = cohortTable)
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
  counts <- DatabaseConnector::querySql(connection, sql)
  names(counts) <- SqlRender::snakeCaseToCamelCase(names(counts))
  counts <- merge(counts, data.frame(cohortDefinitionId = cohortsToCreate$cohortId,
                                     cohortName  = cohortsToCreate$name))
  write.csv(counts, file.path(outputFolder, "CohortCounts.csv"))
  
  
}

