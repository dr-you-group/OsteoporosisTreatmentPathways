createCohorts <- function(connection,
                          cdmDatabaseSchema,
                          vocabularyDatabaseSchema = cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          cohortTable,
                          outputFolder) {

  resultsDir <- file.path(outputFolder, "results")
  drugOutcomeTable <- paste0(cohortTable,"_DRUG")

  # Create study cohort table structure:
  sql <- SqlRender::readSql(file.path(getwd(), "inst/sql/CreateCohortTable.sql"))
  sql <- SqlRender::render(sql,
                           cohort_database_schema = cohortDatabaseSchema,
                           cohort_table = cohortTable)
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  # Create study DRUG table structure:
  sql <- SqlRender::readSql(file.path(getwd(), "inst/sql/CreateDrugOutcomeTable.sql"))
  sql <- SqlRender::render(sql,
                           cohort_database_schema = cohortDatabaseSchema,
                           cohort_table = drugOutcomeTable)
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  # Instantiate cohorts (TARGET):
  cohortsToCreate <- read.csv(file.path(getwd(), "inst/settings/CohortsToCreate.csv"))
  cohortsToCreate <- cohortsToCreate[cohortsToCreate$cohortType =="P_Target",]
  for (i in 1:nrow(cohortsToCreate)) {
    writeLines(paste("Creating", cohortsToCreate$cohortType[i], "cohort:", cohortsToCreate$atlasName[i]))
    if (i==1){
      sql <- SqlRender::readSql(file.path("inst", "sql", paste0(cohortsToCreate$name[i], ".sql")))
      sql <- SqlRender::render(sql,
                               cdm_database_schema = cdmDatabaseSchema,
                               vocabulary_database_schema = cdmDatabaseSchema,
                               target_database_schema = cohortDatabaseSchema,
                               target_cohort_table = cohortTable,
                               target_cohort_id = cohortsToCreate$cohortId[i])
      sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
      DatabaseConnector::executeSql(connection, sql, progressBar = TRUE, reportOverallTime = TRUE)
    }
   else{
     sql <- SqlRender::readSql(file.path("inst", "sql", paste0(cohortsToCreate$name[i], ".sql")))
     sql <- SqlRender::render(sql,
                              cdm_database_schema = cdmDatabaseSchema,
                              vocabulary_database_schema = cdmDatabaseSchema,
                              target_database_schema = cohortDatabaseSchema,
                              target_cohort_table = cohortTable,
                              target_cohort_id = cohortsToCreate$cohortId[i],
                              StartYear = cohortsToCreate$StartYear[i])
     sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
     DatabaseConnector::executeSql(connection, sql, progressBar = TRUE, reportOverallTime = TRUE)
   }
  }
  # Instantiate cohorts(Outcome):
  cohortsToCreate <- read.csv(file.path(getwd(), "inst/settings/CohortsToCreate.csv"))
  cohortsToCreate <- cohortsToCreate[cohortsToCreate$cohortType == "P_Outcome",]
  for (i in 1:nrow(cohortsToCreate)) {
    writeLines(paste("Creating", cohortsToCreate$cohortType[i], "cohort:", cohortsToCreate$atlasName[i]))
    sql <- SqlRender::readSql(file.path("inst", "sql", paste0(cohortsToCreate$name[i], ".sql")))
    sql <- SqlRender::render(sql,
                             cdm_database_schema = cdmDatabaseSchema,
                             vocabulary_database_schema = cdmDatabaseSchema,
                             target_database_schema = cohortDatabaseSchema,
                             target_cohort_table = cohortTable,
                             target_cohort_id = cohortsToCreate$cohortId[i])
    sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
    DatabaseConnector::executeSql(connection, sql, progressBar = TRUE, reportOverallTime = TRUE)
  }


  # Instantiate cohorts(Incidence):
  cohortsToCreate <- read.csv(file.path(getwd(), "inst/settings/CohortsToCreate.csv"))
  cohortsToCreate <- cohortsToCreate[cohortsToCreate$cohortType %in% c("E_Target", "E_Outcome"),]
  for (i in 1:nrow(cohortsToCreate)) {
    writeLines(paste("Creating", cohortsToCreate$cohortType[i], "cohort:", cohortsToCreate$atlasName[i]))
    sql <- SqlRender::readSql(file.path("inst", "sql", paste0(cohortsToCreate$name[i], ".sql")))
    sql <- SqlRender::render(sql,
                             cdm_database_schema = cdmDatabaseSchema,
                             vocabulary_database_schema = cdmDatabaseSchema,
                             target_database_schema = cohortDatabaseSchema,
                             target_cohort_table = cohortTable,
                             target_cohort_id = cohortsToCreate$cohortId[i])
    sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
    DatabaseConnector::executeSql(connection, sql, progressBar = TRUE, reportOverallTime = TRUE)
  }

  # Instantiate cohorts(DRUG):
  cohortsToCreate <- read.csv(file.path(getwd(), "inst/settings/CohortsToCreate.csv"))
  cohortsToCreate <- cohortsToCreate[cohortsToCreate$cohortType =="DRUG",]
  for (i in 1:nrow(cohortsToCreate)) {
    writeLines(paste("Creating", cohortsToCreate$cohortType[i], "cohort:", cohortsToCreate$atlasName[i]))
    sql <- SqlRender::readSql(file.path("inst", "sql", paste0(cohortsToCreate$name[i], ".sql")))
    sql <- SqlRender::render(sql,
                             cdm_database_schema = cdmDatabaseSchema,
                             vocabulary_database_schema = cdmDatabaseSchema,
                             target_database_schema = cohortDatabaseSchema,
                             target_cohort_table = drugOutcomeTable,
                             target_cohort_id = cohortsToCreate$cohortId[i])
    sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
    DatabaseConnector::executeSql(connection, sql, progressBar = TRUE, reportOverallTime = TRUE)
  }

  # Fetch cohort counts:
  sql <- "SELECT cohort_definition_id, COUNT(distinct subject_id) AS P_Counts, count(*) AS R_COUNT FROM @cohort_database_schema.@cohort_table GROUP BY cohort_definition_id
          UNION ALL SELECT cohort_definition_id, COUNT(distinct subject_id) AS P_Counts, count(*) AS R_COUNT FROM @cohort_database_schema.@cohort_table_DRUG GROUP BY cohort_definition_id"
  sql <- SqlRender::render(sql,
                           cohort_database_schema = cohortDatabaseSchema,
                           cohort_table = cohortTable)
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
  incidencecounts <- DatabaseConnector::querySql(connection, sql)
  names(incidencecounts) <- SqlRender::snakeCaseToCamelCase(names(incidencecounts))
  cohortsToCreate <- read.csv(file.path(getwd(), "inst/settings/CohortsToCreate.csv"))
  counts <-  merge(data.frame(cohortDefinitionId = cohortsToCreate$cohortId, cohortName  = cohortsToCreate$atlasName),
                   incidencecounts, by = "cohortDefinitionId")
  write.csv(counts, file.path(resultsDir, "CohortCounts.csv"), row.names = F)
}

