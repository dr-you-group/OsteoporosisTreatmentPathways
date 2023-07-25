#' Run treatment pathway Analysis
#'
#' @description
#' Run the treatment pathways analysis code
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cohortDatabaseSchema Schema name where intermediate data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable          The name of the table that will be created in the work database schema.
#'                             This table will hold the exposure and outcome cohorts used in this
#'                             study.
#' @param oracleTempSchema     Should be used in Oracle to specify a schema where the user has write
#'                             priviliges for storing temporary tables.
#' @param outputFolder         Name of local folder where the results were generated; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#' @export

runPathwayAnalysis <- function(connectionDetails,
                               cohortDatabaseSchema,
                               cohortTable,
                               databaseName,
                               outputFolder){

  tpOutputFolder <- file.path(outputFolder, "results")
  if (!file.exists(tpOutputFolder)){
    dir.create(tpOutputFolder)
  }

  conn <- DatabaseConnector::connect(connectionDetails)

  # Create treatment pathway table structure:
  sql <- SqlRender::readSql(file.path(getwd(),"inst/sql/CreatePathwaysTable.sql"))
  sql <- SqlRender::render(sql = sql,
                           cohortDatabaseSchema=cohortDatabaseSchema,
                           cohortTable=cohortTable)
  DatabaseConnector::executeSql(connection = conn, sql = sql, progressBar = TRUE)

  # Create treatment pathway code table:
  codeTable <- read.csv(file.path(getwd(), "inst/settings/PathwayAnalysisCodes.csv"))
  DatabaseConnector::insertTable(connection = conn,
                                 databaseSchema = cohortDatabaseSchema,
                                 tableName = paste0(cohortTable, "_Osteoporosis_pathway_analysis_code"),
                                 data = codeTable,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 progressBar = TRUE)

  # Instantiate analysis:
  ParallelLogger::logInfo("Running treatment pathway analysis")
  cohortsToCreate <- read.csv(file.path("inst", "settings", "CohortsToCreate.csv"))
  cohortsToCreate <- cohortsToCreate[cohortsToCreate$cohortType=='Target',]
  pathway_results <- data.frame()
  for (id in cohortsToCreate$cohortId){
    ParallelLogger::logInfo(paste0("Execute pathways :", cohortsToCreate[cohortsToCreate$cohortId==id,]$atlasName))
    excSql <- SqlRender::readSql(file.path(getwd(),"inst/sql/TreatmentPathways.sql"))
    excSql <- SqlRender::render(sql = excSql,
                                cohortId=id,
                                generationId=id,
                                cohortDatabaseSchema=cohortDatabaseSchema,
                                cohortTable=cohortTable)
    DatabaseConnector::executeSql(connection = conn, sql = excSql, progressBar = TRUE)

    resultSql <- paste("select pathway_analysis_generation_id, target_cohort_id, t2.name as 'step_1', t3.name as 'step_2', t4.name as 'step_3', t1.count_value",
                       "from @cohortDatabaseSchema.@cohortTable_Osteoporosis_pathway_analysis_paths t1,",
                       "@cohortDatabaseSchema.@cohortTable_Osteoporosis_pathway_analysis_code t2,",
                       "@cohortDatabaseSchema.@cohortTable_Osteoporosis_pathway_analysis_code t3,",
                       "@cohortDatabaseSchema.@cohortTable_Osteoporosis_pathway_analysis_code t4",
                       "where pathway_analysis_generation_id=@generationId",
                       "and t1.step_1=t2.code and t1.step_2=t3.code and t1.step_3=t4.code;")
    resultSql <- SqlRender::render(sql = resultSql,
                                   generationId=id,
                                   cohortDatabaseSchema=cohortDatabaseSchema,
                                   cohortTable=cohortTable)
    result <- DatabaseConnector::querySql(connection = conn, sql = resultSql)
    pathway_results <- rbind(pathway_results, result)
  }

  write.csv(x = pathway_results, file = file.path(tpOutputFolder, "pathways.csv"))

}
