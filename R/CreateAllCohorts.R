#' Create the exposure and outcome cohorts
#'
#' @details
#' This function will create the exposure and outcome cohorts following the definitions included in
#' this package.
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema Schema name where intermediate data can be stored. You will need to have
#'                             write privileges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable          The name of the table that will be created in the work database schema.
#'                             This table will hold the exposure and outcome cohorts used in this
#'                             study.
#' @param oracleTempSchema     Should be used in Oracle to specify a schema where the user has write
#'                             privileges for storing temporary tables.
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/)
#'
#' @export
createAllCohorts <- function(connectionDetails,
                             cdmDatabaseSchema,
                             cohortDatabaseSchema,
                             cohortTable,
                             outputFolder,
                             yearStartDate,
                             yearEndDate) {

  if (!file.exists(outputFolder))
    dir.create(outputFolder)

  `%>%` <- magrittr::`%>%`

  connection <- DatabaseConnector::connect(connectionDetails)

  ParallelLogger::logInfo("Creating Drug, and Dataset Table")
  createCohorts(connection = connection,
               cdmDatabaseSchema = cdmDatabaseSchema,
               cohortDatabaseSchema = cohortDatabaseSchema,
               cohortTable = cohortTable,
               outputFolder = outputFolder)

  if (!file.exists(file.path(outputFolder, "tmpData")))
    dir.create(file.path(outputFolder, "tmpData"))
  tmpDir <- file.path(outputFolder, "tmpData")


  # Prepare DOB, Gender and observation period data
  ParallelLogger::logInfo("Creating DOB, Gender, Observation Table")
  sql <- SqlRender::readSql(file.path(getwd(), "inst/sql/getDOBGender.sql"))
  sql <- SqlRender::render(sql,
                           cdm_database_schema = cdmDatabaseSchema,
                           cohort_database_schema = cohortDatabaseSchema,
                           cohort_table = cohortTable,
                           yearStartDate = lubridate::year(yearStartDate),
                           yearEndDate = lubridate::year(yearEndDate))
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
  data_whole<- DatabaseConnector::querySql(connection, sql)
  saveRDS(data_whole, file.path(tmpDir, "DatasetCohort.RDS"))


  cohortsToCreate <- read.csv(file.path("inst", "settings", "CohortsToCreate.csv"))
  cohortsToCreate <- cohortsToCreate[cohortsToCreate$cohortType=="DRUG",]
  # Prepare data for drug cohorts
  ParallelLogger::logInfo("Preparing data for drug cohorts")
  for (i in cohortsToCreate[, "cohortId"]){
    writeLines(paste("Preparing data for drug cohort:", cohortsToCreate[cohortsToCreate$cohortId==i, "name"]))
    sql <- "SELECT * FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = @cohortId"
    sql <- SqlRender::render(sql,
                             cohort_database_schema = cohortDatabaseSchema,
                             cohort_table = cohortTable,
                             cohortId = i)
    sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
    data_drug <- DatabaseConnector::querySql(connection, sql)

    data_drug <- data_drug %>%
      dplyr::inner_join(data_whole %>% dplyr::select(SUBJECT_ID, GENDER, DOB), by = 'SUBJECT_ID') %>%
      dplyr::mutate(start_year = lubridate::year(COHORT_START_DATE),
                    start_yearMth = as.Date(format(COHORT_START_DATE, "%Y-%m-01")),
                    end_year = lubridate::year(COHORT_END_DATE),
                    end_yearMth = as.Date(format(COHORT_END_DATE, "%Y-%m-01")),
                    AGE = lubridate::as.period(lubridate::interval(DOB, as.Date(COHORT_START_DATE)))$year) %>%
      dplyr::arrange(SUBJECT_ID, COHORT_START_DATE)

    saveRDS(data_drug,file.path(tmpDir, paste0("drugCohort_", i, ".RDS")))
  }
  DatabaseConnector::disconnect(connection)
}

