library(ODTP)

# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "pdw",
                                                                server = Sys.getenv("PDW_SERVER"),
                                                                user = NULL,
                                                                password = NULL,
                                                                port = Sys.getenv("PDW_PORT"))

# The name of the database schema where the CDM data can be found:
cdmDatabaseSchema <- ""

# The name of the database schema and table where the study-specific cohorts will be instantiated:
cohortDatabaseSchema <- ""
cohortTable <- ""
oracleTempSchema <- NULL

# Some meta-information that will be used by the export function:
databaseId <- "Synpuf"
maxCores <- parallel::detectCores()
StartDate <- "2006-01-01" # The start date of CDM-transformation
EndDate <- "2023-12-12" # The end date of CDM-transformation

# The folder where the study intermediate and result files will be written:
outputFolder <- paste0("/home/user/result/output_", databaseId)


execute(connectionDetails,
        cdmDatabaseSchema,
        cohortDatabaseSchema,
        cohortTable,
        outputFolder,
        databaseName = databaseId,
        createCohorts = TRUE,
        runPrescriptionNum = TRUE,
        runPathwayAnalysis = TRUE,
        runCohortMethod = TRUE,
        resultsToZip = TRUE,
        yearStartDate = StartDate,
        yearEndDate = EndDate,
        monthStartDate = StartDate,
        monthEndDate = EndDate)
