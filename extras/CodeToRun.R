library(ODTP)

# The folder where the study intermediate and result files will be written:
outputFolder <- "outputFolder"

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

# Some meta-information that will be used by the export function:
databaseId <- "Synpuf"


execute(connectionDetails,
        cdmDatabaseSchema,
        cohortDatabaseSchema,
        cohortTable,
        outputFolder,
        databaseName = databaseId,
        createCohorts = TRUE,
        runPrescriptionNum = TRUE,
        runPathwayAnalysis = TRUE,
        resultsToZip = TRUE,
        yearStartDate = as.Date("2006-01-01"),
        yearEndDate = as.Date("2022-12-31"),
        monthStartDate = as.Date("2006-01-01"),
        monthEndDate = as.Date("2022-12-31"))
