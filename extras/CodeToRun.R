library(ODTP4TMU)
Sys.setlocale(category = "LC_ALL", locale = "english")
# Optional: specify where the temporary files (used by the Andromeda package) will be created:
options(andromedaTempFolder = "./outputFolder")

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()

# The folder where the study intermediate and result files will be written:
outputFolder <- "./outputFolder"

# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "",
                                                                user = "",
                                                                password = "",
                                                                server = "",
                                                                pathToDriver = "")

# The name of the database schema where the CDM data can be found:
cdmDatabaseSchema <- ""

# The name of the database schema and table where the study-specific cohorts will be instantiated:
cohortDatabaseSchema <- ""
cohortTable <- ""
# # Some meta-information that will be used by the export function:
databaseId <- "TMU"
# 
# # For Oracle: define a schema that can be used to emulate temp tables:
oracleTempSchema <- NULL

execute(connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        oracleTempSchema = oracleTempSchema,
        outputFolder = outputFolder,
        databaseId = databaseId,
        databaseName = databaseId,
        databaseDescription = databaseDescription,
        createCohorts = TRUE,
        synthesizePositiveControls = TRUE,
        runAnalyses = TRUE,
        packageResults = TRUE,
        maxCores = maxCores)

resultsZipFile <- file.path(outputFolder, "export", paste0("Results_", databaseId, ".zip"))
dataFolder <- file.path(outputFolder, "shinyData")

# You can inspect the results if you want:
prepareForEvidenceExplorer(resultsZipFile = resultsZipFile, dataFolder = dataFolder)
launchEvidenceExplorer(dataFolder = dataFolder, blind = TRUE, launch.browser = FALSE)

# Upload the results to the OHDSI SFTP server:
privateKeyFileName <- ""
userName <- ""
uploadResults(outputFolder, privateKeyFileName, userName)
