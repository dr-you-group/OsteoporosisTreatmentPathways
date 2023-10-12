
.libPaths("C:/Users/BDJ/Documents/R/win-library/library_4")
library(ODTP4HIRA)
# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sql server",
                                                                user = "djboo",
                                                                password = "a1234",
                                                                server = "10.19.10.248",
                                                                pathToDriver = "C:/Users/BDJ/Documents")

# The name of the database schema where the CDM data can be found:
cdmDatabaseSchema <- "CDM_v531_YUHS.CDM"

# The name of the database schema and table where the study-specific cohorts will be instantiated:
cohortDatabaseSchema <- "COHORTDB.DJBOO"
cohortTable <- "TEST1012"
oracleTempSchema <- NULL

# Some meta-information that will be used by the export function:
databaseId <- "Synpuf"

# The folder where the study intermediate and result files will be written:
outputFolder <- "C:/Users/BDJ/Desktop/CDM/results/v8"
connection <- DatabaseConnector::connect(connectionDetails)
# 
# # Optional: specify where the temporary files (used by the Andromeda package) will be created:
# options(andromedaTempFolder = "s:/andromeda"))
# 
# # Maximum number of cores to be used:
maxCores <- parallel::detectCores()
# 
# # The folder where the study intermediate and result files will be written:
# outputFolder <- "s:/ODTP4HIRA"
# #
# # # Details for connecting to the server:
# connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "pdw",
#                                                                 server = Sys.getenv("PDW_SERVER"),
#                                                                 user = NULL,
#                                                                 password = NULL,
#                                                                 port = Sys.getenv("PDW_PORT"))
# 
# # # The name of the database schema where the CDM data can be found:
# cdmDatabaseSchema <- "CDM_IBM_MDCD_V1153.dbo"
# # 
# # # The name of the database schema and table where the study-specific cohorts will be instantiated:
# cohortDatabaseSchema <- "scratch.dbo"
# cohortTable <- "mschuemi_skeleton"
# 
# # Some meta-information that will be used by the export function:
# databaseId <- "Synpuf"
# 
# # For Oracle: define a schema that can be used to emulate temp tables:
# oracleTempSchema <- NULL

execute(connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        oracleTempSchema = oracleTempSchema,
        outputFolder = outputFolder,
        databaseId = databaseId,
        databaseName = databaseId,
        databaseDescription = databaseId,
        createCohorts = FALSE,
        synthesizePositiveControls = FALSE,
        runAnalyses = TRUE,
        packageResults = TRUE,
        maxCores = maxCores)


zip::zip(zipfile = paste0(databaseId, "_results.zip"), files = "results", root = outputFolder)
