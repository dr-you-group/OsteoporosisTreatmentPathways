[Osteoporosis Treatment Pathways Analysis]
=============
- Analytics use case(s): Characterization
- Study type: Clinical Application
- Tags: Osteoporosis, TreatmentPathways, CDM
- Study lead: 
- Study lead forums tag: 
- Study start date: 2023-03-08
- Study end date: -
- Protocol: -
- Publications: -
- Results explorer: https://lijbdj2634.shinyapps.io/TreatmentPathways/


 As a chronic degenerative disease, osteoporosis requires a long-term treatment strategy. Recent guidelines emphasize that a sequential treatment strategy tailored to individual risk is a core component to effectively prevent fracture events. To provide further evidence to support the scheme in clinical practice, it is crucial to identify patterns of sequential therapy for osteoporosis in the real world. We aim to evaluate treatment patterns of osteoporosis medication in postmenopausal women using a common data model.

How to run
==========
1. Follow [these instructions](https://ohdsi.github.io/Hades/rSetup.html) for seting up your R environment, including RTools and Java.

   **R version 4.1.3 is required to run this package.**

2. Open your study package in RStudio. 

3. In RStudio, select 'Build' then 'Install and Restart' to build the package.

4. Once installed, you can execute the study by modifying and using the code below. For your convenience, this code is also provided under `extras/CodeToRun.R`:

	```r
    library(ODTP)
 

	# Maximum number of cores to be used:
	maxCores <- parallel::detectCores()
	 
	# The folder where the study intermediate and result files will be written:
	 outputFolder <- "s:/ODTP"
	 
	# Optional: specify where the temporary files (used by the Andromeda package) will be created:
	options(andromedaTempFolder = "s:/andromeda"))
	 
	# Details for connecting to the server:
	connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "pdw",
 									server = Sys.getenv("PDW_SERVER"),
									user = NULL,
									password = NULL,
									port = Sys.getenv("PDW_PORT"))
	 
	# The name of the database schema where the CDM data can be found:
	cdmDatabaseSchema <- "CDM_IBM_MDCD_V1153.dbo"
	 
	# The name of the database schema and table where the study-specific cohorts will be instantiated:
	cohortDatabaseSchema <- "scratch.dbo"
	cohortTable <- "ODTP" # Please, use this name

	# # Some meta-information that will be used by the export function:
	databaseId <- ""
	StartDate <- "" # The start date of CDM-transformation
	EndDate <- "" # The end date of CDM-transformation
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
			databaseDescription = databaseId,
			startDate = StartDate,
			endDate = EndDate,
			createCohorts = TRUE,
			synthesizePositiveControls = FALSE,
			runAnalyses = TRUE,
			packageResults = TRUE,
			maxCores = maxCores)


	zip::zip(zipfile = paste0(databaseId, "_results.zip"), files = "results", root = outputFolder)


    ```
5. Send the zip file ```<DatabaseId>_results.zip``` in the output folder to the study coordinator.
