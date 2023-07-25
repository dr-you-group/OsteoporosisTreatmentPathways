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

   **R version 4.2 is required to run this package.**

3. Open your study package in RStudio. Use the following code to install all the dependencies:

	```r
	renv::restore()
	```

4. In RStudio, select 'Build' then 'Install and Restart' to build the package.

3. Once installed, you can execute the study by modifying and using the code below. For your convenience, this code is also provided under `extras/CodeToRun.R`:

	```r
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
            runDUR = TRUE,
            runPathwayAnalysis = TRUE,
            resultsToZip = TRUE,
            yearStartDate = as.Date("2006-01-01"),
            yearEndDate = as.Date("2022-12-31"),
            monthStartDate = as.Date("2006-01-01"),
            monthEndDate = as.Date("2022-12-31"))
    ```
4. Send the zip file ```<DatabaseId>_results.zip``` in the output folder to the study coordinator.
