# BigQuery Connector

>
> This version is compatible with **Fabric 8.2**.  
> TDM table-level flows are compatible with **TDM 9.3**.  
> If you are using **Fabric 8.1**, please install v1.1.x.

## Overview
The BigQuery Connector provides integration with Google's BigQuery platform through the following two Broadway actors:
1. **BigQueryRead**: Utilizes the BigQuery Storage API to read data from BigQuery tables.
2. **BigQueryWrite**: Utilizes the BigQuery Storage API to write data to BigQuery tables.

Additionally, the connector allows the use of **DbCommand** to execute statements against a BigQuery interface.

---

## Getting Started

1. Install the extension.
   > Note: If you are upgrading from v1.2 or older to v1.3 or later, you need to restart Fabric after the upgrade.
2. Create a BigQuery interface.
3. In the interface settings, set the desired credentials method:
   - [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
   - [Service Account Key File](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) - specify the path to the credentials JSON file under `OAuth Private Key Path`.
4. (Optional) Check the flag `Use BigQuery Storage API for Data Snapshots` if you wish to use the Storage API for data snapshotting done by the discovery crawler job.
5. Set the `Datasets Project Id` to the project in which the datasets and tables exist.
6. (Optional - defaults to `Datasets Project Id`) Set the `Query Jobs Project Id` - when running queries against BigQuery (i.e., not via the Storage API), you can set this property to the project in which you would like the query jobs to be created.
   >In such cases, please note that LU populations would not work since the query would be looking for datasets in this project id and not in `Datasets Project Id` (unless you manually prepend the datasets project id to the query - select * from project.dataset.table)
5. Redeploy the project implementation and the environments.
6. (Optional - for TDM table-level) - the connector also provides the following flows: `BQTableLevelDelete` (uses DbCommand), `BQTableLevelExtractByQuery` (uses DbCommand), `BQTableLevelExtractByStorage` (uses BigQueryRead actor), `BQTableLevelLoadByStorage` (uses BigQueryWrite actor).
   - Add the below row to the `TableLevelDefinitions` MTable (with your preffered choice of extract flow `BQTableLevelExtractByQuery`/`BQTableLevelExtractByStorage`):
      
      | interface_name           | schema_name | table_name | extract_flow                   | table_order | delete_flow        | load_flow                    |
      |--------------------------|-------------|------------|--------------------------------|-------------|--------------------|------------------------------|
      | <Your_BigQuery_Interface>  |             |            | BQTableLevelExtractByQuery     |             | BQTableLevelDelete | BQTableLevelLoadByStorage    |

   - Redeploy the `References` LU after updating the TableLevelDefinition MTable.

---

## Limitations
1. Updating existing records in is not supported.
2. BigQueryWrite actor must always be placed in a transaction stage.
3. DbCommand: 
   - Not optimized for high-throughput reads/writes compared to the Storage API.
   - The batch option in DbCommand is not supported.
4. TDM table-level: Data retention is not supported when the source table contains a complex field (array/struct/range), so the option "Do not retain" must be selected in that case.
---

## Change Log

### v1.3.2
- Fixed bytes field parsing in Storage read.
- Removed NotNull annotation due to compilation issue.

### v1.3.1
- Added an option in the interface to override query job project id.

### v1.3.0
#### Added
- Added support for RANGE type.
- Added support for Google Application Default Credentials.
- Upgraded BigQuery SDK from 2.19.1 to 2.48.1.

#### Fixed
- Fixed invalid class cast in discovery when last LU deployed is not k2_ws.
- Changed monitor behavior - total fields updated only when done.
- Fixed NPE in snapshot via storage.
- Data types parsing.
- Creation of QueryJob with interface project id instead of project id in service account file.
- Other minor fixes and refactorings.

### v1.2.4
- Fixed fields input cast issue in BigQueryRead actor.

### v1.2.3
- Fixed root table population issue when IID field is not a string.
- Proper parsing of datetime fields.
- Fixed field type check to properly parse the query params in table-level extract.
- Added data-type checks and conversions before writes to BigQuery.

### v1.2.2
- Minor fixes for table-level Extract Flows.

### v1.2.1
#### Fixed
- Table-level Extract flows.

### v1.2.0
#### Changed
- Dropped support for Fabric 8.1 and added support for Fabric 8.2.

#### Fixed
- Aborting a Discovery job was not actually stopping the job. Support for proper job termination has been added.

### v1.1.3

#### Fixed
- Added a missing function that parses TDM table-level Extract params to relevant BigQuery types.

#### Changed
- The table-level extract flow now expects parameters as an array of maps (containing field name, type, and value) instead of just values, in accordance with TDM 9.3.

### v1.1.2

#### Added
- Table-level extract/load/delete flows for TDM tasks.
- Support for complex types (e.g., structs and arrays) in the Catalog.

#### Changed
- BigQuery `DATE` type is now parsed to/from `java.sql.Date` (previously `LocalDate`).
- BigQuery `TIMESTAMP` type is now parsed to/from `java.sql.Timestamp` (previously `Instant`).
- **DbCommand** now supports BigQuery interfaces directly (eliminating the need for `BigQueryCommand`).
- **Discovery data snapshot** now uses queries instead of the Storage Read API. This behavior is configurable in the interface.

#### Fixed
- Resolved connection reuse issues in DbCommand to avoid opening a new connection for each query.
- Improved support for paginated query results using `iterateAll`.
- Fixed class loader issues when fetching the `Operation` value in BigQueryIoProvider.

---

### v1.1.0

#### Added
- Monitoring support for the Discovery crawler job.

#### Changed
- Introduced a new interface type: **BigQuery**. BigQuery interfaces should now be created using this specific type instead of the **Custom** interface type.

#### Fixed
- Corrected the data platform name in the Catalog (was displayed as `bigQueryIoProvider` instead of the correct interface name).
- Added mandatory Catalog field properties:
  - Column size
  - SQL data type
  - Ordinal posi
