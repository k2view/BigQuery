# BigQuery Connector

> This version is compatible with Fabric 8.2 or higher. If you are using Fabric 8.1, please install v1.1.x.
> TDM table-level flows are compatible with TDM 9.3.  
> TDM templates are compatible with TDM 9.4.

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
   - Credentials JSON - the content of credentials JSON file.
   - [Credentials File](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) - specify the path to the credentials JSON file under `Credentials File`.
4. Set the `Data-Accessing Project Id` — the project from which the query jobs and Storage API read sessions will be initiated when accessing data in BigQuery.
5. Set the `Data Owner Project Id` to the project in which the datasets and tables exist.  
   > **Important:** You must use the placeholder __$projectId__ in your query so that it is dynamically replaced at runtime with the value of this property.  
   > For example:  
   > *_SELECT * FROM \`$projectId.dataset.table\`_*
   >  
   > If you instead write _SELECT * FROM \`dataset.table\`_, the query will target the project `Data-Accessing Project Id`.
6. (Optional) Check the flag `Use BigQuery Storage API for Data Snapshots` if you wish to use the Storage API for data snapshotting done by the discovery crawler job.
7. Redeploy the project and environments.
8. (Optional - for TDM Table Level) - The connector provides also the flows `BQTableLevelDelete` (uses DbCommand), `BQTableLevelExtractByQuery` (uses DbCommand), `BQTableLevelExtractByStorage` (uses BigQueryRead actor), `BQTableLevelLoadByStorage` (uses BigQueryWrite actor).
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

### v1.6.0
- Added support for complex fields (starting from TDM 9.4.2).

### v1.5.2
- Handle NPE in case of null inside an array in BigQueryWrite Actor (not really allowed, but previously it was failing in incorrect place).
- Avoid hanging writer stream cleanup in case an exception occurs before assigning a callback.
- Set definedBy of JSON to STRING instead of UNKNOWN (ComplexFieldParser plugin overrides it anyway, needed just in case the plugin is disabled or the value is null always)

### v1.5.1
- Support secret manager in `Credentials JSON` authentication mode.

### v1.5.0
- Add authentication mode option of `Credentials JSON`.

### v1.4.4
- Fix definedBy for Datetime.

### v1.4.3
- Fix catalog definedBy for bytes, boolean.
- Remove column size 0 from catalog (caused issue in masking).
- Accept java.sql.Timestamp in writes.

### v1.4.2
- Upgrade BigQuery jars to 2.54.0 to resolve vulnerabilities in dependencies: netty-common and netty-buffer.
- Upgrade Avro dependency org.apache.commons.commons-lang3 to 3.18.0 to resolve vulnerability.

### v1.4.1
- Fix parent rows mapper in TDM table population.

### v1.4.0
- Add TDM templates.
- Handle empty parent rows in LU table population

### v1.3.7
- Fix compilation error in BigQueryMetadata.

### v1.3.6
- Fixed affectedRows for DML statements using DbCommand.
- Added the ability to use the _$projectId_ placeholder in `DbCommand`, which is dynamically replaced by the `Data Owner Project Id`.
- Added support for LU table populations.
- Fixed issue with actor name in LU table population due to recent change in Fabric Studio 8.2 that wraps the LU table names with ''.

### v1.3.5
- Incorrect project id when fetching table schema during discovery.
- Added project id to snapshot query.

### v1.3.4
- Fix table-level flows

### v1.3.3
- Fixed projects specification in BigQuery actors and TDM-Table level flows.

### v1.3.2
- Fix bytes field parsing in Storage read.
- Remove NotNull annotation due to compilation issue.

### v1.3.1
- Added option in interface to override query job project id.

### v1.3.0
#### Added
- Add support for RANGE type.
- Add support for Google Application Default Credentials.
- Upgrade BigQuery SDK from 2.19.1 to 2.48.1.

#### Fixed
- Fix invalid class cast in discovery when last LU deployed is not k2_ws.
- Change monitor behavior - total fields updated only when done.
- Fix NPE in snapshot via storage
- Data types parsing
- Creation of QueryJob with interface project id instead of project id in service account file.
- Other minor fixes and refactorings

### v1.2.4
- Fixed fields input cast issue in BigQueryRead actor.

### v1.2.3
- Fix root table population issue when IID field is not a string.
- Proper parsing of datetime fields.
- Fix field type check to properly parse the query params in Table-Level extract.
- Added data-type checks and conversions before writes to BigQuery.

### v1.2.2
- Minor fixes for Table-Level Extract Flows

### v1.2.1
#### Fixed
- Table-Level Extract flows

### v1.2.0
#### Changed
- Dropped support for Fabric 8.1 and added support for Fabric 8.2.

#### Fixed
- Aborting a discovery job was not actually stopping the job. Support for proper job termination has been added.

### v1.1.3

#### Fixed
- Add missing function that parses TDM Table-Level Extract params to relevant BigQuery types.

#### Changed
- The Table-Level extract flow now expects parameters as an array of maps (containing field name, type, and value) instead of just values, in accordance with TDM 9.3.

### v1.1.2

#### Added
- Table-Level Extract/Load/Delete Flows for TDM tasks.
- Support for complex types (e.g., structs and arrays) in the catalog.

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
- Monitoring support for the discovery crawler job.

#### Changed
- Introduced a new interface type: **BigQuery**. BigQuery interfaces should now be created using this specific type instead of the **Custom** interface type.

#### Fixed
- Corrected the data platform name in the catalog (was displayed as `bigQueryIoProvider` instead of the correct interface name).
- Added mandatory catalog field properties:
  - Column size
  - SQL data type
  - Ordinal posi
