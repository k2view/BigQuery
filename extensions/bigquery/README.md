# BigQuery Connector

>
> This version is compatible with **Fabric 8.2**.  
> TDM Table-Level flows are compatible with **TDM 9.3**.  
> If you're using **Fabric 8.1**, please install v1.1.x.

## Overview
The BigQuery Connector provides integration with Google's BigQuery through two Broadway actors:
1. **BigQueryRead**: Utilizes the BigQuery Storage API to read data from BigQuery tables.
2. **BigQueryWrite**: Utilizes the BigQuery Storage API to write data to BigQuery tables.

Additionally, the connector allows the use of **DbCommand** to execute statements against a BigQuery interface.

---

## Limitations
1. **Record Updates**: Updating existing records in BigQuery is not supported.
2. **BigQueryWrite Actor**: This actor must be placed in a transaction stage.
3. **DbCommand**: 
   - Not optimized for high-throughput reads/writes compared to the Storage API.
   - The batch option in DbCommand is not supported for BigQuery.

---

## TDM Table-Level Tasks:
   - The connector provides the below flows:
     - BQTableLevelDelete - uses DbCommand.
     - BQTableLevelExtractByQuery - uses DbCommand.
     - BQTableLevelExtractByStorage - uses BigQueryRead actor.
     - BQTableLevelLoadByStorage - uses BigQueryWrite actor.
   - To specify which flows to use, add a row in the MTable `TableLevelInterfaces` for your BigQuery interface. 

---

## Change Log

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
