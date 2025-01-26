# BigQuery Connector

**Note**: This version is compatible with **Fabric v8.1**.

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
     - BQTableLevelExtractByStorage - uses BigQueryRead actor, should be faster but doesn't support filtering.
     - BQTableLevelLoadByStorage - uses BigQueryWrite actor.
   - The connector includes the MTable `TableLevelInterfaces___bigquery` which defines the default flows for BigQuery interfaces. If you want to set a custom flow, don't change this file (unless you want to make it a default for all BigQuery interfaces), rather add a separate row in `TableLevelInterfaces` for the specific interface name.


---

## Change Log

### **v1.1.2**

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

### **v1.1.0**

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
