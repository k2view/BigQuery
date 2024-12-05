# BigQuery Connector

**Note**: This version is compatible with Fabric v8.1.

## Broadway Actors
The extension provides 3 Broadway actors to integrate with Google's BigQuery:
1. BigQueryRead - The actor utilizes BigQuery Storage API to read data from a BigQuery table.
2. BigQueryWrite - The actor utilizes BigQuery Storage API to write data to a BigQuery table.
3. BigQueryCommand - execute SQL commands (not optimized for reads/writes)

## Limitations:
1. Updating an existing record is not supported. 
2. BigQueryWrite Actor must be put in a transaction stage. The reason is, that the last batch will be written to BigQuery in the commit() method which is called only if the actor is in a transaction stage.

## Change Log

### **v1.1.0**
### Added
- Support for discovery crawler job monitoring.

### Changed
- Introduced new interface type BigQuery. Now a BigQuery interface should be created only using this interface type and not via the Custom interface type.

### Fixed
- Data platform name in Catalog was appearing as `bigQueryIoProvider` instead of the interface name.
- Add mandatory properties to the field node in Catalog: 
    - Column size
    - Sql data type
    - Ordinal position

### **v1.0.0**
- Initial version