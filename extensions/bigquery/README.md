# BigQuery Connector
**Fabric version required: 7.2.1_103**

The extension provides 3 Broadway actors to integrate with Google's BigQuery:
1. BigQueryRead - read data from a table in BigQuery DB.
2. BigQueryWrite - write data to a table in BigQuery DB.  
3. BigQueryCommand - execute SQL commands (not optimized for reads/writes)

If you want integration with BigQuery without Broadway, you can use the class BigQueryIoProvider to create a Read/Write/Command session.

## BigQueryRead
The actor utilizes BigQuery Storage API to read data from a BigQuery table.

## BigQueryWrite
The actor utilizes BigQuery Storage API to write data to a BigQuery table.
**Note**:
1. Updating an existing record is not supported. 
2. **Actor must be put in a transaction stage.** The reason is, that the last batch will be written to BigQuery in the commit() method which is called only if the actor is in a transaction stage.
3. **The interface must be a custom interface whose "IoProvider Function" property is set to "bigQueryIoProvider" and "Tech Type" property is set (manually) to "BigQuery". The "Data" property should hold a map of this structure:
  {"ProjectId":"k2view-coe","OAuthPvtKeyPath":"C:\\gcp-bigquery\\k2view-coe-cb81dc8507f8.json"}** 
- The authentication json file (specified in OAuthPvtKeyPath) should hold the connection credentials to BigQuery (used when opening the connection).
  
### License
[Open license file](/api/k2view/bigquery/0.0.1/file/LICENSE.txt)

