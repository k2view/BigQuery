# BigQuery Connector V2
**Fabric version required: 7.2.1_103**

This project provides 3 actors to integrate with Google's BigQuery:
1. BigQueryRead - read data from a table in BigQuery DB.
2. BigQueryWrite - write data to a table in BigQuery DB.  
3. BigQueryCommand - execute SQL commands (not optimized for reads/writes)

## BigQueryRead
The actor is using BigQueryReadClient API to read data from a specific BigQuery table.  
Accepts 5 inputs: interface, dataset, table, filter, limit. The output is an iterator.  
The actor opens a read session to the big query database while considering the filter string, and each record is read by the iterator code, translated to a map and the iterator is sent as the output.  
Limitation on the number of returned records is supported using the 'limit' input (default is 0, meaning no limit), in case the limit is set to a number bigger than 0, the iterator will keep reading until the limit is reached.

## BigQueryWrite
The actor is using BigQueryWriteClient API to write data to a specific tables.  
Accepts 6 inputs: interface, dataset, table, data, batchSize, streamType.  
The actor aggregates records from "data" input into a batch, until it reaches the "batchSize" input. Once reached, the batch is written BigQuery. Only insert is supported - updating an existing record is not. 

**Actor must be put in a transaction stage.** The reason is, that the last batch will be written to BigQuery in the commit() method which is called only if the actor is in a transaction stage.

**streamType** - specifies which type of BigQuery stream to use:
- **DEFAULT** - a batch will be committed immediately upon its completion, regardless of any failures in previous batches. The last batch, if incomplete, will be committed at the end of the transaction.
- **PENDING** - all batches will be committed at the end of the transaction, and only in case no errors occurred in any of the batches.

**Notes:**
- **The interface must be a custom interface whose "IoProvider Function" property is set to "bigQueryIoProvider" and "Tech Type" property is set (manually) to "BigQuery". The "Data" property should hold a map of this structure:
  {"ProjectId":"k2view-coe","OAuthPvtKeyPath":"C:\\gcp-bigquery\\k2view-coe-cb81dc8507f8.json"}** 
- The authentication json file (specified in OAuthPvtKeyPath) will hold the connection credentials to the big query client, it is extracted in the READ/WRITE actors and used when opening the connection.
  
<img width="388" alt="image" src="https://github.com/k2view-COE-KB/BigQueryConnector/assets/104128649/9b678cfa-3df2-4974-b4ff-04060deac829">