package com.k2view.cdbms.usercode.common.BigQuery;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializtionError;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;

public class BigQueryWriteIoSession extends BigQuerySession {
	public static final String INPUT_DATASET = "dataset";
	public static final String INPUT_TABLE = "table";
	public static final String INPUT_STREAM_TYPE = "streamType";
	public static final String INPUT_BATCH_SIZE = "batchSize";
	public static final String INPUT_DATA = "data";
	public static final String INPUT_OPERATION = BigQueryIoProvider.OPERATION_PARAM_NAME;
	public static final String STREAM_TYPE_PENDING = "PENDING";
	public static final String STREAM_TYPE_DEFAULT = "DEFAULT";
	private static final int DEFAULT_BATCH_SIZE = 1000;
	private final Log log = Log.a(this.getClass());

	private final Object writeStreamLock = new Object();
	@GuardedBy("writeStreamLock")
	private WriteStream writeStream;

	private final Object batchDataLock = new Object();
	@GuardedBy("batchDataLock")
	private final Map<TableName, JSONArray> tableToBatchData = new LinkedHashMap<>();
	@GuardedBy("batchDataLock")
	private int accumulatedBatchSize = 0;

	private final BigQueryWriteStatement statement;
	private boolean inTransaction;

	public BigQueryWriteIoSession(Map<String, Object> sessionProps) {
		super(sessionProps);
		this.statement = new BigQueryWriteStatement();
	}

	@Override
	public IoCommand.Statement statement() {
		return statement;
	}

	@Override
	public boolean isTransactional() {
		return true;
	}

	@Override
	public void beginTransaction() {
		// set isTransaction=true in order to identify in BigQueryWriteStatement.batch()
		// that we're in a transaction and throw an error otherwise.
		log.debug("TRANSACTION BEGIN");
		inTransaction = true;
	}

	@Override
	public void commit() throws Exception {
		// Writes the last (incomplete) batch to BigQuery and cleans up.
		log.debug("COMMITTING");
		writeToBigQuery();
		cleanup(true);
	}

	@Override
	public void rollback() throws Exception {
		// In case an error occurs, although the stage is marked as a transaction,
		// the flow will only clean up on failure, without rolling back any changes.
		log.debug("IN ROLLBACK");
		cleanup(false);
	}

	@Override
	public void abort() throws Exception {
		// In case the process is aborted, need to clean up.
		log.debug("ABORTING");
		cleanup(false);
	}

	@Override
	public void close() throws Exception {
		// Cleanup on session close.
		log.debug("CLOSING SESSION");
		cleanup(true);
	}

	private void cleanup(boolean commit) throws Exception {
		// Close all resources
		log.debug("Cleaning up - {}", this);
		tableToBatchData.clear();
		if (writeStream != null) {
			log.debug("Closing writeStream {}", writeStream);
			if (commit) {
				writeStream.close();
			} else {
				writeStream.abort();
			}
		}
		writeStream = null;
		inTransaction = false;
	}

	private void writeToBigQuery() throws Exception {
		// Writes to the BigQuery write stream and clears the current batch data array.
		if (accumulatedBatchSize <= 0)
			return;
		synchronized (writeStreamLock) {
			for (Map.Entry<TableName, JSONArray> entry : tableToBatchData.entrySet()) {
				TableName tableName = entry.getKey();
				JSONArray tableBatchData = entry.getValue();
				try {
					writeStream.write(tableName.getDataset(), tableName.getTable(), tableBatchData);
				} catch (AppendSerializtionError e) {
					log.error("Error per row index: {}", e.getRowIndexToErrorMessage());
					throw e;
				}
			}
		}
		tableToBatchData.clear();
		accumulatedBatchSize = 0;
	}

	public class BigQueryWriteStatement implements IoCommand.Statement {
		Schema tableSchema;

		@Override
		public IoCommand.Result execute(Object... objects) throws UnsupportedOperationException {
			throw new UnsupportedOperationException("Only batch writing is supported.");
		}

		/*
		 * Should be called with an argument of type Map<String, Object>, for each
		 * object to be inserted.
		 * Expected inputs in objects[0] (Map<String, Object> arg):
		 * batchSize (Optional, int),
		 * data (Mandatory, Map),
		 * interface (Mandatory, string),
		 * dataset (Mandatory, string),
		 * table (Mandatory, string),
		 * BigQueryIoProvider.OPERATION_PARAM_NAME=BigQueryIoProvider.Operation.WRITE
		 */
		@Override
		public void batch(Object... objects) throws Exception {
			if (objects == null || objects.length == 0 || !(objects[0] instanceof Map)) {
				throw new IllegalArgumentException(
						"Either no args were provided, or wrong type of args. The first argument must be of type Map");
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> input = (Map<String, Object>) objects[0];
			if (!inTransaction) {
				throw new IllegalStateException("Must be in transaction!");
			}
			String dataset = (String) input.get(INPUT_DATASET);
			String table = (String) input.get(INPUT_TABLE);
			TableName parentTable = TableName.of(datasetsProjectId, dataset, table);
			if (this.tableSchema == null) {
				this.tableSchema = client()
						.getTable(TableId.of(datasetsProjectId, dataset, table))
						.getDefinition().getSchema();
			}
			// When called in the first time, will initialize the BigQuery write stream.
			synchronized (writeStreamLock) {
				if (writeStream == null) {
					String streamType = (String) input.get(INPUT_STREAM_TYPE);
					if (STREAM_TYPE_PENDING.equals(streamType)) {
						writeStream = WriteStream.createWriteStream(Type.PENDING, datasetsProjectId, credentials());
					} else if (STREAM_TYPE_DEFAULT.equals(streamType)) {
						writeStream = WriteStream.createWriteStream(datasetsProjectId, credentials());
					} else {
						writeStream = WriteStream.createWriteStream(Type.UNRECOGNIZED, datasetsProjectId,
								credentials());
					}
				}
			}

			// Take the default batch size if wasn't provided
			long batchSize = ParamConvertor.toInteger(input.get(INPUT_BATCH_SIZE));
			if (batchSize <= 0) {
				batchSize = DEFAULT_BATCH_SIZE;
			}

			// Create JSONObject from data map and add it to the batch array.
			@SuppressWarnings("unchecked")
			Map<String, Object> data = (Map<String, Object>) input.get(INPUT_DATA);
			if (!Util.isEmpty(data)) {
				data.replaceAll((k, v) -> {
					return BigQueryParamParser.parseToBqByField(v, tableSchema.getFields().get(k));
				});
			}
			JSONObject jsonObject = new JSONObject(data);

			synchronized (batchDataLock) {
				long finalBatchSize = batchSize;
				// Initialize the batch data array with the right capacity.
				tableToBatchData.computeIfAbsent(parentTable, key -> new JSONArray((int) finalBatchSize));
				tableToBatchData.get(parentTable).put(jsonObject);
				accumulatedBatchSize++;
				if (accumulatedBatchSize >= batchSize) {
					// Reached the defined batch size, send to BigQuery write stream.
					log.debug("Executing batch");
					writeToBigQuery();
				}
			}
		}

		@Override
		public void close() throws Exception {
			this.tableSchema = null;
		}
	}

	@Override
	public IoSessionCompartment compartment() {
		return IoSessionCompartment.SHARED;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getMetadata(Map<String, Object> params) throws Exception {
		return (T) new BigQueryMetadata(interfaceName, null, null, client(), datasetsProjectId,
				this.snapshotViaStorageApi,
				params);
	}
}