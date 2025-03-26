package com.k2view.cdbms.usercode.common.BigQuery;

import static com.k2view.cdbms.usercode.common.BigQuery.BigQueryParamParser.parseAvroValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.k2view.broadway.exception.AbortException;
import com.k2view.fabric.common.IteratorTranslate;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.basic.IoSimpleRow;

public class BigQueryReadIoSession extends BigQuerySession {
	// private final Log log = Log.a(this.getClass());

	public static final String INPUT_CREDENTIALS_PATH = "credentialsPath";
	public static final String INPUT_DATASET = "dataset";
	public static final String INPUT_TABLE = "table";
	public static final String INPUT_FILTERS = "filter";
	public static final String INPUT_FIELDS = "fields";
	public static final String INPUT_LIMIT = "limit";
	public static final String INPUT_OPERATION = BigQueryIoProvider.OPERATION_PARAM_NAME;

	public BigQueryReadIoSession(Map<String, Object> props) {
		super(props);
	}

	@Override
	public IoCommand.Statement statement() {
		return new BigQueryReadStatement(() -> {
			if (aborted) {
				throw new AbortException("BigQuery Read session aborted!");
			}
		});
	}

	private class BigQueryReadStatement implements IoCommand.Statement {
		private BigQueryIterator bigQueryIterator;
		private BigQueryReadClient client;
		private Runnable assertAborted;

		BigQueryReadStatement(Runnable assertAborted) {
			this.assertAborted = assertAborted;
		}

		@Override
		public IoCommand.Result execute(Object... objects) throws IOException {
			@SuppressWarnings("unchecked")
			Map<String, Object> input = (Map<String, Object>) objects[0];
			String dataset = (String) input.get(INPUT_DATASET);
			String table = (String) input.get(INPUT_TABLE);
			String filter = input.get(INPUT_FILTERS) != null ? (String) input.get(INPUT_FILTERS) : "";
			long limit = input.get(INPUT_LIMIT) != null ? (long) input.get(INPUT_LIMIT) : 0;
			@SuppressWarnings("unchecked")
			Iterable<String> fields = input.get(INPUT_FIELDS) != null ? (Iterable<String>) input.get(INPUT_FIELDS)
					: new ArrayList<>();
			BigQueryReadSettings settings = BigQueryReadSettings.newBuilder()
					.setCredentialsProvider(FixedCredentialsProvider.create(credentials()))
					.build();
			Util.safeClose(this.client);
			// Set up the BigQueryReadClient with credentials
			assertAborted.run();
			this.client = BigQueryReadClient.create(settings);
			ReadSession session = createBqReadSession(dataset, table, filter, fields);
			String streamName = session.getStreamsCount() > 0 ? session.getStreams(0).getName() : null;
			String avroSchemaString = session.getAvroSchema().getSchema();
			org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaString);
			GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
			// Create Big Query Iterator to read and iterate over the records.
			this.bigQueryIterator = new BigQueryIterator(this.client, streamName, reader, limit, assertAborted);
			
			return new BigQueryReadResult();
		}

		@Override
		public void close() {
			if (bigQueryIterator != null) {
				bigQueryIterator = null;
			}
			Util.safeClose(client);
			client = null;
		}

		private ReadSession createBqReadSession(String dataset,
				String table, String filter, Iterable<String> fields) {
			// The project to initiate the read request from
			String parent = String.format("projects/%s", userProjectId);
			// Format table name to connect to
			String srcTable = String.format(
					"projects/%s/datasets/%s/tables/%s",
					datasetsProjectId, dataset, table);
			// Options to utilize the filter input for the read
			ReadSession.TableReadOptions options = ReadSession.TableReadOptions.newBuilder()
					.addAllSelectedFields(fields).setRowRestriction(filter)
					.build();
			// The data format is AVRO, easier to read and utilize
			ReadSession.Builder sessionBuilder = ReadSession.newBuilder()
					.setTable(srcTable)
					.setDataFormat(DataFormat.AVRO)
					.setReadOptions(options);
			CreateReadSessionRequest.Builder builder = CreateReadSessionRequest.newBuilder()
					.setParent(parent)
					.setReadSession(sessionBuilder)
					.setMaxStreamCount(1);
			// Return the Read Session
			return client.createReadSession(builder.build());
		}

		private class BigQueryReadResult implements Result {
			@Override
			public Iterator<IoCommand.Row> iterator() {
				// Translator that translates each Generic Record read by the bq itr into
				// IoSimpleRow
				// The translator class the "next" function of the bq itr, then runs the
				// translation function on the record to convert it to IoSimpleRow
				return new IteratorTranslate<>(bigQueryIterator, rec -> {
					List<Schema.Field> schemaFields = rec.getSchema().getFields();
					Map<String, Integer> keys = new LinkedHashMap<>();
					for (int i = 0; i < schemaFields.size(); i++) {
						keys.put(schemaFields.get(i).name(), i);
					}
					Object[] values = new Object[keys.size()];
					for (int i = 0; i < schemaFields.size(); i++) {
						values[i] = parseAvroValue(rec.get(i), schemaFields.get(i));
					}
					return new IoSimpleRow(values, keys);
				});
			}
		}

	}

	@Override
	public IoSessionCompartment compartment() {
		return IoSessionCompartment.SHARED;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getMetadata(Map<String, Object> params) throws Exception {
		return (T) new BigQueryMetadata(interfaceName, null, this, client(), datasetsProjectId, this.snapshotViaStorageApi,
				params);
	}
}
