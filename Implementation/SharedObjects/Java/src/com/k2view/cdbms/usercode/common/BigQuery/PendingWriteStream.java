package com.k2view.cdbms.usercode.common.BigQuery;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import javax.annotation.concurrent.GuardedBy;

import com.k2view.fabric.common.Log;
import org.json.JSONArray;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;

public class PendingWriteStream implements WriteStream {
    private final Log log = Log.a(this.getClass());
    private final String projectId;
    private final BigQueryWriteClient bigQueryWriteClient;
    private final Map<String, DataWriter> dataWriters = new HashMap<>();

    public PendingWriteStream(String projectId, String credentialFilePath) throws IOException {
        this.projectId = projectId;
        log.debug("projectId={}", this.projectId);

        // If you don't specify credentials when constructing the client, the client library will
        // look for credentials via the environment variable GOOGLE_APPLICATION_CREDENTIALS.
        Path path = Paths.get(credentialFilePath);
        log.debug("path={}", path);

        ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(new FileInputStream(credentialFilePath));
        log.debug("credentials={}", credentials);

        BigQueryWriteSettings bigQueryWriteSettings = BigQueryWriteSettings
                .newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .build();
        log.debug("bigQueryWriteSettings={}", bigQueryWriteSettings);

        this.bigQueryWriteClient = BigQueryWriteClient.create(bigQueryWriteSettings);
        log.debug("bigQueryWriteClient={}", this.bigQueryWriteClient);
    }

    public void write(String dataset, String table, JSONArray rows)
            throws DescriptorValidationException, InterruptedException, IOException {
        log.debug("dataset={}", dataset);
        log.debug("table={}", table);
        TableName parentTable = TableName.of(this.projectId, dataset, table);
        log.debug("parentTable={}", parentTable);

        // If we already have a dataWriter for parentTable, we use it, otherwise create a new one
        // and save it in dataWriters
        DataWriter dataWriter = dataWriters.get(parentTable.toString());
        if (dataWriter == null) {
            log.debug("Instantiating data writer for {}", parentTable);
            dataWriter = new DataWriter();
            dataWriter.initialize(parentTable, this.bigQueryWriteClient);
            this.dataWriters.put(parentTable.toString(), dataWriter);
        }
        try {
            // Data may be batched up to the maximum request size:
            // https://cloud.google.com/bigquery/quotas#write-api-limits
            dataWriter.append(rows);
        } catch (ExecutionException e) {
            // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
            // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information, see:
            // https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
            log.error("Failed to append records: {}" + e);
        }
    }

    @Override
    public void close() throws Exception {
        for (Map.Entry<String, DataWriter> entry : dataWriters.entrySet()) {
            DataWriter dataWriter = entry.getValue();
            String parentTable = entry.getKey();
            dataWriter.cleanup(bigQueryWriteClient);
            log.debug("Appended records successfully for writer={}.", dataWriter);
            this.commit(parentTable);
        }
        dataWriters.clear();
        bigQueryWriteClient.shutdown();
        bigQueryWriteClient.close();
    }

    @Override
    public void abort() {
        log.debug("Aborting write streams");
        dataWriters.values().forEach((dataWriter -> dataWriter.cleanup(bigQueryWriteClient)));
    }

    private void commit(String parentTable) {
        // Once all streams are done, if all writes were successful, commit all of them in one request.
        // This example only has the one stream. If any streams failed, their workload may be
        // retried on a new stream, and then only the successful stream should be included in the
        // commit.
        log.debug("Committing write stream for {}", parentTable);
        DataWriter dataWriter = dataWriters.get(parentTable);
        BatchCommitWriteStreamsRequest commitRequest =
                BatchCommitWriteStreamsRequest.newBuilder()
                        .setParent(parentTable)
                        .addWriteStreams(dataWriter.getStreamName())
                        .build();
        BatchCommitWriteStreamsResponse commitResponse = bigQueryWriteClient.batchCommitWriteStreams(commitRequest);
        // If the response does not have a commit time, it means the commit operation failed.
        if (!commitResponse.hasCommitTime()) {
            for (StorageError err : commitResponse.getStreamErrorsList()) {
                log.error(err.getErrorMessage());
            }
            throw new RuntimeException("Error committing the streams");
        }
        log.debug("Appended and committed records successfully.");
    }

    // A simple wrapper object showing how the stateful stream writer should be used.
    private static class DataWriter {
        private final Log log = Log.a(DataWriter.this.getClass());
        private JsonStreamWriter streamWriter;
        // Track the number of in-flight requests to wait for all responses before shutting down.
        private final Phaser inFlightRequestCount = new Phaser(1);

        private final Object lock = new Object();

        @GuardedBy("lock")
        private RuntimeException error = null;

        void initialize(TableName parentTable , BigQueryWriteClient bigQueryWriteClient)
                throws IOException, DescriptorValidationException, InterruptedException {
            // Initialize a write-stream for the specified table.
            // For more information on WriteStream.Type, see:
            // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/WriteStream.Type.html
            com.google.cloud.bigquery.storage.v1.WriteStream stream = com.google.cloud.bigquery.storage.v1.WriteStream.newBuilder().setType(Type.PENDING).build();
            log.debug("stream={}", stream);

            CreateWriteStreamRequest createWriteStreamRequest =
                    CreateWriteStreamRequest.newBuilder()
                            .setParent(parentTable.toString())
                            .setWriteStream(stream)
                            .build();
            log.debug("createWriteStreamRequest={}", createWriteStreamRequest);

            com.google.cloud.bigquery.storage.v1.WriteStream writeStream = bigQueryWriteClient.createWriteStream(createWriteStreamRequest);
            log.debug("writeStream={}", writeStream);
            // Use the JSON stream writer to send records in JSON format.
            // For more information about JsonStreamWriter, see:
            // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1beta2/JsonStreamWriter.html
            streamWriter =
                    JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema()).build();
            log.debug("streamWriter={}", this.streamWriter);
        }

        public void append(JSONArray data)
                throws DescriptorValidationException, IOException, ExecutionException {
            synchronized (this.lock) {
                if (this.error != null) {
                    log.debug("Earlier appends have failed. Need to reset before continuing.");
                    throw this.error;
                }
            }
            // Increase the count of in-flight requests.
            inFlightRequestCount.register();
            log.debug("In-flight requests counter increased. registered={}", this.inFlightRequestCount.getRegisteredParties());

            // Append asynchronously for increased throughput.
            try {
                ApiFuture<AppendRowsResponse> future = streamWriter.append(data);
                log.debug("future={}", future);

                ApiFutures.addCallback(
                        future, new AppendCompleteCallback(this), MoreExecutors.directExecutor());
            } catch (Exceptions.AppendSerializtionError e) {
                log.error(e.getRowIndexToErrorMessage().toString());
                throw e;
            }
        }

        public void cleanup(BigQueryWriteClient client) {
            log.debug("Waiting for in-flight requests to complete. unarrived={}", this.inFlightRequestCount.getUnarrivedParties());
            inFlightRequestCount.arriveAndAwaitAdvance();

            this.streamWriter.close();


            // Verify that no error occurred in the stream.
            synchronized (this.lock) {
                if (this.error != null) {
                    log.debug("An error occurred in the stream.");
                    throw this.error;
                }
            }

            // Finalize the stream.
            FinalizeWriteStreamResponse finalizeResponse =
                    client.finalizeWriteStream(streamWriter.getStreamName());
            log.debug("Rows written: " + finalizeResponse.getRowCount());
        }

        public String getStreamName() {
            return streamWriter.getStreamName();
        }

        static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {
            private final Log log = Log.a(this.getClass());
            private final DataWriter parent;

            public AppendCompleteCallback(DataWriter parent) {
                this.parent = parent;
                log.debug("parent={}", this.parent);
            }

            public void onSuccess(AppendRowsResponse response) {
                log.debug("Append {} success", response.getAppendResult().getOffset().getValue());
                done();
            }

            public void onFailure(Throwable throwable) {
                log.debug("onFailure");
                synchronized (this.parent.lock) {
                    if (this.parent.error == null) {
                        StorageException storageException = Exceptions.toStorageException(throwable);
                        this.parent.error =
                                (storageException != null) ? storageException : new RuntimeException(throwable);
                    }
                }
                log.error("Error: {}", throwable.toString());
                done();
            }

            private void done() {
                log.debug("De-registering in-flight requests. unarrived={}", this.parent.inFlightRequestCount.getUnarrivedParties());
                this.parent.inFlightRequestCount.arriveAndDeregister();
                log.debug("Done! arrived={}", this.parent.inFlightRequestCount.getArrivedParties());
            }
        }
    }
}