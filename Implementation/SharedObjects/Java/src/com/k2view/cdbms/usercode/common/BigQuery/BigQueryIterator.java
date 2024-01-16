package com.k2view.cdbms.usercode.common.BigQuery;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.ByteString;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.Util;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.util.Collections;
import java.util.Iterator;

// Big Query Generic Record Iterator
public class BigQueryIterator implements  Iterator<GenericRecord> {
    private final Log log = Log.a(this.getClass());

    private BigQueryReadClient readClient;
    private final GenericDatumReader<GenericRecord> reader;
    private final Iterator<ReadRowsResponse> responseIterator;
    private BinaryDecoder binaryDecoder;
    private GenericRecord nextRecord;
    private final long limit;
    private long currRowsCount;

    public BigQueryIterator(BigQueryReadClient readClient, String streamName, GenericDatumReader<GenericRecord> reader, long limit) {
        this.readClient = readClient;
        this.reader = reader;
        this.responseIterator = streamName == null ? Collections.emptyIterator() :
                this.readClient.readRowsCallable().call(ReadRowsRequest.newBuilder().setReadStream(streamName).build()).iterator();
        this.limit = Math.max(limit, 0);
        this.currRowsCount = 0;
        advanceToNextRecord();
    }

    @Override
    public boolean hasNext() {
        return nextRecord != null;
    }

    public GenericRecord next() {
        GenericRecord currentRecord = nextRecord;
        advanceToNextRecord();
        return currentRecord;
    }

    /**
     * checks if the responseIterator still has next, meaning if there's still records to be read in the stream,
     * if so decodes it and reads it.
     */
    private void advanceToNextRecord() {
        if (this.limit > 0 && currRowsCount == this.limit){
            nextRecord = null;
            return;
        }
        try {
            if (binaryDecoder == null || binaryDecoder.isEnd()) {
                if (!responseIterator.hasNext()) {
                    nextRecord = null;
                    return;
                }
                ByteString avroRows = responseIterator.next().getAvroRows().getSerializedBinaryRows();
                binaryDecoder = DecoderFactory.get().binaryDecoder(avroRows.toByteArray(), binaryDecoder);

            }
            nextRecord = reader.read(null, binaryDecoder);
            currRowsCount ++;
        } catch (Exception e) {
            log.error("Error advancing BigQuery iterator: {}", e);
            Util.safeClose(readClient);
            readClient = null;
            nextRecord=null;
        }
    }
}