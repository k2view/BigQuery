package com.k2view.cdbms.usercode.common.BigQuery;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoSession;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * NOTE ON TEST STRATEGY:
 * BigQueryReadStatement.execute(...) creates a real BigQueryReadClient via
 * BigQueryReadClient.create(settings) and then a real ReadSession via
 * client.createReadSession(...) - both are live Google Cloud network calls. There is no way to
 * unit-test execute() end-to-end without a fake/mocked gRPC server (e.g. wiremock or an in-process
 * gRPC test server standing in for the BigQuery Storage Read API) standing behind
 * BigQueryReadSettings' channel provider; that is out of scope here and is not attempted.
 *
 * What IS unit-tested below:
 *  - compartment() - trivial, no I/O.
 *  - getMetadata(...) - only the observable fact that it does not silently succeed without a live
 *    Fabric interface registry / real GCP credentials (see the test for why, and its limits).
 *  - The Avro GenericRecord -> IoSimpleRow translation logic inside
 *    BigQueryReadStatement.BigQueryReadResult.iterator() - by reflectively injecting a fake
 *    BigQueryIterator (a subclass overriding hasNext()/next(), constructed with streamName=null so
 *    its own constructor never touches a real BigQueryReadClient) into the private
 *    "bigQueryIterator" field of a real BigQueryReadStatement obtained via session.statement(),
 *    then reflectively constructing the private BigQueryReadResult inner class to call its
 *    iterator(). This exercises parseAvroValue(...) and the LinkedHashMap field-order-preserving
 *    key map exactly as production code would, without needing any network access.
 */
class BigQueryReadIoSessionTest {

    private static Map<String, Object> baseProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(BigQueryIoProvider.SESSION_PROP_INTERFACE, "testInterface");
        props.put(BigQueryIoProvider.SESSION_PROP_USER_PROJECT, "user-project");
        props.put(BigQueryIoProvider.SESSION_PROP_DATASETS_PROJECT, "datasets-project");
        props.put(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD, "default");
        props.put(BigQueryIoProvider.SESSION_PROP_SNAPSHOT_VIA_STORAGE, "false");
        return props;
    }

    // --- compartment ---

    @Test
    void compartment_returnsShared() {
        BigQueryReadIoSession session = new BigQueryReadIoSession(baseProps());

        assertEquals(IoSession.IoSessionCompartment.SHARED, session.compartment());
    }

    // --- getMetadata: integration boundary ---

    @Test
    void getMetadata_withoutLiveInterfaceRegistryOrRealCredentials_throws() {
        // getMetadata(...) unconditionally passes commandIoSession=null into BigQueryMetadata's
        // constructor, which (when null) falls back to
        // InterfacesManager.getInstance().getInterface(interfaceName).getIoSession(null) - a real
        // Fabric interface registry lookup that has nothing registered for "testInterface" in a
        // plain unit test process. Depending on credential/auth setup this can also fail earlier,
        // at client() -> credentials() (here: authenticationMethod=json with a deliberately
        // malformed credentialsJSON, so GoogleCredentials.fromStream(...) fails fast and locally -
        // no network I/O - rather than risking a slow/hanging real ADC/metadata-server lookup).
        // Either way, full success requires live Fabric+GCP wiring that is out of scope for a unit
        // test; we only assert that this integration boundary is not silently papered over.
        Map<String, Object> props = baseProps();
        props.put(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD, "json");
        props.put(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_JSON, "{}");
        BigQueryReadIoSession session = new BigQueryReadIoSession(props);

        assertThrows(Exception.class, () -> session.getMetadata(new HashMap<>()));
    }

    // --- BigQueryReadStatement / BigQueryReadResult translation logic ---

    @Test
    void statementResultIterator_translatesGenericRecordToIoSimpleRow_preservingFieldOrderAndValues() throws Exception {
        BigQueryReadIoSession session = new BigQueryReadIoSession(baseProps());
        IoCommand.Statement stmt = session.statement();

        Schema schema = Schema.createRecord("TestRow", null, null, false, List.of(
                new Schema.Field("id", Schema.create(Schema.Type.LONG), null, (Object) null),
                new Schema.Field("name", Schema.create(Schema.Type.STRING), null, (Object) null)));
        GenericData.Record record = new GenericData.Record(schema);
        record.put("id", 7L);
        record.put("name", "Alice");

        BigQueryIterator fakeIterator = new BigQueryIterator(null, null, null, 0, () -> { }) {
            private boolean served = false;

            @Override
            public boolean hasNext() {
                return !served;
            }

            @Override
            public GenericRecord next() {
                served = true;
                return record;
            }
        };

        Field bigQueryIteratorField = stmt.getClass().getDeclaredField("bigQueryIterator");
        bigQueryIteratorField.setAccessible(true);
        bigQueryIteratorField.set(stmt, fakeIterator);

        Class<?> resultClass = Class.forName(stmt.getClass().getName() + "$BigQueryReadResult");
        Constructor<?> resultCtor = resultClass.getDeclaredConstructor(stmt.getClass());
        resultCtor.setAccessible(true);
        Object result = resultCtor.newInstance(stmt);

        @SuppressWarnings("unchecked")
        Iterator<IoCommand.Row> rowIterator = (Iterator<IoCommand.Row>) resultClass.getMethod("iterator").invoke(result);

        assertTrue(rowIterator.hasNext());
        IoCommand.Row row = rowIterator.next();

        assertEquals(7L, row.get("id"));
        assertEquals("Alice", row.get("name"));
        assertEquals(List.of("id", "name"), new ArrayList<>(row.keySet()),
                "field order from the Avro schema must be preserved via the LinkedHashMap key map");
        assertFalse(rowIterator.hasNext(), "the translating iterator must end when the underlying BigQueryIterator does");
    }

    @Test
    void statementResultIterator_nullAvroFieldValue_translatesToNull() throws Exception {
        BigQueryReadIoSession session = new BigQueryReadIoSession(baseProps());
        IoCommand.Statement stmt = session.statement();

        // parseAvroValue(...) short-circuits to null on a null value before it ever inspects the
        // field's schema type, so a plain (non-union) field is enough to exercise that path.
        Schema schema = Schema.createRecord("TestRowWithNull", null, null, false, List.of(
                new Schema.Field("name", Schema.create(Schema.Type.STRING), null, (Object) null)));
        GenericData.Record record = new GenericData.Record(schema);
        record.put("name", null);

        BigQueryIterator fakeIterator = new BigQueryIterator(null, null, null, 0, () -> { }) {
            private boolean served = false;

            @Override
            public boolean hasNext() {
                return !served;
            }

            @Override
            public GenericRecord next() {
                served = true;
                return record;
            }
        };

        Field bigQueryIteratorField = stmt.getClass().getDeclaredField("bigQueryIterator");
        bigQueryIteratorField.setAccessible(true);
        bigQueryIteratorField.set(stmt, fakeIterator);

        Class<?> resultClass = Class.forName(stmt.getClass().getName() + "$BigQueryReadResult");
        Constructor<?> resultCtor = resultClass.getDeclaredConstructor(stmt.getClass());
        resultCtor.setAccessible(true);
        Object result = resultCtor.newInstance(stmt);

        @SuppressWarnings("unchecked")
        Iterator<IoCommand.Row> rowIterator = (Iterator<IoCommand.Row>) resultClass.getMethod("iterator").invoke(result);

        assertTrue(rowIterator.hasNext());
        IoCommand.Row row = rowIterator.next();

        assertNotNull(row);
        assertTrue(row.containsKey("name"));
        assertEquals(null, row.get("name"));
    }
}
