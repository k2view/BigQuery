package com.k2view.cdbms.usercode.common.BigQuery;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.k2view.cdbms.usercode.common.BigQuery.BigQueryIoProvider.Operation;
import com.k2view.fabric.common.io.IoSession;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BigQueryIoProviderTest {

    private final BigQueryIoProvider provider = new BigQueryIoProvider();

    private static Map<String, Object> baseMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD, "DEFAULT");
        return map;
    }

    private static Object getPrivateField(Object target, String name) throws Exception {
        Field f = BigQuerySession.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }

    // --- createSession: Operation branching ---

    @Test
    void createSession_readOperation_returnsBigQueryReadIoSession() {
        Map<String, Object> map = baseMap();
        map.put(BigQueryIoProvider.OPERATION_PARAM_NAME, "READ");

        IoSession session = provider.createSession("func", map);

        assertInstanceOf(BigQueryReadIoSession.class, session);
    }

    @Test
    void createSession_writeOperation_returnsBigQueryWriteIoSession() {
        Map<String, Object> map = baseMap();
        map.put(BigQueryIoProvider.OPERATION_PARAM_NAME, "WRITE");

        IoSession session = provider.createSession("func", map);

        assertInstanceOf(BigQueryWriteIoSession.class, session);
    }

    @Test
    void createSession_commandOperation_returnsBigQueryCommandIoSession() {
        Map<String, Object> map = baseMap();
        map.put(BigQueryIoProvider.OPERATION_PARAM_NAME, "COMMAND");

        IoSession session = provider.createSession("func", map);

        assertInstanceOf(BigQueryCommandIoSession.class, session);
    }

    @Test
    void createSession_operationAbsent_defaultsToCommandAndPopulatesMap() {
        Map<String, Object> map = baseMap();
        assertFalse(map.containsKey(BigQueryIoProvider.OPERATION_PARAM_NAME));

        IoSession session = provider.createSession("func", map);

        assertInstanceOf(BigQueryCommandIoSession.class, session);
        // putIfAbsent side-effect: the raw input map is mutated with the default operation.
        assertEquals(Operation.COMMAND, map.get(BigQueryIoProvider.OPERATION_PARAM_NAME));
    }

    @Test
    void createSession_operationAlreadyPresent_isNotOverwritten() {
        Map<String, Object> map = baseMap();
        map.put(BigQueryIoProvider.OPERATION_PARAM_NAME, "READ");

        provider.createSession("func", map);

        assertEquals("READ", map.get(BigQueryIoProvider.OPERATION_PARAM_NAME));
    }

    @Test
    void createSession_invalidOperationString_throwsIllegalArgumentException() {
        Map<String, Object> map = baseMap();
        map.put(BigQueryIoProvider.OPERATION_PARAM_NAME, "BOGUS");

        // Note: this IllegalArgumentException is thrown by Operation.valueOf(...) itself
        // (an unrecognized enum name), NOT by the provider's own "Unsupported operation"
        // throw at the bottom of createSession. Since `operation` is statically typed as
        // Operation and the enum only has READ/WRITE/COMMAND, every value that survives
        // valueOf() matches one of the three if/else-if branches - so the final
        // `else { throw new IllegalArgumentException("Unsupported operation"); }` branch
        // is unreachable dead code. Both exceptions happen to be the same type, so this
        // is not currently observable by callers, but the custom message is never seen.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> provider.createSession("func", map));
        assertTrue(ex.getMessage().contains("BOGUS"),
                "expected Operation.valueOf's own message, got: " + ex.getMessage());
    }

    // --- createSession: prop extraction / mapping into the session ---

    @Test
    void createSession_mapsRawPropsIntoSessionFields() {
        Map<String, Object> map = baseMap();
        map.put(BigQueryIoProvider.OPERATION_PARAM_NAME, "COMMAND");
        map.put(BigQueryIoProvider.SESSION_PROP_INTERFACE, "itf-1");
        map.put(BigQueryIoProvider.SESSION_PROP_DATASETS_PROJECT, "ds-proj-1");
        map.put(BigQueryIoProvider.SESSION_PROP_USER_PROJECT, "user-proj-1");
        map.put(BigQueryIoProvider.SESSION_PROP_SNAPSHOT_VIA_STORAGE, true);

        BigQuerySession session = (BigQuerySession) provider.createSession("func", map);

        assertEquals("itf-1", session.interfaceName);
        assertEquals("ds-proj-1", session.datasetsProjectId);
        assertEquals("user-proj-1", session.userProjectId);
        assertTrue(session.snapshotViaStorageApi);
    }

    @Test
    void createSession_interfaceAbsent_isNull_butProjectIdsAbsent_areEmptyString() {
        // SESSION_PROP_INTERFACE is extracted via a raw (String) cast (map.get(...)),
        // while the project-id props go through ParamConvertor.toString(...), which
        // maps a null/missing value to "" rather than null. This is an inconsistency
        // in null-handling between sibling properties within the same method - worth
        // confirming it's intentional.
        Map<String, Object> map = baseMap();
        map.put(BigQueryIoProvider.OPERATION_PARAM_NAME, "COMMAND");
        // interface / ProjectId / jobsProjectId intentionally omitted

        BigQuerySession session = (BigQuerySession) provider.createSession("func", map);

        assertNull(session.interfaceName);
        assertEquals("", session.datasetsProjectId);
        assertEquals("", session.userProjectId);
    }

    @Test
    void createSession_snapshotViaStorageApiAbsent_defaultsFalse() {
        Map<String, Object> map = baseMap();
        map.put(BigQueryIoProvider.OPERATION_PARAM_NAME, "COMMAND");

        BigQuerySession session = (BigQuerySession) provider.createSession("func", map);

        assertFalse(session.snapshotViaStorageApi);
    }

    @Test
    void createSession_oAuthPvtKeyPathRawKey_flowsIntoCredentialsFilePath(@TempDir Path tempDir) throws Exception {
        // The raw map lookup key "OAuthPvtKeyPath" (used only here, as a literal string)
        // is asymmetric with the constant it's assigned to internally,
        // SESSION_PROP_CREDENTIALS_FILE = "credentialsFilePath". This test confirms the
        // mapping does work end-to-end today, but the naming asymmetry is undocumented
        // and easy to break (e.g. if the constant's value or the literal ever drift).
        Path credFile = tempDir.resolve("creds.json");
        Files.writeString(credFile, "raw-file-creds-xyz");

        Map<String, Object> map = baseMap();
        map.put(BigQueryIoProvider.OPERATION_PARAM_NAME, "COMMAND");
        map.put(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD, "FILE");
        map.put("OAuthPvtKeyPath", credFile.toString());

        BigQuerySession session = (BigQuerySession) provider.createSession("func", map);

        assertEquals("raw-file-creds-xyz", getPrivateField(session, "credentialsJSON"));
    }

    @Test
    void createSession_fileAuthWithMissingOAuthPvtKeyPath_throwsAtConstructionTime() {
        // ParamConvertor.toString(null) yields "" (not null) for the missing
        // "OAuthPvtKeyPath" key, so BigQuerySession's constructor attempts
        // Files.readString(Path.of("")), which fails (empty path resolves to the
        // current directory, not a file) and is wrapped by Util.rte into a
        // RuntimeException. Confirmed by this test.
        Map<String, Object> map = baseMap();
        map.put(BigQueryIoProvider.OPERATION_PARAM_NAME, "COMMAND");
        map.put(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD, "FILE");
        // OAuthPvtKeyPath intentionally omitted

        assertThrows(RuntimeException.class, () -> provider.createSession("func", map));
    }

    // --- unwrap ---

    @Test
    void unwrap_returnsSameProviderInstance() {
        assertSame(provider, provider.unwrap(BigQueryIoProvider.class));
    }
}
