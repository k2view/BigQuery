package com.k2view.cdbms.usercode.common.BigQuery;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Notes on what is intentionally NOT covered here (network/environment-bound production
 * surface, per the assignment's instructions):
 * - credentials() for AuthMethod.DEFAULT calls GoogleCredentials.getApplicationDefault(),
 *   which inspects environment/well-known-file/metadata-server state - integration-only.
 * - buildClient() calls BigQueryOptions.newBuilder()...build().getService(), which reaches
 *   the network - integration-only. Tests below avoid ever invoking it, either by
 *   pre-seeding the private `bqClient` field via reflection, or by pre-seeding the static
 *   CLIENT_CACHE with a mock keyed to match what clientKey() would compute.
 */
class BigQuerySessionTest {

    private static Map<String, Object> propsFor(String authMethod, String userProjectId) {
        Map<String, Object> props = new HashMap<>();
        props.put(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD, authMethod);
        props.put(BigQueryIoProvider.SESSION_PROP_USER_PROJECT, userProjectId);
        return props;
    }

    private static Object getPrivateField(BigQuerySession session, String name) throws Exception {
        Field f = BigQuerySession.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(session);
    }

    private static void setPrivateField(BigQuerySession session, String name, Object value) throws Exception {
        Field f = BigQuerySession.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(session, value);
    }

    private static Class<?> authMethodClass() throws Exception {
        return Class.forName("com.k2view.cdbms.usercode.common.BigQuery.BigQuerySession$AuthMethod");
    }

    private static Object authMethodConstant(String name) throws Exception {
        for (Object constant : authMethodClass().getEnumConstants()) {
            if (constant.toString().equals(name)) {
                return constant;
            }
        }
        throw new IllegalStateException("No AuthMethod constant named " + name);
    }

    private static Object newClientKey(String authMethodName, String userProjectId, String credentialsId) throws Exception {
        Class<?> clientKeyClass = Class.forName("com.k2view.cdbms.usercode.common.BigQuery.BigQuerySession$ClientKey");
        Constructor<?> ctor = clientKeyClass.getDeclaredConstructor(authMethodClass(), String.class, String.class);
        ctor.setAccessible(true);
        return ctor.newInstance(authMethodConstant(authMethodName), userProjectId, credentialsId);
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<Object, BigQuery> clientCache() throws Exception {
        Field f = BigQuerySession.class.getDeclaredField("CLIENT_CACHE");
        f.setAccessible(true);
        return (ConcurrentHashMap<Object, BigQuery>) f.get(null);
    }

    // --- constructor: authMethod parsing ---

    @Test
    void constructor_authMethodLowercase_isNormalizedCaseInsensitively() throws Exception {
        BigQuerySession session = new BigQuerySession(propsFor("default", "proj-lower"));

        assertEquals(authMethodConstant("DEFAULT"), getPrivateField(session, "authMethod"));
    }

    @Test
    void constructor_authMethodMixedCase_isNormalized() throws Exception {
        BigQuerySession session = new BigQuerySession(propsFor("DeFaUlT", "proj-mixed"));

        assertEquals(authMethodConstant("DEFAULT"), getPrivateField(session, "authMethod"));
    }

    @Test
    void constructor_invalidAuthMethodString_throwsIllegalArgumentException() {
        Map<String, Object> props = propsFor("NOT_A_REAL_METHOD", "proj-x");

        assertThrows(IllegalArgumentException.class, () -> new BigQuerySession(props));
    }

    @Test
    void constructor_missingAuthMethodKey_throwsNullPointerException() {
        // Unlike BigQueryIoProvider (which routes the value through
        // ParamConvertor.toString and would get "" for a missing key), BigQuerySession's
        // constructor does a raw props.get(...).toString() with no null-guard. A caller
        // that builds the props map by hand (bypassing the provider) and omits
        // "authenticationMethod" gets an unhelpful NullPointerException instead of a
        // clear "missing required property" error.
        Map<String, Object> props = new HashMap<>();
        props.put(BigQueryIoProvider.SESSION_PROP_USER_PROJECT, "proj-y");

        assertThrows(NullPointerException.class, () -> new BigQuerySession(props));
    }

    // --- constructor: field assignment from props ---

    @Test
    void constructor_assignsInterfaceAndProjectFieldsDirectlyFromProps() {
        Map<String, Object> props = propsFor("DEFAULT", "proj-fields");
        props.put(BigQueryIoProvider.SESSION_PROP_INTERFACE, "my-interface");
        props.put(BigQueryIoProvider.SESSION_PROP_DATASETS_PROJECT, "my-dataset-project");
        props.put(BigQueryIoProvider.SESSION_PROP_SNAPSHOT_VIA_STORAGE, true);

        BigQuerySession session = new BigQuerySession(props);

        assertEquals("my-interface", session.interfaceName);
        assertEquals("my-dataset-project", session.datasetsProjectId);
        assertEquals("proj-fields", session.userProjectId);
        assertTrue(session.snapshotViaStorageApi);
    }

    @Test
    void constructor_missingOptionalKeys_fieldsAreNullOrFalse() {
        // Direct construction (no ParamConvertor involved) - interfaceName/datasetsProjectId
        // are plain (String) casts of a missing map.get(...), so they come out null here,
        // in contrast to the provider path where ParamConvertor.toString coerces to "".
        Map<String, Object> props = propsFor("DEFAULT", "proj-missing-opts");

        BigQuerySession session = new BigQuerySession(props);

        assertNull(session.interfaceName);
        assertNull(session.datasetsProjectId);
        assertFalse(session.snapshotViaStorageApi);
    }

    // --- constructor: FILE auth eagerly reads credentials file ---

    @Test
    void constructor_fileAuth_readsCredentialsFileContentsEagerly(@TempDir Path tempDir) throws Exception {
        Path credFile = tempDir.resolve("creds.json");
        Files.writeString(credFile, "file-contents-abc");

        Map<String, Object> props = propsFor("FILE", "proj-file-ok");
        props.put(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_FILE, credFile.toString());

        BigQuerySession session = new BigQuerySession(props);

        assertEquals("file-contents-abc", getPrivateField(session, "credentialsJSON"));
    }

    @Test
    void constructor_fileAuth_missingFile_throwsRuntimeExceptionAtConstructionTime() {
        Map<String, Object> props = propsFor("FILE", "proj-file-missing");
        props.put(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_FILE, "/no/such/path/does-not-exist.json");

        assertThrows(RuntimeException.class, () -> new BigQuerySession(props));
    }

    @Test
    void constructor_fileAuth_nullFilePath_throwsAtConstructionTime() {
        // filePath ends up null (props.get returns null, no key present); Path.of(null)
        // throws NullPointerException, which Util.rte re-wraps/re-throws as a
        // RuntimeException.
        Map<String, Object> props = propsFor("FILE", "proj-file-null-path");
        // SESSION_PROP_CREDENTIALS_FILE intentionally omitted

        assertThrows(RuntimeException.class, () -> new BigQuerySession(props));
    }

    @Test
    void constructor_jsonAuth_storesRawJsonStringWithoutReadingAnyFile() throws Exception {
        String json = "{\"type\":\"authorized_user\"}";
        Map<String, Object> props = propsFor("JSON", "proj-json");
        props.put(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_JSON, json);
        // Deliberately do NOT set SESSION_PROP_CREDENTIALS_FILE - if JSON auth ever
        // accidentally tried to read a file, this would NPE/throw and fail the test.

        BigQuerySession session = new BigQuerySession(props);

        assertEquals(json, getPrivateField(session, "credentialsJSON"));
    }

    // --- credentials(): JSON auth path (local parsing only, no network) ---

    @Test
    void credentials_jsonAuth_validAuthorizedUserJson_parsesSuccessfully() throws Exception {
        String json = "{\"type\":\"authorized_user\",\"client_id\":\"test-client-id\","
                + "\"client_secret\":\"test-client-secret\",\"refresh_token\":\"test-refresh-token\"}";
        Map<String, Object> props = propsFor("JSON", "proj-creds-valid");
        props.put(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_JSON, json);
        BigQuerySession session = new BigQuerySession(props);

        GoogleCredentials credentials = session.credentials();

        assertNotNull(credentials);
    }

    @Test
    void credentials_jsonAuth_malformedJson_throwsIOException() {
        Map<String, Object> props = propsFor("JSON", "proj-creds-malformed");
        props.put(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_JSON, "this is not json {{{");
        BigQuerySession session = new BigQuerySession(props);

        assertThrows(IOException.class, session::credentials);
    }

    @Test
    void credentials_jsonAuth_missingTypeField_throwsIOException() {
        Map<String, Object> props = propsFor("JSON", "proj-creds-notype");
        props.put(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_JSON, "{}");
        BigQuerySession session = new BigQuerySession(props);

        assertThrows(IOException.class, session::credentials);
    }

    // --- client(): caching shape, exercised without ever reaching the network ---

    @Test
    void client_instanceFieldAlreadyPopulated_isReturnedDirectlyWithoutTouchingCache() throws Exception {
        BigQuerySession session = new BigQuerySession(propsFor("DEFAULT", "proj-instance-cache"));
        BigQuery preSeeded = mock(BigQuery.class);
        setPrivateField(session, "bqClient", preSeeded);

        BigQuery result = session.client();

        assertSame(preSeeded, result);
    }

    @Test
    void client_staticCacheHit_returnsCachedClientWithoutBuildingANewOne() throws Exception {
        String uniqueProjectId = "proj-static-cache-" + UUID.randomUUID();
        BigQuery cached = mock(BigQuery.class);
        Object key = newClientKey("DEFAULT", uniqueProjectId, "");
        ConcurrentHashMap<Object, BigQuery> cache = clientCache();
        cache.put(key, cached);
        try {
            BigQuerySession session = new BigQuerySession(propsFor("DEFAULT", uniqueProjectId));

            BigQuery result = session.client();

            assertSame(cached, result);
            // and the instance now remembers it too, without needing to consult the
            // static cache again on a second call.
            assertSame(cached, getPrivateField(session, "bqClient"));
        } finally {
            cache.remove(key);
        }
    }

    // --- testConnection(): delegates to client().listDatasets(), stubbed via a pre-seeded client ---

    @Test
    void testConnection_delegatesToClientListDatasets() throws Exception {
        BigQuerySession session = new BigQuerySession(propsFor("DEFAULT", "proj-test-connection-ok"));
        BigQuery mockClient = mock(BigQuery.class);
        setPrivateField(session, "bqClient", mockClient);

        session.testConnection();

        verify(mockClient, times(1)).listDatasets();
    }

    @Test
    void testConnection_whenListDatasetsThrows_propagatesAsRuntimeException() throws Exception {
        BigQuerySession session = new BigQuerySession(propsFor("DEFAULT", "proj-test-connection-fail"));
        BigQuery mockClient = mock(BigQuery.class);
        when(mockClient.listDatasets()).thenThrow(new RuntimeException("boom"));
        setPrivateField(session, "bqClient", mockClient);

        assertThrows(RuntimeException.class, session::testConnection);
    }

    // --- abort() ---

    @Test
    void abort_setsAbortedFlagTrue() throws Exception {
        BigQuerySession session = new BigQuerySession(propsFor("DEFAULT", "proj-abort"));

        assertFalse(session.aborted);
        session.abort();
        assertTrue(session.aborted);
    }
}
