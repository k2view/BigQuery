package com.k2view.cdbms.usercode.common.BigQuery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.k2view.broadway.metadata.ArrayType;
import com.k2view.broadway.metadata.ObjectType;
import com.k2view.broadway.metadata.Primitive;
import com.k2view.broadway.metadata.Properties;
import com.k2view.broadway.metadata.Type;
import com.k2view.discovery.plugins.complexfield.ComplexFieldPlugin;
import com.k2view.discovery.schema.model.DefinedBy;
import com.k2view.discovery.schema.model.Property;
import com.k2view.discovery.schema.model.impl.ConcreteClassNode;
import com.k2view.discovery.schema.model.impl.ConcreteField;
import com.k2view.discovery.schema.model.impl.PrimitiveClass;

/**
 * Behavioral note on the collaborators used below (verified empirically by compiling/running throwaway
 * probes against the project's jars, since only .class files are available - no source):
 * - {@code com.k2view.broadway.metadata.Properties(Object...)} takes alternating key/Schema pairs; a
 *   {@code Type} value is auto-wrapped into a {@code Primitive(Type)}.
 * - {@code ComplexFieldPlugin.createClassName(String)} is a pure, side-effect-free static string transform:
 *   it title-cases the input and appends the literal suffix "Class" (e.g. "address" -> "AddressClass",
 *   "items" -> "ItemsClass"). No mocking required - it is called directly for expected-value computation
 *   below instead of hardcoding the transformed strings.
 */
class DatasetFieldsBuilderTest {

    private static final Function<String, PrimitiveClass> FAILING_PROVIDER = desc -> {
        throw new AssertionError("definedByProvider should not be invoked when there is no primitive array field");
    };

    private static PrimitiveClass primitiveClassNamed(String name) {
        return new PrimitiveClass() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public double getScore() {
                return 1.0;
            }

            @Override
            public String getOrigin() {
                return "test";
            }

            @Override
            public String getUpdatedBy() {
                return "";
            }

            @Override
            public String getNotes() {
                return "";
            }

            @Override
            public String getClassName() {
                return name;
            }
        };
    }

    private static Property findByName(List<Property> properties, String name) {
        return properties.stream().filter(p -> name.equals(p.getName())).findFirst()
                .orElseThrow(() -> new AssertionError("no property named '" + name + "' among " + properties));
    }

    // --- flat top-level object ---

    @Test
    void fromObjectSchema_flatTopLevelPrimitives_reportsOrdinalsTopLevelAndPlainFieldPath() {
        Properties props = new Properties("id", Type.integer, "name", Type.string, "active", Type.bool);
        ObjectType root = new ObjectType(props, "root");
        ConcreteClassNode classNode = new ConcreteClassNode("Dataset", 1.0, "Crawler", "", "root", "Dataset");

        List<DatasetFieldsBuilder.SchemaPropertyContext> captured = new ArrayList<>();
        DatasetFieldsBuilder.fromObjectSchema(classNode, root, captured::add, FAILING_PROVIDER);

        assertEquals(3, captured.size());

        DatasetFieldsBuilder.SchemaPropertyContext idCtx = captured.get(0);
        assertEquals("id", idCtx.field().getId());
        assertEquals(0, idCtx.ordinalPosition());
        assertTrue(idCtx.isTopLevel());
        assertEquals("id", idCtx.fieldPath());

        DatasetFieldsBuilder.SchemaPropertyContext nameCtx = captured.get(1);
        assertEquals("name", nameCtx.field().getId());
        assertEquals(1, nameCtx.ordinalPosition());
        assertTrue(nameCtx.isTopLevel());
        assertEquals("name", nameCtx.fieldPath());

        DatasetFieldsBuilder.SchemaPropertyContext activeCtx = captured.get(2);
        assertEquals("active", activeCtx.field().getId());
        assertEquals(2, activeCtx.ordinalPosition());
        assertTrue(activeCtx.isTopLevel());
        assertEquals("active", activeCtx.fieldPath());

        assertEquals(3, classNode.getFields().size());
    }

    // --- nested object property ---

    @Test
    void fromObjectSchema_nestedObjectProperty_marksNonTopLevelBuildsDottedPathAndDelimitedClassId() {
        Properties geoProps = new Properties("lat", Type.real);
        ObjectType geoObj = new ObjectType(geoProps, "geo");
        Properties addressProps = new Properties("street", Type.string, "geo", geoObj);
        ObjectType addressObj = new ObjectType(addressProps, "address");
        Properties rootProps = new Properties("id", Type.integer, "address", addressObj);
        ObjectType root = new ObjectType(rootProps, "root");

        ConcreteClassNode classNode = new ConcreteClassNode("Dataset", 1.0, "Crawler", "", "root", "Dataset");
        List<DatasetFieldsBuilder.SchemaPropertyContext> captured = new ArrayList<>();
        DatasetFieldsBuilder.fromObjectSchema(classNode, root, captured::add, FAILING_PROVIDER);

        // 5 properties total: id, address (top-level) + street, geo (nested under address) + lat (nested under geo)
        assertEquals(5, captured.size());

        DatasetFieldsBuilder.SchemaPropertyContext addressCtx = captured.get(1);
        assertEquals("address", addressCtx.field().getId());
        assertTrue(addressCtx.isTopLevel());
        assertEquals("address", addressCtx.fieldPath());

        DatasetFieldsBuilder.SchemaPropertyContext streetCtx = captured.get(2);
        assertEquals("street", streetCtx.field().getId());
        assertFalse(streetCtx.isTopLevel());
        assertEquals(0, streetCtx.ordinalPosition());
        assertEquals("address.street", streetCtx.fieldPath());

        DatasetFieldsBuilder.SchemaPropertyContext geoCtx = captured.get(3);
        assertEquals("geo", geoCtx.field().getId());
        assertFalse(geoCtx.isTopLevel());
        assertEquals(1, geoCtx.ordinalPosition());
        assertEquals("address.geo", geoCtx.fieldPath());

        DatasetFieldsBuilder.SchemaPropertyContext latCtx = captured.get(4);
        assertEquals("lat", latCtx.field().getId());
        assertFalse(latCtx.isTopLevel());
        assertEquals("address.geo.lat", latCtx.fieldPath());

        // Verify the ConcreteClassNode graph: "address" is defined by a top-level class node ("AddressClass",
        // no delimiter prefix since the address field itself is top-level), and within it "geo" is defined by
        // a class node using the INNER_CLASS_DELIMITER (";") to prefix with the parent class id.
        ConcreteField addressField = (ConcreteField) classNode.getFields().stream()
                .map(c -> c.getNode()).filter(f -> f.getId().equals("address")).findFirst().orElseThrow();
        List<DefinedBy<com.k2view.discovery.schema.model.ClassNode>> addressDefinedBy = addressField.getDefinedBy();
        assertEquals(1, addressDefinedBy.size());
        ConcreteClassNode addressClassNode = (ConcreteClassNode) addressDefinedBy.get(0).getNode();
        assertEquals(ComplexFieldPlugin.createClassName("address"), addressClassNode.getId());

        ConcreteField geoField = (ConcreteField) addressClassNode.getFields().stream()
                .map(c -> c.getNode()).filter(f -> f.getId().equals("geo")).findFirst().orElseThrow();
        ConcreteClassNode geoClassNode = (ConcreteClassNode) geoField.getDefinedBy().get(0).getNode();
        assertEquals(addressClassNode.getId() + ";" + ComplexFieldPlugin.createClassName("geo"), geoClassNode.getId());
    }

    // --- array of primitives ---

    @Test
    void fromObjectSchema_arrayOfPrimitives_wrapsProviderTypeInSingleCollection() {
        Primitive itemSchema = new Primitive(Type.string, "myType", null); // description = "myType" (lower-case on purpose)
        ArrayType arrayType = new ArrayType(itemSchema);
        Properties props = new Properties("tags", arrayType);
        ObjectType root = new ObjectType(props, "root");
        ConcreteClassNode classNode = new ConcreteClassNode("Dataset", 1.0, "Crawler", "", "root", "Dataset");

        List<String> providerInvocations = new ArrayList<>();
        Function<String, PrimitiveClass> provider = desc -> {
            providerInvocations.add(desc);
            return primitiveClassNamed(desc);
        };

        DatasetFieldsBuilder.fromObjectSchema(classNode, root, ctx -> {
        }, provider);

        assertEquals(List.of("myType"), providerInvocations);

        ConcreteField tagsField = (ConcreteField) classNode.getFields().get(0).getNode();
        Property definedBy = findByName(tagsField.getProperties(), "definedBy");
        // definedByProvider's result is upper-cased and wrapped exactly once: "Collection(MYTYPE)"
        assertEquals("Collection(MYTYPE)", definedBy.getValue());
    }

    // --- array of objects ---

    @Test
    void fromObjectSchema_arrayOfObjects_createsInnerClassNodeAndRecursesViaProcessObject() {
        Properties itemProps = new Properties("x", Type.integer);
        ObjectType itemObj = new ObjectType(itemProps, "item");
        ArrayType arrayType = new ArrayType(itemObj);
        Properties props = new Properties("items", arrayType);
        ObjectType root = new ObjectType(props, "root");
        ConcreteClassNode classNode = new ConcreteClassNode("Dataset", 1.0, "Crawler", "", "root", "Dataset");

        List<DatasetFieldsBuilder.SchemaPropertyContext> captured = new ArrayList<>();
        // the array-of-objects branch never calls definedByProvider - only the primitive branch does
        DatasetFieldsBuilder.fromObjectSchema(classNode, root, captured::add, FAILING_PROVIDER);

        assertEquals(2, captured.size());
        assertEquals("items", captured.get(0).field().getId());
        assertTrue(captured.get(0).isTopLevel());

        assertEquals("x", captured.get(1).field().getId());
        assertFalse(captured.get(1).isTopLevel());
        assertEquals("items.x", captured.get(1).fieldPath());

        ConcreteField itemsField = (ConcreteField) classNode.getFields().get(0).getNode();
        assertEquals(1, itemsField.getDefinedBy().size());
        ConcreteClassNode innerClassNode = (ConcreteClassNode) itemsField.getDefinedBy().get(0).getNode();
        assertEquals(ComplexFieldPlugin.createClassName("items"), innerClassNode.getId());
        // the inner class node really got the "x" field via the processObject recursion, not the processArray
        // primitive branch
        assertEquals(1, innerClassNode.getFields().size());
        assertEquals("x", innerClassNode.getFields().get(0).getNode().getId());

        Property definedBy = findByName(itemsField.getProperties(), "definedBy");
        assertEquals("Collection(" + innerClassNode.getId() + ")", definedBy.getValue());
    }

    // --- BUG HUNT: nested array-of-arrays (collectionDepth > 1) ---

    /**
     * DatasetFieldsBuilder.processArray (DatasetFieldsBuilder.java:98-129), when it detects that an array's
     * item type is itself an array (line 115: {@code itemsType == Type.array}), recurses at line 116 with
     * {@code itemsSchema.items()} as the new "fieldSchema" argument. But processArray's very first statement
     * (line 107) is {@code Schema itemsSchema = fieldSchema.items();} - i.e. every call already unwraps one
     * level via {@code .items()} on entry. Passing {@code itemsSchema.items()} (already one level unwrapped)
     * instead of {@code itemsSchema} itself means the recursive call unwraps a second, extra level before it
     * has even looked at the array it just found. For an array nested exactly one level inside another array
     * (e.g. array-of-array-of-primitive), this makes the recursive call operate directly on the innermost leaf
     * schema; leaf schemas (Primitive, ObjectType) do not override {@code Schema.items()}, whose default
     * implementation returns null - so the recursive call's own {@code fieldSchema.items()} at line 107
     * evaluates to null, and the very next line ({@code itemsSchema.type()}, line 108) throws a
     * NullPointerException. This reproduces with a real (non-mocked) two-level array-of-array-of-primitive
     * schema below - confirmed by running it standalone against the compiled class.
     *
     * Confidence: high. This is a genuine bug, not a mocking artifact - it means ANY field whose schema is an
     * array nested inside another array (regardless of the innermost leaf being primitive or object) crashes
     * fromObjectSchema with an NPE instead of producing the intended "Collection(Collection(TYPE))"-style
     * doubled wrapping.
     */
    @Test
    void fromObjectSchema_nestedArrayOfArrays_isABug_throwsNullPointerExceptionInsteadOfDoubleWrapping() {
        Primitive innerPrimitive = new Primitive(Type.string, "STRING", null);
        ArrayType innerArray = new ArrayType(innerPrimitive);
        ArrayType outerArray = new ArrayType(innerArray);
        Properties props = new Properties("tags", outerArray);
        ObjectType root = new ObjectType(props, "root");
        ConcreteClassNode classNode = new ConcreteClassNode("Dataset", 1.0, "Crawler", "", "root", "Dataset");

        Function<String, PrimitiveClass> provider = DatasetFieldsBuilderTest::primitiveClassNamed;

        NullPointerException npe = assertThrows(NullPointerException.class,
                () -> DatasetFieldsBuilder.fromObjectSchema(classNode, root, ctx -> {
                }, provider));

        // sanity: it really is the items()/type() chain inside processArray blowing up, not something unrelated
        StackTraceElement top = npe.getStackTrace()[0];
        if (!top.getClassName().equals(DatasetFieldsBuilder.class.getName())
                || !top.getMethodName().equals("processArray")) {
            fail("expected the NPE to originate in DatasetFieldsBuilder.processArray, was: " + npe);
        }
    }
}
