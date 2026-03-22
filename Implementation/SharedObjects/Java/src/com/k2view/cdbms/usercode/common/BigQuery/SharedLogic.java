/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.BigQuery;

import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.CustomIoProvider;
import static com.k2view.cdbms.usercode.common.BigQuery.BigQueryParamParser.parseBqValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.k2view.cdbms.interfaces.GenericInterface;
import com.k2view.cdbms.lut.InterfacesManager;
import com.k2view.cdbms.lut.LUType;
import com.k2view.cdbms.lut.LudbColumn;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.out;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.type;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.io.IoProvider;

@SuppressWarnings({ "all" })
public class SharedLogic {
    @type(CustomIoProvider)
    @out(name = "ioProvider", type = IoProvider.class, desc = "")
    public static IoProvider bigQueryIoProvider() throws Exception {
        return new BigQueryIoProvider();
    }

    public static Object bqParseTdmQueryParam(String fieldName, String value, String type) {
        // Assuming value is primitive for the sake of select queries built by TDM
        Field field = Field.of(fieldName, mapToStandardSQLType(type));
        FieldValue fieldValue = FieldValue.of(Attribute.PRIMITIVE, value);
        return parseBqValue(field, fieldValue, false);
    }

    public static String bqReplaceFilterPlaceholders(String filter, Iterable<Object> values) {
        if (filter == null || values == null) {
            throw new IllegalArgumentException("Filter and values cannot be null");
        }

        Iterator<Object> iterator = values.iterator();
        StringBuilder result = new StringBuilder();
        Matcher matcher = Pattern.compile("\\?").matcher(filter);

        while (matcher.find()) {
            if (!iterator.hasNext()) {
                throw new IllegalArgumentException("Not enough values provided for placeholders");
            }

            Object value = iterator.next();
            String replacement;

            if (value == null) {
                replacement = "NULL";
            } else if (value instanceof String) {
                replacement = "'" + value.toString().replace("'", "''") + "'"; // Escape single quotes
            } else {
                replacement = value.toString();
            }
            matcher.appendReplacement(result, replacement);
        }

        matcher.appendTail(result);

        if (iterator.hasNext()) {
            throw new IllegalArgumentException("Too many values provided for placeholders");
        }

        return result.toString();
    }

    public static Iterable<Map<String, Object>> bqParentRowsMapper(
            String lu,
            String table,
            Iterable<Map<String, Object>> parentRows) {

        if (parentRows == null) {
            return Collections.emptyList();
        }

        LUType luType = LUType.getTypeByName(lu);
        LudbColumn ludbEntityIDColumnObject = luType.ludbEntityIDColumnObject;
        String iidColName = ludbEntityIDColumnObject.originColumnName;
        String colType = ludbEntityIDColumnObject.originalColumnDataType.toUpperCase();

        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> originalRow : parentRows) {
            Map<String, Object> row = new LinkedHashMap<>(originalRow);

            row.computeIfPresent(iidColName, (k, v) -> switch (colType) {
                case "INTEGER" -> ParamConvertor.toInteger(v);
                case "REAL" -> ParamConvertor.toReal(v);
                case "DATETIME", "DATE", "TIME" -> ParamConvertor.toDate(v);
                case "BLOB" -> ParamConvertor.toBuffer(v);
                case "TEXT" -> ParamConvertor.toString(v);
                default -> v;
            });
            
            result.add(row);
        }

        return result;
    }

    public static String bqGetDatasetsProject(String interfaceName, String env) {
        return ((GenericInterface) InterfacesManager.getInstance().getTypedInterface(interfaceName, env))
                .getProperty(BigQueryIoProvider.SESSION_PROP_DATASETS_PROJECT);
    }

    public static String bqQueryBuilderAddLimit(String sql, int limit) {
        if (sql == null) return null;
        String text = sql.trim();
        if (text.isEmpty()) return sql;

        List<String> statements = splitStatementsSafely(text);
        if (statements.isEmpty())
            return sql;

        int last = statements.size() - 1;
        String lastStmt = statements.get(last).trim();

        if (!lastStmt.toLowerCase().startsWith("select"))
            return sql;

        boolean hadSemicolon = lastStmt.endsWith(";");
        if (hadSemicolon)
            lastStmt = lastStmt.substring(0, lastStmt.length() - 1).trim();

        if (hasLimit(lastStmt))
            return sql;

        lastStmt = lastStmt + " LIMIT " + limit;

        if (hadSemicolon)
            lastStmt += ";";

        statements.set(last, lastStmt);

        return joinStatements(statements);
    }

    /**
     * Converts INFORMATION_SCHEMA `data_type` into StandardSQLTypeName.
     */
    private static StandardSQLTypeName mapToStandardSQLType(String type) {
        // Remove precision/length specifiers, e.g., "STRING(10)" → "STRING"
        String baseType = type.replaceAll("\\(.*\\)", "").toUpperCase();

        // Handle special case: ARRAY<TYPE>
        if (baseType.startsWith("ARRAY<")) {
            return StandardSQLTypeName.ARRAY;
        }

        return switch (baseType) {
            case "STRING" -> StandardSQLTypeName.STRING;
            case "INT64", "INTEGER" -> StandardSQLTypeName.INT64;
            case "FLOAT64", "FLOAT" -> StandardSQLTypeName.FLOAT64;
            case "BOOLEAN", "BOOL" -> StandardSQLTypeName.BOOL;
            case "DATETIME" -> StandardSQLTypeName.DATETIME;
            case "TIMESTAMP" -> StandardSQLTypeName.TIMESTAMP;
            case "DATE" -> StandardSQLTypeName.DATE;
            case "NUMERIC" -> StandardSQLTypeName.NUMERIC;
            case "BIGNUMERIC" -> StandardSQLTypeName.BIGNUMERIC;
            case "BYTES" -> StandardSQLTypeName.BYTES;
            default -> throw new IllegalArgumentException("Unsupported BigQuery type: " + type);
        };
    }

    private static List<String> splitStatementsSafely(String sql) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        boolean inSingle = false;
        boolean inDouble = false;
        boolean inBacktick = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char n = (i + 1 < sql.length()) ? sql.charAt(i + 1) : '\0';

            // Handle exiting comments first
            if (inLineComment) {
                if (c == '\n') inLineComment = false;
                current.append(c);
                continue;
            }

            if (inBlockComment) {
                if (c == '*' && n == '/') {
                    inBlockComment = false;
                    current.append("*/");
                    i++;
                } else {
                    current.append(c);
                }
                continue;
            }

            // Entering comments
            if (!inSingle && !inDouble && !inBacktick) {
                if (c == '-' && n == '-') {
                    inLineComment = true;
                    current.append("--");
                    i++;
                    continue;
                }
                if (c == '/' && n == '*') {
                    inBlockComment = true;
                    current.append("/*");
                    i++;
                    continue;
                }
            }

            // String toggles
            if (c == '\'' && !inDouble && !inBacktick) inSingle = !inSingle;
            if (c == '"' && !inSingle && !inBacktick) inDouble = !inDouble;
            if (c == '`' && !inSingle && !inDouble) inBacktick = !inBacktick;

            // Real statement boundary?
            if (c == ';' && !inSingle && !inDouble && !inBacktick) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0)
            result.add(current.toString());

        return result.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }


    private static boolean hasLimit(String stmt) {
        // Normalize whitespace, ignore case, avoid false positives
        String lower = stmt.toLowerCase();

        // If LIMIT exists anywhere outside string literals (approx safe check)
        return lower.matches("(?s).*\\blimit\\s+\\d+.*");
    }


    private static String joinStatements(List<String> parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            sb.append(parts.get(i).trim());
            if (!parts.get(i).trim().endsWith(";"))
                sb.append(";");
            if (i < parts.size() - 1)
                sb.append(" ");
        }
        return sb.toString().trim();
    }

}
