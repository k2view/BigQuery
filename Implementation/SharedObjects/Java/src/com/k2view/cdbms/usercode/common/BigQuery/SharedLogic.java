/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.BigQuery;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;

import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.FieldValue;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.fabric.common.io.IoProvider;
import com.k2view.fabric.fabricdb.datachange.TableDataChange;

import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.usercode.common.BigQuery.BigQueryParamParser.parseBqValue;

@SuppressWarnings({"all"})
public class SharedLogic {
	@type(CustomIoProvider)
	@out(name = "ioProvider", type = IoProvider.class, desc = "")
	public static IoProvider bigQueryIoProvider() throws Exception {
		return new BigQueryIoProvider();
	}

    public static Object bigQueryParseTdmQueryParam(String fieldName, String value, String type) {
        // Assuming value is primitive for the sake of select queries built by TDM
        Field field = Field.of(fieldName, StandardSQLTypeName.valueOf(type));
        FieldValue fieldValue = FieldValue.of(Attribute.PRIMITIVE, value);
        return parseBqValue(field, fieldValue, false);
    }
}
