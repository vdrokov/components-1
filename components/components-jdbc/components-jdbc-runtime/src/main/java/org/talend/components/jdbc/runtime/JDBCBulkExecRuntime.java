// ============================================================================
//
// Copyright (C) 2006-2020 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jdbc.runtime;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.jdbc.CommonUtils;
import org.talend.components.jdbc.RuntimeSettingProvider;
import org.talend.components.jdbc.runtime.setting.AllSetting;
import org.talend.components.jdbc.runtime.setting.JdbcRuntimeSourceOrSinkDefault;
import org.talend.daikon.avro.SchemaConstants;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.ValidationResult.Result;
import org.talend.daikon.properties.ValidationResultMutable;

/**
 * JDBC bulk exec runtime execution object
 *
 */
public class JDBCBulkExecRuntime extends JdbcRuntimeSourceOrSinkDefault {
	private static final Logger LOG = LoggerFactory.getLogger(JDBCBulkExecRuntime.class);

	private static final long serialVersionUID = 1L;

	public RuntimeSettingProvider properties;

	protected AllSetting setting;

	private boolean useExistedConnection;

	@Override
	public ValidationResult initialize(RuntimeContainer runtime, ComponentProperties properties) {
		LOG.debug("Parameters: [{}]", getLogString(properties));
		this.properties = (RuntimeSettingProvider) properties;
		setting = this.properties.getRuntimeSetting();
		useExistedConnection = setting.getReferencedComponentId() != null;
		return ValidationResult.OK;
	}

	private String createBulkSQL() {
		StringBuilder sb = new StringBuilder();

		sb.append("LOAD DATA LOCAL INFILE '").append(setting.bulkFile).append("' INTO TABLE ")
				.append(setting.getTablename()).append(" FIELDS TERMINATED BY '").append(setting.fieldSeparator).append("' ");
		if(setting.setTextEnclosure) {
			sb.append("OPTIONALLY ENCLOSED BY '").append(setting.textEnclosure).append("' ");
		}
		sb.append("LINES TERMINATED BY '").append(setting.rowSeparator).append("' ");
		if(setting.setNullValue) {
			sb.append("NULL DEFINED BY '").append(setting.nullValue).append("' ");
		}

		if (setting.getSchema() == null) {
			return sb.toString();
		}

		List<Field> fields = setting.getSchema().getFields();

		if (fields == null || fields.isEmpty()) {
			return sb.toString();
		}

		//TODO support dynamic
		sb.append('(');
		for (int i = 0; i < fields.size(); i++) {
			Schema.Field field = fields.get(i);
			String originName = field.getProp(SchemaConstants.TALEND_COLUMN_DB_COLUMN_NAME);
			String headerName = StringUtils.isEmpty(originName) ? field.name() : originName;
			sb.append('`').append(headerName).append('`');
			if (i != fields.size() - 1) {
				sb.append(',');
			}
		}
		sb.append(')');
		return sb.toString();
	}

	@Override
	public ValidationResult validate(RuntimeContainer runtime) {
		ValidationResultMutable vr = new ValidationResultMutable();

		AllSetting setting = properties.getRuntimeSetting();

		Connection conn = null;
		try {
			LOG.debug("Connection attempt to '{}' with the username '{}'", setting.getJdbcUrl(), setting.getUsername());
			conn = connect(runtime);
		} catch (ClassNotFoundException | SQLException e) {
			throw CommonUtils.newComponentException(e);
		}

		try {
			try (Statement stmt = conn.createStatement()) {
				String bulkSql = createBulkSQL();
				LOG.debug("Executing the query: '{}'", bulkSql);
				stmt.execute(bulkSql);
			}
		} catch (Exception ex) {
			vr.setStatus(Result.ERROR);
			vr.setMessage(CommonUtils.correctExceptionInfo(ex));
		} finally {
			if (!useExistedConnection) {
				try {
					LOG.debug("Closing connection");
					conn.close();
				} catch (SQLException e) {
					throw CommonUtils.newComponentException(e);
				}
			}
		}
		return vr;
	}

	protected Connection connect(RuntimeContainer runtime) throws ClassNotFoundException, SQLException {
		// using another component's connection
		if (useExistedConnection) {
			LOG.debug("Uses an existing connection");
			return JdbcRuntimeUtils.fetchConnectionFromContextOrCreateNew(setting, runtime);
		} else {
			Connection conn = JdbcRuntimeUtils.createConnectionOrGetFromSharedConnectionPoolOrDataSource(runtime,
					properties.getRuntimeSetting(), false);
			return conn;
		}
	}

}
