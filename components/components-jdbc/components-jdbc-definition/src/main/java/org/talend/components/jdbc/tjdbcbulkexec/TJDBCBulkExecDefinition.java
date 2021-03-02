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
package org.talend.components.jdbc.tjdbcbulkexec;

import java.util.EnumSet;
import java.util.Set;

import org.talend.components.api.component.AbstractComponentDefinition;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.ExecutionEngine;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.jdbc.JdbcRuntimeInfo;
import org.talend.components.jdbc.wizard.JDBCConnectionWizardProperties;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.runtime.RuntimeInfo;

public class TJDBCBulkExecDefinition extends AbstractComponentDefinition {

    public static final String COMPONENT_NAME = "tJDBCBulkExec";

    public TJDBCBulkExecDefinition() {
    	super(COMPONENT_NAME, ExecutionEngine.DI);
    }
    
    @Override
    public String[] getFamilies() {
        return new String[] { "Databases/DB Specifics/JDBC" };
    }

    @Override
    public String getPartitioning() {
        return NONE;
    }
    
    @Override
    public boolean isStartable() {
        return true;

    }

    @Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return TJDBCBulkExecProperties.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<? extends ComponentProperties>[] getNestedCompatibleComponentPropertiesClass() {
    	return new Class[] { JDBCConnectionWizardProperties.class };
    }

    @Override
    public Property<?>[] getReturnProperties() {
        return new Property[]{RETURN_ERROR_MESSAGE_PROP};
    }

    @Override
    public RuntimeInfo getRuntimeInfo(ExecutionEngine engine, ComponentProperties properties, ConnectorTopology componentType) {
        assertEngineCompatibility(engine);
        if (ConnectorTopology.NONE.equals(componentType)) {
        	return new JdbcRuntimeInfo((TJDBCBulkExecProperties) properties,
                    "org.talend.components.jdbc.runtime.JDBCBulkExecRuntime");
        } else {
            return null;
        }
    }

    @Override
    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.NONE);
    }
}
