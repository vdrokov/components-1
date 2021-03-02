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
package org.talend.components.jdbc.tjdbcoutputbulkexec;

import java.util.EnumSet;
import java.util.Set;

import org.talend.components.api.component.AbstractComponentDefinition;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.VirtualComponentDefinition;
import org.talend.components.api.component.runtime.ExecutionEngine;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.jdbc.tjdbcbulkexec.TJDBCBulkExecDefinition;
import org.talend.components.jdbc.tjdbcoutputbulk.TJDBCOutputBulkDefinition;
import org.talend.components.jdbc.wizard.JDBCConnectionWizardProperties;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.runtime.RuntimeInfo;

public class TJDBCOutputBulkExecDefinition extends AbstractComponentDefinition implements VirtualComponentDefinition {

    public static final String COMPONENT_NAME = "tJDBCOutputBulkExec";

    public TJDBCOutputBulkExecDefinition() {
        super(COMPONENT_NAME, ExecutionEngine.DI);
    }
    
    @Override
    public String[] getFamilies() {
        return new String[] { "Databases/DB Specifics/JDBC" };
    }

    @Override
    public boolean isStartable() {
        return false;
    }

    @Override
    public String getPartitioning() {
        return NONE;
    }
    
    @Override
    public boolean isSchemaAutoPropagate() {
        return true;
    }

	@Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return TJDBCOutputBulkExecProperties.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<? extends ComponentProperties>[] getNestedCompatibleComponentPropertiesClass() {
    	return new Class[] { JDBCConnectionWizardProperties.class };
    }

    @Override
    public Property[] getReturnProperties() {
        return new Property[] { RETURN_ERROR_MESSAGE_PROP };
    }

    @Override
    public ComponentDefinition getInputComponentDefinition() {
        return new TJDBCOutputBulkDefinition();
    }

    @Override
    public ComponentDefinition getOutputComponentDefinition() {
        return new TJDBCBulkExecDefinition();
    }

    @Override
    public RuntimeInfo getRuntimeInfo(ExecutionEngine engine, ComponentProperties properties, ConnectorTopology connectorTopology) {
        assertEngineCompatibility(engine);
        return null;// this is a very specific component that delegates the runtime to the output and input components
    }

    @Override
    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.INCOMING);
    }

}
