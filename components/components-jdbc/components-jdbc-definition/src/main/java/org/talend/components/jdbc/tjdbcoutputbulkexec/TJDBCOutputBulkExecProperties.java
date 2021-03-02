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

import static org.talend.daikon.properties.presentation.Widget.widget;
import static org.talend.daikon.properties.property.PropertyFactory.newBoolean;

import java.util.Collections;
import java.util.Set;

import org.talend.components.api.component.Connector;
import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.properties.ComponentReferenceProperties;
import org.talend.components.api.properties.VirtualComponentProperties;
import org.talend.components.jdbc.CommonUtils;
import org.talend.components.jdbc.RuntimeSettingProvider;
import org.talend.components.jdbc.module.BulkModule;
import org.talend.components.jdbc.module.JDBCConnectionModule;
import org.talend.components.jdbc.module.JDBCTableSelectionModule;
import org.talend.components.jdbc.runtime.setting.AllSetting;
import org.talend.components.jdbc.tjdbcbulkexec.TJDBCBulkExecProperties;
import org.talend.components.jdbc.tjdbcconnection.TJDBCConnectionDefinition;
import org.talend.components.jdbc.tjdbcconnection.TJDBCConnectionProperties;
import org.talend.components.jdbc.tjdbcoutputbulk.TJDBCOutputBulkProperties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;

public class TJDBCOutputBulkExecProperties extends BulkModule implements VirtualComponentProperties, RuntimeSettingProvider {

    // main
    public ComponentReferenceProperties<TJDBCConnectionProperties> referencedComponent = new ComponentReferenceProperties<>(
            "referencedComponent", TJDBCConnectionDefinition.COMPONENT_NAME);

    public JDBCConnectionModule connection = new JDBCConnectionModule("connection");
    
    public JDBCTableSelectionModule tableSelection = new JDBCTableSelectionModule("tableSelection");

    //public Property<Boolean> includeHeader = newBoolean("includeHeader");
    public Property<Boolean> append = newBoolean("append");
    
    public TJDBCOutputBulkExecProperties(String name) {
        super(name);
    }

    @Override
    public void setupProperties() {
        super.setupProperties();
        
        tableSelection.setConnection(this);

        connection.setNotRequired();
    }

    @Override
    public void setupLayout() {
        super.setupLayout();
        
        Form mainForm = CommonUtils.addForm(this, Form.MAIN);
        
        Widget compListWidget = widget(referencedComponent).setWidgetType(Widget.COMPONENT_REFERENCE_WIDGET_TYPE);
        mainForm.addRow(compListWidget);

        mainForm.addRow(connection.getForm(Form.MAIN));
        mainForm.addRow(main.getForm(Form.REFERENCE));

        mainForm.addRow(tableSelection.getForm(Form.REFERENCE));
        
        mainForm.addRow(widget(bulkFilePath).setWidgetType(Widget.FILE_WIDGET_TYPE));
        
        mainForm.addRow(append);

        Form advancedForm = CommonUtils.addForm(this, Form.ADVANCED);
        advancedForm.addRow(rowSeparator);
        advancedForm.addColumn(fieldSeparator);
        
        advancedForm.addRow(setTextEnclosure);
        advancedForm.addColumn(textEnclosure);
        
        //advancedForm.addRow(setEscapeChar);
        //advancedForm.addColumn(escapeChar);
        
        advancedForm.addRow(setNullValue);
        advancedForm.addColumn(nullValue);
        
        //advancedForm.addRow(includeHeader);
    }

    @Override
    public void refreshLayout(Form form) {
        super.refreshLayout(form);

        boolean useOtherConnection = CommonUtils.useExistedConnection(referencedComponent);

        if (form.getName().equals(Form.MAIN)) {
            form.getChildForm(connection.getName()).setHidden(useOtherConnection);
        }
    }
    
    public void afterReferencedComponent() {
        refreshLayout(getForm(Form.MAIN));
    }

    @Override
    public ComponentProperties getInputComponentProperties() {
        TJDBCOutputBulkProperties outputBulkProperties =
                new TJDBCOutputBulkProperties("outputBulkProperties");
        
        outputBulkProperties.init();
        outputBulkProperties.copyValuesFrom(this, true, true);

        outputBulkProperties.main.schema.setStoredValue(main.schema.getStoredValue());
        outputBulkProperties.main.schema.setValueEvaluator(main.schema.getValueEvaluator());

        // we need to pass also the possible values, only way from the studio to know it comes from a combo box (need to
        // add quotes for generation)
        for (Form form : outputBulkProperties.getForms()) {
            outputBulkProperties.refreshLayout(form);
        }
        return outputBulkProperties;
    }

    @Override
    public ComponentProperties getOutputComponentProperties() {
        TJDBCBulkExecProperties bulkExecProperties = new TJDBCBulkExecProperties("bulkExecProperties");

        bulkExecProperties.init();
        bulkExecProperties.copyValuesFrom(this, true, true);
        
        //not sure the copy above can copy the schema, so do this
        bulkExecProperties.main.schema.setStoredValue(main.schema.getStoredValue());
        bulkExecProperties.main.schema.setValueEvaluator(main.schema.getValueEvaluator());

        // Seems that properties copy can't copy the reference properties
        String refComponentIdValue = this.referencedComponent.componentInstanceId.getStringValue();
        boolean isUseExistConnection = refComponentIdValue != null
                && refComponentIdValue.startsWith(TJDBCConnectionDefinition.COMPONENT_NAME);
        if (isUseExistConnection) {
            bulkExecProperties.referencedComponent
                    .setReference(this.referencedComponent.getReference());
        }

        for (Form form : bulkExecProperties.getForms()) {
            bulkExecProperties.refreshLayout(form);
        }
        return bulkExecProperties;
    }
    
    //only works for ui trigger runtime
    @Override
    public AllSetting getRuntimeSetting() {
        AllSetting setting = new AllSetting();

        CommonUtils.setReferenceInfoAndConnectionInfo(setting, referencedComponent, connection);
        
        return setting;
    }

    @Override
    protected Set<PropertyPathConnector> getAllSchemaPropertiesConnectors(boolean isOutputConnection) {
        return Collections.singleton(new PropertyPathConnector(Connector.MAIN_NAME, "main"));
    }
}
