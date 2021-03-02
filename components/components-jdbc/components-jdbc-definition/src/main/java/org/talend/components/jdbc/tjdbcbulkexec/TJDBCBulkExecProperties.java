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

import static org.talend.daikon.properties.presentation.Widget.widget;

import java.util.Collections;
import java.util.Set;

import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.api.properties.ComponentReferenceProperties;
import org.talend.components.jdbc.CommonUtils;
import org.talend.components.jdbc.RuntimeSettingProvider;
import org.talend.components.jdbc.module.BulkModule;
import org.talend.components.jdbc.module.JDBCConnectionModule;
import org.talend.components.jdbc.module.JDBCTableSelectionModule;
import org.talend.components.jdbc.runtime.setting.AllSetting;
import org.talend.components.jdbc.tjdbcconnection.TJDBCConnectionDefinition;
import org.talend.components.jdbc.tjdbcconnection.TJDBCConnectionProperties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;

public class TJDBCBulkExecProperties extends BulkModule implements RuntimeSettingProvider {

    // main
    public ComponentReferenceProperties<TJDBCConnectionProperties> referencedComponent = new ComponentReferenceProperties<>(
            "referencedComponent", TJDBCConnectionDefinition.COMPONENT_NAME);

    public JDBCConnectionModule connection = new JDBCConnectionModule("connection");
    
    public JDBCTableSelectionModule tableSelection = new JDBCTableSelectionModule("tableSelection");
    
    public TJDBCBulkExecProperties(String name) {
        super(name);
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

        Form advancedForm = CommonUtils.addForm(this, Form.ADVANCED);
        advancedForm.addRow(rowSeparator);
        advancedForm.addColumn(fieldSeparator);
        
        advancedForm.addRow(setTextEnclosure);
        advancedForm.addColumn(textEnclosure);
        
        //advancedForm.addRow(setEscapeChar);
        //advancedForm.addColumn(escapeChar);
        
        advancedForm.addRow(setNullValue);
        advancedForm.addColumn(nullValue);
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
    public void setupProperties() {
        super.setupProperties();
        
        tableSelection.setConnection(this);

        connection.setNotRequired();
    }
    
    @Override
    protected Set<PropertyPathConnector> getAllSchemaPropertiesConnectors(boolean isOutputConnection) {
        return Collections.emptySet();
    }

    @Override
    public AllSetting getRuntimeSetting() {
        AllSetting setting = new AllSetting();

        CommonUtils.setReferenceInfoAndConnectionInfo(setting, referencedComponent, connection);

        setting.setTablename(this.tableSelection.tablename.getValue());
        
        setting.setSchema(main.schema.getValue());
        
        setting.bulkFile = this.bulkFilePath.getValue();
        setting.rowSeparator = this.rowSeparator.getValue();
        setting.fieldSeparator = this.fieldSeparator.getValue();
        
        setting.setTextEnclosure = this.setTextEnclosure.getValue();
        setting.textEnclosure = this.textEnclosure.getValue();
        //setting.setEscapeChar = this.setEscapeChar.getValue();
        //setting.escapeChar = this.escapeChar.getValue();
        
        setting.setNullValue = this.setNullValue.getValue();
        setting.nullValue = this.nullValue.getValue();

        return setting;
    }
}
