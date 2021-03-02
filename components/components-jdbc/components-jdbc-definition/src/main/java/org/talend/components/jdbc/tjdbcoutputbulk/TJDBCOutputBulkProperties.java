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
package org.talend.components.jdbc.tjdbcoutputbulk;

import static org.talend.daikon.properties.presentation.Widget.widget;
import static org.talend.daikon.properties.property.PropertyFactory.newBoolean;

import java.util.Collections;
import java.util.Set;

import org.talend.components.api.component.Connector;
import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.jdbc.CommonUtils;
import org.talend.components.jdbc.RuntimeSettingProvider;
import org.talend.components.jdbc.module.BulkModule;
import org.talend.components.jdbc.runtime.setting.AllSetting;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;

public class TJDBCOutputBulkProperties extends BulkModule implements RuntimeSettingProvider {

    //public Property<Boolean> includeHeader = newBoolean("includeHeader");
    public Property<Boolean> append = newBoolean("append");
    
    public TJDBCOutputBulkProperties(String name) {
        super(name);
    }

    @Override
    public void setupProperties() {
        super.setupProperties();
    }

    @Override
    public void setupLayout() {
        super.setupLayout();
        
        Form mainForm = CommonUtils.addForm(this, Form.MAIN);
        mainForm.addRow(main.getForm(Form.REFERENCE));
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
    public AllSetting getRuntimeSetting() {
        AllSetting setting = new AllSetting();
        setting.setSchema(main.schema.getValue());
        setting.bulkFile = this.bulkFilePath.getValue();
        setting.append = this.append.getValue();
        setting.rowSeparator = this.rowSeparator.getValue();
        setting.fieldSeparator = this.fieldSeparator.getValue();
        
        setting.setTextEnclosure = this.setTextEnclosure.getValue();
        setting.textEnclosure = this.textEnclosure.getValue();
        //setting.setEscapeChar = this.setEscapeChar.getValue();
        //setting.escapeChar = this.escapeChar.getValue();
        
        setting.setNullValue = this.setNullValue.getValue();
        setting.nullValue = this.nullValue.getValue();
        //setting.includeHeader = this.includeHeader.getValue();
        return setting;
    }
    
    @Override
    protected Set<PropertyPathConnector> getAllSchemaPropertiesConnectors(boolean isOutputConnection) {
        return Collections.singleton(new PropertyPathConnector(Connector.MAIN_NAME, "main"));
    }

}
