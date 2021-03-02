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
package org.talend.components.jdbc.module;

import static org.talend.daikon.properties.property.PropertyFactory.newProperty;

import java.util.Collections;
import java.util.Set;

import org.talend.components.api.component.ISchemaListener;
import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.common.FixedConnectorsComponentProperties;
import org.talend.components.common.SchemaProperties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

/**
 * common JDBC bulk properties for outputbulk, bulkexec and outputbulkexec all
 *
 */
public class BulkModule extends FixedConnectorsComponentProperties {

    public Property<String> bulkFilePath = newProperty("bulkFilePath").setRequired();
    
    public Property<String> rowSeparator = PropertyFactory.newProperty("rowSeparator").setRequired();
    public Property<String> fieldSeparator = PropertyFactory.newProperty("fieldSeparator").setRequired();
    
    //not sure need to support this, always use "\" as escape is ok, and not easy to control the relationship between bulkexec and outputbulk
    //public Property<Boolean> setEscapeChar = PropertyFactory.newBoolean("setEscapeChar");
    //public Property<String> escapeChar = PropertyFactory.newString("escapeChar");
    
    public Property<Boolean> setTextEnclosure = PropertyFactory.newBoolean("setTextEnclosure");
    public Property<String> textEnclosure = PropertyFactory.newString("textEnclosure");
    
    public Property<Boolean> setNullValue = PropertyFactory.newBoolean("setNullValue");
    public Property<String> nullValue = PropertyFactory.newString("nullValue");
    
    public ISchemaListener schemaListener;
    
    public SchemaProperties main = new SchemaProperties("main") {

        @SuppressWarnings("unused")
        public void afterSchema() {
        
        }

    };
    
    public void setSchemaListener(ISchemaListener schemaListener) {
        this.schemaListener = schemaListener;
    }
    
    public BulkModule(String name) {
        super(name);
    }

    @Override
    public void setupProperties() {
        super.setupProperties();
        
        rowSeparator.setValue("\\n");
        fieldSeparator.setValue(";");
        
        textEnclosure.setValue("\"");
    }
    
    @Override
    public void refreshLayout(Form form) {
        super.refreshLayout(form);
        
        if (form.getName().equals(Form.ADVANCED)) {
            //form.getWidget(escapeChar.getName()).setHidden(!setEscapeChar.getValue());
            form.getWidget(textEnclosure.getName()).setHidden(!setTextEnclosure.getValue());
            form.getWidget(nullValue.getName()).setHidden(!setNullValue.getValue());
        }
    }
    
    public void afterSetTextEnclosure() {
        refreshLayout(getForm(Form.ADVANCED));
    }
    
    public void afterSetNullValue() {
        refreshLayout(getForm(Form.ADVANCED));
    }
    
    /*
    public void afterSetEscapeChar() {
        refreshLayout(getForm(Form.ADVANCED));
    }
    */

    @Override
    protected Set<PropertyPathConnector> getAllSchemaPropertiesConnectors(boolean isOutputConnection) {
        return Collections.emptySet();
    }

}
