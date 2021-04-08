// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.salesforce;

import org.talend.components.api.properties.ComponentBasePropertiesImpl;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;

import java.util.EnumSet;

import static org.talend.daikon.properties.presentation.Widget.widget;
import static org.talend.daikon.properties.property.PropertyFactory.newBoolean;
import static org.talend.daikon.properties.property.PropertyFactory.newString;

public class SalesforceSSLProperties extends ComponentBasePropertiesImpl {

    public Property<Boolean> mutualAuth  = newBoolean("mutualAuth"); //$NON-NLS-1$

    public Property<String> keyStorePath = newString("keyStorePath"); //$NON-NLS-1$

    public Property<String> keyStorePwd = newString("keyStorePwd")
            .setFlags(EnumSet.of(Property.Flags.ENCRYPT, Property.Flags.SUPPRESS_LOGGING));

    public SalesforceSSLProperties(String name) {
        super(name);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form form = Form.create(this, Form.MAIN);
        form.addRow(widget(mutualAuth));
        form.addRow(widget(keyStorePath));
        form.addRow(widget(keyStorePath).setWidgetType(Widget.FILE_WIDGET_TYPE));
        form.addRow(widget(keyStorePwd).setWidgetType(Widget.HIDDEN_TEXT_WIDGET_TYPE));

    }

    @Override
    public void refreshLayout(Form form) {
        super.refreshLayout(form);
        if (form.getName().equals(Form.MAIN)) {
            boolean isMutualAuth = mutualAuth.getValue();
            form.getWidget(keyStorePath.getName()).setHidden(!isMutualAuth);
            form.getWidget(keyStorePwd.getName()).setHidden(!isMutualAuth);
        }
    }

    public void afterMutualAuth() {
        refreshLayout(getForm(Form.MAIN));
    }

}
