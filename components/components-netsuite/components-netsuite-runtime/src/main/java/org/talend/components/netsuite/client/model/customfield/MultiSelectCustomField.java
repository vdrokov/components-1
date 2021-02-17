// ============================================================================
//
// Copyright (C) 2006-2021 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.netsuite.client.model.customfield;

import java.util.List;

public class MultiSelectCustomField {

    private List<ListOrRecord> value;

    public List<ListOrRecord> getValue() {
        return value;
    }

    public void setValue(List<ListOrRecord> value) {
        this.value = value;
    }
}
