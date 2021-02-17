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

import org.talend.components.netsuite.client.model.beans.Beans;

public class ListOrRecord {

    private String name;

    private String internalId;

    private String externalId;

    private String typeId;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getInternalId() {
        return internalId;
    }

    public void setInternalId(String internalId) {
        this.internalId = internalId;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public String getTypeId() {
        return typeId;
    }

    public void setTypeId(String typeId) {
        this.typeId = typeId;
    }

    public static ListOrRecord fromObject(Object object) {
        ListOrRecord instance = new ListOrRecord();
        instance.setName((String) Beans.getSimpleProperty(object, "name"));
        instance.setInternalId((String) Beans.getSimpleProperty(object, "internalId"));
        instance.setExternalId((String) Beans.getSimpleProperty(object, "externalId"));
        instance.setTypeId((String) Beans.getSimpleProperty(object, "typeId"));
        return instance;
    }
}
