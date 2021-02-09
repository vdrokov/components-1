//==============================================================================
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
//==============================================================================

package org.talend.components.azurestorage.blob.runtime;

import org.junit.Assert;
import org.junit.Test;
import org.talend.components.azurestorage.blob.tazurestorageget.TAzureStorageGetProperties;
import org.talend.components.azurestorage.tazurestorageconnection.AuthType;
import org.talend.components.azurestorage.tazurestorageconnection.TAzureStorageConnectionProperties;
import org.talend.daikon.properties.ValidationResult;

public class AzureStorageContainerRuntimeTest {

    @Test
    public void testInitializeDoesNotThrowErrorForNumericContainerName() {
        AzureStorageContainerRuntime containerRuntime = new AzureStorageContainerRuntime();
        TAzureStorageGetProperties properties = new TAzureStorageGetProperties("properties");

        TAzureStorageConnectionProperties connectionProperties = new TAzureStorageConnectionProperties("connection");
        connectionProperties.authenticationType.setValue(AuthType.BASIC);
        connectionProperties.accountName.setValue("account");
        connectionProperties.accountKey.setValue("key");

        properties.connection = connectionProperties;
        properties.container.setValue("1234");

        ValidationResult validationResult = containerRuntime.initialize(null, properties);

        Assert.assertEquals(ValidationResult.OK, validationResult);
    }
}