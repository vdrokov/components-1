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

package org.talend.components.salesforce.runtime.bulk.v2;

import java.io.IOException;

import org.apache.http.impl.io.EmptyInputStream;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;
import org.talend.components.salesforce.runtime.BulkResult;
import org.talend.components.salesforce.runtime.BulkResultSet;
import org.talend.components.salesforce.runtime.bulk.v2.request.GetQueryJobResultRequest;
import org.talend.components.salesforce.tsalesforceinput.TSalesforceInputProperties;

import static org.junit.Assert.*;

public class SalesforceBulkQueryV2RuntimeTest {

    @Test
    public void testGetResultSetUsesResultRequest() throws IOException {
        String testJobId = "abc";
        BulkV2Connection mockedConnection = Mockito.mock(BulkV2Connection.class);
        JobInfoV2 mockedJob = Mockito.mock(JobInfoV2.class);
        Mockito.when(mockedJob.getId()).thenReturn(testJobId);
        Mockito.when(mockedConnection.getResult(Mockito.any(GetQueryJobResultRequest.class))).thenReturn(EmptyInputStream.INSTANCE);

        TSalesforceInputProperties inputProperties = new TSalesforceInputProperties("inputProps");

        SalesforceBulkQueryV2Runtime runtime = new SalesforceBulkQueryV2Runtime(mockedConnection, inputProperties);
        runtime.job = mockedJob;

        runtime.getResultSet();

        Mockito.verify(mockedConnection, Mockito.times(0)).getResult(testJobId);
    }
}