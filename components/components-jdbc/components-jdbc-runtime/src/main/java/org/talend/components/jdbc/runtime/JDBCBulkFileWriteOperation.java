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
package org.talend.components.jdbc.runtime;

import org.talend.components.api.component.runtime.Result;
import org.talend.components.api.component.runtime.Sink;
import org.talend.components.api.component.runtime.Writer;
import org.talend.components.api.container.RuntimeContainer;

public class JDBCBulkFileWriteOperation extends DefaultWriteOperation {

    private static final long serialVersionUID = 1L;

    public JDBCBulkFileWriteOperation(Sink sink) {
        super(sink);
    }

    @Override
    public Writer<Result> createWriter(RuntimeContainer adaptor) {
        return new JDBCBulkFileWriter(this, ((JDBCBulkFileRuntime)getSink()).properties, adaptor);
    }

}
