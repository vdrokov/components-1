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

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.api.component.runtime.Result;
import org.talend.components.api.component.runtime.WriteOperation;
import org.talend.components.api.component.runtime.Writer;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.jdbc.CommonUtils;
import org.talend.components.jdbc.RuntimeSettingProvider;
import org.talend.components.jdbc.runtime.setting.AllSetting;
import org.talend.components.jdbc.runtime.type.BulkFormatter;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;

import com.csvreader.CsvWriter;

/**
 * Generate bulk file
 */
public class JDBCBulkFileWriter implements Writer<Result> {

    protected RuntimeContainer container;

    private WriteOperation<Result> writeOperation;

    private Result result;

    protected RuntimeSettingProvider bulkProperties;
    
    private AllSetting setting;

    private CsvWriter csvWriter;

    private String charset = "UTF-8";

    private boolean isAppend;
    
    private boolean includeHeader;
    
    private String nullValue; 
    
    private Schema designSchema;
    private Schema currentSchema;
    
    private boolean isDynamic;
    
    private BulkFormatter bulkFormatter;
    
    public JDBCBulkFileWriter(WriteOperation<Result> writeOperation, RuntimeSettingProvider bulkProperties, RuntimeContainer container) {
        this.writeOperation = writeOperation;
        this.container = container;
        this.bulkProperties = bulkProperties;
        this.setting = bulkProperties.getRuntimeSetting();
        this.isAppend = setting.append;
        this.includeHeader = setting.includeHeader;
        if(setting.setNullValue) {
            this.nullValue = setting.nullValue;
        }
        this.designSchema = setting.getSchema();
        isDynamic = AvroUtils.isIncludeAllFields(this.designSchema);
    }

    @Override
    public void open(String uId) throws IOException {
        this.result = new Result(uId);
        String filepath = setting.bulkFile;
        if (filepath == null || filepath.isEmpty()) {
            throw new RuntimeException("Please set a valid value for \"Bulk File Path\" field.");
        }
        File file = new File(setting.bulkFile);
        file.getParentFile().mkdirs();
        if(setting.rowSeparator.length()>1) {
            throw new RuntimeException("only support one char row separator");
        }
        if(setting.fieldSeparator.length()>1) {
            throw new RuntimeException("only support one char field separator");
        }
        csvWriter = new CsvWriter(new OutputStreamWriter(new java.io.FileOutputStream(file, isAppend), charset), setting.fieldSeparator.charAt(0));
        csvWriter.setRecordDelimiter(setting.rowSeparator.charAt(0));
        if(setting.setTextEnclosure) {
            if(setting.textEnclosure.length()>1) {
                throw new RuntimeException("only support one char text enclosure");
            }
            csvWriter.setUseTextQualifier(true);
            //not let it to do the "smart" thing, avoid to promise too much for changing api in future
            csvWriter.setForceQualifier(true);
            csvWriter.setTextQualifier(setting.textEnclosure.charAt(0));
        } else {
            csvWriter.setUseTextQualifier(false);
            csvWriter.setForceQualifier(false);
        }
        csvWriter.setEscapeMode(com.csvreader.CsvWriter.ESCAPE_MODE_BACKSLASH);

        fileIsEmpty = (file.length() == 0);
    }

    private boolean headerIsReady = false;

    private boolean fileIsEmpty = false;

    @Override
    public void write(Object datum) throws IOException {
        if (null == datum) {
            return;
        }
        
        IndexedRecord record = (IndexedRecord)datum;
        
        if(currentSchema==null) {
            currentSchema = this.designSchema;
            Schema inputSchema = record.getSchema();
            if (isDynamic) {
                currentSchema = CommonUtils.mergeRuntimeSchema2DesignSchema4Dynamic(this.designSchema, inputSchema);
            }
            
            bulkFormatter = new BulkFormatter(inputSchema, currentSchema, setting.setTextEnclosure);
        }

        if (includeHeader && !headerIsReady && (!isAppend || fileIsEmpty)) {
            csvWriter.writeRecord(getHeaders(currentSchema));
            headerIsReady = true;
        }

        writeValues(record);
        
        result.totalCount++;
    }

    private void flush() throws IOException {
        csvWriter.flush();
    }

    @Override
    public Result close() throws IOException {
        flush();
        csvWriter.close();
        return result;
    }

    @Override
    public WriteOperation<Result> getWriteOperation() {
        return writeOperation;
    }

    private String[] getHeaders(Schema schema) {
        List<String> headers = new ArrayList<String>();
        for (Schema.Field f : schema.getFields()) {
            String dbColumnName = f.getProp(SchemaConstants.TALEND_COLUMN_DB_COLUMN_NAME);
            headers.add(StringUtils.isEmpty(dbColumnName) ? f.name() : dbColumnName);
        }
        return headers.toArray(new String[headers.size()]);
    }

    private void writeValues(IndexedRecord input) throws IOException {
        List<Field> fields = currentSchema.getFields();
        for (int i=0;i<fields.size();i++) {
            bulkFormatter.getFormatter(i).format(input, nullValue, csvWriter);
        }
        csvWriter.endRecord();
    }
}
