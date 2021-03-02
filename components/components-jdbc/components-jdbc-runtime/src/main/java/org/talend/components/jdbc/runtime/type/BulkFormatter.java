package org.talend.components.jdbc.runtime.type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.talend.components.common.runtime.FormatterUtils;
import org.talend.components.jdbc.CommonUtils;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;

import com.csvreader.CsvWriter;

public class BulkFormatter {

    private Formatter[] formatter;
    private boolean useTextEnclosure;

    public BulkFormatter(Schema inputSchema, Schema currentSchema, boolean useTextEnclosure) {
        List<Formatter> writers = new ArrayList<Formatter>();
        List<Field> fields = currentSchema.getFields();

        for (Field field : fields) {
            Field inputField = CommonUtils.getField(inputSchema, field.name());

            Field componentField = CommonUtils.getField(currentSchema, field.name());
            int inputValueLocation = inputField.pos();
            String pattern = componentField.getProp(SchemaConstants.TALEND_COLUMN_PATTERN);

            Schema basicSchema = AvroUtils.unwrapIfNullable(componentField.schema());

            Formatter writer = null;

            if (AvroUtils.isSameType(basicSchema, AvroUtils._date())) {
                writer = new DateTypeWriter(inputValueLocation, pattern);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._bytes())) {
                writer = new BytesTypeWriter(inputValueLocation);
            } else {
                writer = new StringTypeWriter(inputValueLocation);
            }

            writers.add(writer);
        }

        formatter = writers.toArray(new Formatter[0]);
        this.useTextEnclosure = useTextEnclosure;
    }
    
    public Formatter getFormatter(int i) {
        return formatter[i];
    }

    public class Formatter {

        protected final int inputValueLocation;

        Formatter(int inputValueLocation) {
            this.inputValueLocation = inputValueLocation;
        }

        public void format(IndexedRecord input, String nullValue, CsvWriter writer) throws IOException {
            // do nothing
        }

    }
    
    private void fillNull(String nullValue, CsvWriter writer) throws IOException {
        writer.setUseTextQualifier(false);
        writer.setForceQualifier(false);
        if(nullValue!=null) {
            writer.write(nullValue);
        } else {
            writer.write("");
        }
        writer.setUseTextQualifier(useTextEnclosure);
        writer.setForceQualifier(useTextEnclosure);
    }

    public class StringTypeWriter extends Formatter {

        StringTypeWriter(int inputValueLocation) {
            super(inputValueLocation);
        }

        public void format(IndexedRecord input, String nullValue, CsvWriter writer) throws IOException {
            Object inputValue = input.get(inputValueLocation);
            if(inputValue==null) {
                fillNull(nullValue, writer);
            } else {
                writer.write(String.valueOf(inputValue), true);
            }
        }
    }

    public class DateTypeWriter extends Formatter {

        private String pattern;
        
        DateTypeWriter(int inputValueLocation, String pattern) {
            super(inputValueLocation);
            this.pattern = pattern;
        }

        public void format(IndexedRecord input, String nullValue, CsvWriter writer) throws IOException {
            Object inputValue = input.get(inputValueLocation);
            if(inputValue==null) {
                fillNull(nullValue, writer);
            } else {
                writer.write(FormatterUtils.formatDate((Date)inputValue, pattern), true);
            }
        }

    }
    
    class BytesTypeWriter extends Formatter {
        //always use utf8? bytes array can't mean a lob object?
        //now use utf8, not use platform default as easy migration if fix it in future
        private Charset charset = Charset.forName("UTF-8");

        BytesTypeWriter(int inputValueLocation) {
            super(inputValueLocation);
        }

        public void format(IndexedRecord input, String nullValue, CsvWriter writer) throws IOException {
            Object inputValue = input.get(inputValueLocation);
            if(inputValue==null) {
                fillNull(nullValue, writer);
            } else {
                writer.write(charset.decode(ByteBuffer.wrap((byte[])inputValue)).toString(), true);
            }
        }

    }

}
