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
package org.talend.components.simplefileio.runtime;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ReflectionUtils;
import org.talend.components.simplefileio.SimpleFileIOErrorCode;
import org.talend.components.simplefileio.runtime.beamcopy.Write;
import org.talend.components.simplefileio.runtime.hadoop.csv.CSVFileInputFormat;
import org.talend.components.simplefileio.runtime.hadoop.csv.CSVFileRecordReader;
import org.talend.components.simplefileio.runtime.hadoop.csv.CSVFileSplit;
import org.talend.components.simplefileio.runtime.sinks.UgiFileSinkBase;
import org.talend.components.simplefileio.runtime.sinks.UnboundedWrite;
import org.talend.components.simplefileio.runtime.sources.CsvHdfsFileSource;
import org.talend.components.simplefileio.runtime.ugi.UgiDoAs;
import org.talend.daikon.avro.NameUtil;
import org.talend.daikon.avro.converter.IndexedRecordConverter;
import org.talend.daikon.exception.TalendRuntimeException;
import org.talend.daikon.exception.error.CommonErrorCodes;

public class SimpleRecordFormatCsvIO extends SimpleRecordFormatBase {

    private static final Log LOG = LogFactory.getLog(SimpleRecordFormatCsvIO.class);

    static {
        // Ensure that the singleton for the SimpleFileIOAvroRegistry is created.
        SimpleFileIOAvroRegistry.get();
    }

    private final String recordDelimiter;

    private final Integer maxRowSize;

    private final String fieldDelimiter;
    
    private String encoding = "UTF-8";
    
    private long header;

    private String username;

    private boolean s3;

    private String textEnclosure;
    
    private String escapeChar;

    /**
     * work for the reading
     * @param doAs
     * @param path
     * @param limit
     * @param recordDelimiter
     * @param fieldDelimiter
     */
    
    public SimpleRecordFormatCsvIO(UgiDoAs doAs, String path, int limit, String recordDelimiter, Integer maxRowSize,
            String fieldDelimiter, String encoding, long header, String textEnclosure, String escapeChar , String username, boolean s3) {
        this(doAs, path, false, limit, recordDelimiter, maxRowSize, fieldDelimiter, false);
        this.encoding = encoding;
        this.header = header < 1 ? 0 : header;
        this.textEnclosure = textEnclosure;
        this.escapeChar = escapeChar;
    }
    
    public SimpleRecordFormatCsvIO(UgiDoAs doAs, String path, boolean overwrite, int limit, String recordDelimiter,
                                   Integer maxRowSize, String fieldDelimiter, boolean mergeOutput) {
        super(doAs, path, overwrite, limit, mergeOutput);
        this.recordDelimiter = recordDelimiter;
        this.maxRowSize = maxRowSize;

        String fd = fieldDelimiter;
        if (fd.length() > 1) {
            fd = fd.trim();
        }
        if (fd.isEmpty())
            TalendRuntimeException.build(CommonErrorCodes.UNEXPECTED_ARGUMENT).setAndThrow("single character field delimiter",
                    fd);
        this.fieldDelimiter = fd;
    }

    @Override
    public PCollection<IndexedRecord> read(PBegin in) {
        boolean isGSFileSystem = false;
        final List<String> fields;
        PCollection<?> pc2;
        if (path.startsWith("gs://")) {
            isGSFileSystem = true;
            pc2 = in.apply(TextIO.read().from(path));
            fields = null;
        } else {
            CsvHdfsFileSource source = CsvHdfsFileSource.of(doAs, path, recordDelimiter, this.maxRowSize, encoding, header, textEnclosure, escapeChar);
            source.getExtraHadoopConfiguration().addFrom(getExtraHadoopConfiguration());
            fields = fetchSchemaAndInitSkipLengthFromHeader(source);
            source.setLimit(limit);

            PCollection<KV<org.apache.hadoop.io.LongWritable, BytesWritable>> pc1 = in.apply(Read.from(source));

            pc2 = pc1.apply(Values.<BytesWritable> create());
        }

        Character te = null;
        if(this.textEnclosure!=null && !this.textEnclosure.isEmpty()) {
            te = this.textEnclosure.charAt(0);
        }
        
        Character ec = null;
        if(this.escapeChar!=null && !this.escapeChar.isEmpty()) {
            ec = this.escapeChar.charAt(0);
        }
        
        PCollection<IndexedRecord> pc3 = pc2.apply(ParDo.of(new ExtractCsvRecord<>(fieldDelimiter.charAt(0), isGSFileSystem, encoding, te, ec, fields)));

        return pc3;
    }

    private List<String> fetchSchemaAndInitSkipLengthFromHeader(CsvHdfsFileSource source) {
        // 1. fetch the schema information by the header from one single
        // file which the path point to or the path is a directory, we get
        // one not empty single file from the directory to do it
        // 2. if only one single file, not a directory which contains multi
        // csv files, we can get
        // the skip length bytes by the header setting and pass it to
        // CSVFileInputFormat to avoid computer it two times
        // But if a directory, we can't do it and have to computer it two
        // times

        try {
            return doAs.doAs(new PrivilegedExceptionAction<List<String>>() {

                @Override
                public List<String> run() throws Exception {
                    Job job = source.jobInstance();

                    // do check to avoid the no meaning reflect issue from the
                    // listStatus below
                    Path pt = new Path(path);// the path may be a pattern, not a
                    // static path value
                    FileSystem fs = pt.getFileSystem(job.getConfiguration());
                    fs.exists(pt);

                    List<FileStatus> status = null;
                    try {
                        status = source.listStatus(job);
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "the path : " + path + " don't exist or don't contain any file in file system : " + e.getMessage()
                                        + " cause : " + e.getCause() != null ? e.getCause().getMessage() : "");
                    }

                    if (status == null || status.isEmpty()) {
                        throw new RuntimeException(
                                "the path : " + path + " don't exist or don't contain any file in file system");
                    }

                    FileStatus firstNotEmptyFile = null;
                    for (FileStatus stt : status) {
                        if (stt.getLen() > 0) {
                            firstNotEmptyFile = stt;
                            break;
                        }
                    }

                    List<String> fields = new ArrayList<>();

                    if (firstNotEmptyFile == null) {
                        LOG.info("all the files are empty in the path : " + path);
                        return fields;
                    }

                    try (CSVFileRecordReader reader = new CSVFileRecordReader(recordDelimiter, encoding,
                            (textEnclosure != null && textEnclosure.length() > 0) ? textEnclosure.charAt(0) : null,
                            (escapeChar != null && escapeChar.length() > 0) ? escapeChar.charAt(0) : null)) {
                        CSVFileSplit split = new CSVFileSplit(firstNotEmptyFile.getPath(), 0, firstNotEmptyFile.getLen(), 0,
                                new String[0]);
                        reader.initialize(split, job.getConfiguration());
                        boolean hasNext = false;
                        long header = SimpleRecordFormatCsvIO.this.header;
                        boolean useDefaultFieldName = header == 0;// use default
                        // field
                        // name if
                        // not set
                        // header
                        // like
                        // field0,
                        // field1,field2
                        while ((header--) > -1 && (hasNext = reader.nextKeyValue())) {
                            if (header < 1) {
                                BytesWritable bytes = reader.getCurrentValue();
                                String rowValue = new String(bytes.copyBytes(), encoding);
                                CSVFormat cf = createCSVFormat(fieldDelimiter.charAt(0), textEnclosure, escapeChar);
                                for (CSVRecord r : cf.parse(new StringReader(rowValue))) {
                                    fields = inferSchemaInfo(r, useDefaultFieldName);
                                    LOG.info("success to fetch the schema : " + fields);

                                    // as the first not empty file is
                                    // computer for the skip
                                    // length by header, no need to computer
                                    // it second time in CSVFileInputFormat
                                    long skipLength4FirstNotEmptyFile = reader.getFilePosition();
                                    source.getExtraHadoopConfiguration().set(CSVFileInputFormat.FILE_TO_FETCH_SCHEMA,
                                            firstNotEmptyFile.getPath().toString());
                                    source.getExtraHadoopConfiguration().set(
                                            CSVFileInputFormat.SKIP_LENGTH_FOR_FILE_TO_FETCH_SCHEMA,
                                            "" + skipLength4FirstNotEmptyFile);
                                }
                                break;
                            }
                        }

                        if (!hasNext && header >= 0) {
                            LOG.info("header value exceed the limit of the file");
                        }
                    }

                    return fields;
                }

            });
        } catch (AccessControlException e) {
            throw createCommonException(e);
        } catch (InvalidInputException e) {
            throw createCommonException(e);
        } catch (LoginException e) {
            throw createCommonException(e);
        } catch (IOException e) {
            throw TalendRuntimeException.createUnexpectedException(e);
        } catch (RuntimeException e) {
            if (e.getCause() == null) {
                throw e;
            }
            Throwable cause = e.getCause();
            if (cause instanceof AccessControlException)
                throw createCommonException(e);
            if (cause instanceof LoginException)
                throw createCommonException(e);
            if (cause instanceof InvalidInputException)
                throw createCommonException(e);
            if (cause instanceof IOException) {
                throw TalendRuntimeException.createUnexpectedException(e);
            }
            throw TalendRuntimeException.createUnexpectedException(cause);
        } catch (Exception e) {
            throw TalendRuntimeException.createUnexpectedException(e);
        }
    }




    private RuntimeException createCommonException(Exception e) {
        if (s3) {
            return TalendRuntimeException.createUnexpectedException(e);
        }
        return SimpleFileIOErrorCode.createInputNotAuthorized(e, username, path);
    }

    // TODO check if can round trip : any the same config, generated data by
    // sink can be read by source
    private static CSVFormat createCSVFormat(char fieldDelimiter, String textEnclosure, String escapeChar) {
        // CSVFormat.RFC4180 use " as quote and no escape char and "," as field
        // delimiter and only quote if quote is set and necessary
        CSVFormat format = CSVFormat.RFC4180.withDelimiter(fieldDelimiter);

        Character te = null;
        if (textEnclosure != null && !textEnclosure.isEmpty()) {
            te = textEnclosure.charAt(0);
        }

        Character ec = null;
        if (escapeChar != null && !escapeChar.isEmpty()) {
            ec = escapeChar.charAt(0);
        }

        // the with method return a new object, so have to assign back
        if (te != null) {
            format = format.withQuote(te);
        } else {
            format = format.withQuote(null);
        }

        if (ec != null) {
            format = format.withEscape(ec);
        }
        return format;
    }

    // get the schema name information from the single header row
    private static List<String> inferSchemaInfo(CSVRecord singleHeaderRow, boolean useDefaultFieldName) {
        // ArrayList can Serializable, so can pass to the ExtractCsvRecord
        List<String> result = new ArrayList<>();
        Set<String> existNames = new HashSet<String>();
        int index = 0;
        for (int i = 0; i < singleHeaderRow.size(); i++) {
            String fieldName = singleHeaderRow.get(i);
            if (useDefaultFieldName || fieldName == null || fieldName.isEmpty()) {
                fieldName = "field" + i;
            }

            String finalName = NameUtil.correct(fieldName, index++, existNames);
            existNames.add(finalName);

            result.add(finalName);
        }
        return result;
    }




    @Override
    public PDone write(PCollection<IndexedRecord> in) {

        if (path.startsWith("gs://")) {
            // The Google Storage case is a special workaround. We expect all filesystems to use the Beam unified
            // file system eventually in the same way.
            PCollection<String> pc1 =
                    in.apply("FormatCSVRecord", ParDo.of(new FormatCsvRecord2(fieldDelimiter.charAt(0))));

            if (in.isBounded() == PCollection.IsBounded.BOUNDED) {
                return pc1.apply(TextIO.write().to(path));
            } else {
                pc1 = UnboundedWrite.ofDefaultWindow(pc1);
                WriteFilesResult results =
                        pc1.apply(FileIO.<String> write().withNumShards(1).via(TextIO.sink()).to(path));
                return PDone.in(results.getPipeline());
            }

        } else {
            ExtraHadoopConfiguration conf = new ExtraHadoopConfiguration();
            conf.set(CsvTextOutputFormat.RECORD_DELIMITER, recordDelimiter);
            conf.set(CsvTextOutputFormat.ENCODING, CsvTextOutputFormat.UTF_8);
            UgiFileSinkBase<NullWritable, Text> sink =
                    new UgiFileSinkBase<>(doAs, path, overwrite, mergeOutput, CsvTextOutputFormat.class, conf);
            sink.getExtraHadoopConfiguration().addFrom(getExtraHadoopConfiguration());

            PCollection<KV<NullWritable, Text>> pc1 = in.apply(ParDo.of(new FormatCsvRecord(fieldDelimiter.charAt(0))))
                    .setCoder(KvCoder.of(WritableCoder.of(NullWritable.class), WritableCoder.of(Text.class)));

            if (in.isBounded() == PCollection.IsBounded.BOUNDED) {
                return pc1.apply(Write.to(sink));
            } else {
                return pc1.apply(UnboundedWrite.of(sink));
            }
        }

    }

    public static class ExtractCsvSplit extends DoFn<Text, String[]> {

        static {
            // Ensure that the singleton for the SimpleFileIOAvroRegistry is created.
            SimpleFileIOAvroRegistry.get();
        }

        public final String fieldDelimiter;

        ExtractCsvSplit(String fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String in = c.element().toString();
            c.output(in.split("\\Q" + fieldDelimiter + "\\E"));
        }
    }

    public static class ExtractCsvRecord<T> extends DoFn<T, IndexedRecord> {

        static {
            // Ensure that the singleton for the SimpleFileIOAvroRegistry is created.
            SimpleFileIOAvroRegistry.get();
        }

        public final char fieldDelimiter;
        
        public final boolean isGSFileSystem; 
        
        public final String encoding; 
        
        public final Character textEnclosure; 
        
        public final Character escapeChar;

        private final List<String> fields;

        /** The converter is cached for performance. */
        private transient IndexedRecordConverter<CSVRecord, ? extends IndexedRecord> converter;

        public ExtractCsvRecord(char fieldDelimiter, boolean isGSFileSystem, String encoding, Character textEnclosure, Character escapeChar,List<String> fields) {
            this.fieldDelimiter = fieldDelimiter;
            this.isGSFileSystem = isGSFileSystem;
            this.encoding = encoding;
            this.textEnclosure = textEnclosure;
            this.escapeChar = escapeChar;
            this.fields = fields;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            if (converter == null) {
                converter = new SimpleFileIOAvroRegistry.CsvRecordToIndexedRecordConverter();
                if (fields == null || fields.isEmpty()) {
                    // do nothing, then computer the schema by the value size
                    // from the data row, the get "field0","field1"...
                } else {
                    // set the schema by the schema we computer by header
                    // successfully
                    converter.setSchema(convertList2Schema(this.fields));
                }
            }
            
            if(isGSFileSystem) {
                String in = c.element().toString();
                for (CSVRecord r : CSVFormat.RFC4180.withDelimiter(fieldDelimiter).parse(new StringReader(in))) {
                    c.output(converter.convertToAvro(r));
                }
                
                return;
            }
            
            BytesWritable bytes = (BytesWritable)c.element();
            String rowValue = new String(bytes.copyBytes(), encoding);
            
            //CSVFormat.RFC4180 use " as quote and no escape char and "," as field delimiter
            
            //TODO create it every time? performance?
            CSVFormat cf = CSVFormat.RFC4180.withDelimiter(fieldDelimiter);
            //the with method return a new object, so have to assign back
            if(textEnclosure!=null) {
                cf = cf.withQuote(textEnclosure);
            } else {
                cf = cf.withQuote(null);
            }
            
            if(escapeChar!=null) {
                cf = cf.withEscape(escapeChar);
            }
            
            for (CSVRecord r : cf.parse(new StringReader(rowValue))) {
                c.output(converter.convertToAvro(r));
            }
        }


    }

    // get the schema name information from the single header row
    private static Schema convertList2Schema(List<String> fields) {
        SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder.record("StringArrayRecord").fields();
        for (int i = 0; i < fields.size(); i++) {
            fa = fa.name(fields.get(i)).type(Schema.create(Schema.Type.STRING)).noDefault();
        }
        return fa.endRecord();
    }

    public static class CsvTextOutputFormat extends TextOutputFormat<NullWritable, Text> {

        public static final String RECORD_DELIMITER = "textoutputformat.record.delimiter";

        public static final String ENCODING = "csvtextoutputformat.encoding";

        public static final String UTF_8 = "UTF-8";

        public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            boolean isCompressed = getCompressOutput(job);
            String recordDelimiter = conf.get(RECORD_DELIMITER, "\n");
            CompressionCodec codec = null;
            String extension = "";
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
                codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
                extension = codec.getDefaultExtension();
            }
            Path file = getDefaultWorkFile(job, extension);
            FileSystem fs = file.getFileSystem(conf);
            if (!isCompressed) {
                FSDataOutputStream fileOut = fs.create(file, false);
                return new CsvRecordWriter(fileOut, UTF_8, recordDelimiter);
            } else {
                FSDataOutputStream fileOut = fs.create(file, false);
                return new CsvRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), UTF_8, recordDelimiter);
            }
        }

        protected static class CsvRecordWriter extends RecordWriter<NullWritable, Text> {

            public final String encoding;

            private final byte[] recordDelimiter;

            protected DataOutputStream out;

            public CsvRecordWriter(DataOutputStream out, String encoding, String recordDelimiter) {
                this.out = out;
                this.encoding = encoding;
                try {
                    this.recordDelimiter = recordDelimiter.getBytes(encoding);
                } catch (UnsupportedEncodingException uee) {
                    throw new IllegalArgumentException("Encoding " + encoding + " not found.");
                }
            }

            public synchronized void write(NullWritable key, Text value) throws IOException {
                out.write(value.toString().getBytes(encoding));
                out.write(recordDelimiter);
            }

            public synchronized void close(TaskAttemptContext context) throws IOException {
                out.close();
            }
        }
    }

    public static class FormatCsvRecord extends DoFn<IndexedRecord, KV<NullWritable, Text>> {

        public final char fieldDelimiter;

        private final CSVFormat format;

        private StringBuilder sb = new StringBuilder();

        public FormatCsvRecord(char fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
            format = CSVFormat.RFC4180.withDelimiter(fieldDelimiter);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            // Join the strings with the delimiter.
            IndexedRecord in = c.element();
            int size = in.getSchema().getFields().size();
            for (int i = 0; i < size; i++) {
                Object valueToWrite = in.get(i);
                if (valueToWrite instanceof ByteBuffer)
                    valueToWrite = new String(((ByteBuffer) valueToWrite).array());
                format.print(valueToWrite, sb, sb.length() == 0);
            }
            c.output(KV.of(NullWritable.get(), new Text(sb.toString())));
            sb.setLength(0);
        }
    }

    public static class FormatCsvRecord2 extends DoFn<IndexedRecord, String> {

        public final char fieldDelimiter;

        private final CSVFormat format;

        private StringBuilder sb = new StringBuilder();

        public FormatCsvRecord2(char fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
            format = CSVFormat.RFC4180.withDelimiter(fieldDelimiter);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            // Join the strings with the delimiter.
            IndexedRecord in = c.element();
            int size = in.getSchema().getFields().size();
            for (int i = 0; i < size; i++) {
                Object valueToWrite = in.get(i);
                if (valueToWrite instanceof ByteBuffer)
                    valueToWrite = new String(((ByteBuffer) valueToWrite).array());
                format.print(valueToWrite, sb, sb.length() == 0);
            }
            c.output(sb.toString());
            sb.setLength(0);
        }
    }
}
