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
package org.talend.components.salesforce.runtime;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.talend.components.api.component.runtime.Reader;
import org.talend.components.api.container.DefaultComponentRuntimeContainerImpl;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.DataRejectException;
import org.talend.components.salesforce.test.SalesforceRuntimeTestUtil;
import org.talend.components.salesforce.test.SalesforceTestBase;
import org.talend.components.salesforce.tsalesforcebulkexec.TSalesforceBulkExecDefinition;
import org.talend.components.salesforce.tsalesforcebulkexec.TSalesforceBulkExecProperties;
import org.talend.daikon.avro.AvroRegistry;
import org.talend.daikon.avro.converter.IndexedRecordConverter;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SalesforceBulkLoadJSONTestIT extends SalesforceTestBase {
	@ClassRule
	public static final TestRule DISABLE_IF_NEEDED = new DisableIfMissingConfig();

	private SalesforceRuntimeTestUtil util = new SalesforceRuntimeTestUtil();

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void  testInsert() throws Throwable {
		String data_file = tempFolder.newFile("Insert.json").getAbsolutePath();
		new ObjectMapper().writeValue(new File(data_file),util.getTestData());
		// bulkexec part
		TSalesforceBulkExecDefinition defin = (TSalesforceBulkExecDefinition) getComponentService()
				.getComponentDefinition(TSalesforceBulkExecDefinition.COMPONENT_NAME);
		TSalesforceBulkExecProperties modelProps = (TSalesforceBulkExecProperties) defin.createRuntimeProperties();
		SalesforceBulkExecRuntime bulk =
				util.initBulk(defin, data_file, modelProps, util.getTestSchema1(), util.getTestSchema5());

		modelProps.outputAction.setValue(TSalesforceBulkExecProperties.OutputAction.INSERT);
		modelProps.contentType.setValue(TSalesforceBulkExecProperties.ContentType.JSON);
		RuntimeContainer container = new DefaultComponentRuntimeContainerImpl() {

			@Override
			public String getCurrentComponentId() {
				return "tSalesforceBulkExec";
			}
		};
		bulk.runAtDriver(container);
		Assert.assertEquals(3, container.getGlobalData("tSalesforceBulkExecNB_SUCCESS"));
	}


	@Test
	public void  testBulkReaderInsert() throws Throwable {
		String data_file = tempFolder.newFile("Insert.json").getAbsolutePath();
		new ObjectMapper().writeValue(new File(data_file),util.getTestData());
		// bulkexec part
		TSalesforceBulkExecDefinition defin = (TSalesforceBulkExecDefinition) getComponentService()
				.getComponentDefinition(TSalesforceBulkExecDefinition.COMPONENT_NAME);
		TSalesforceBulkExecProperties modelProps = (TSalesforceBulkExecProperties) defin.createRuntimeProperties();

		Reader reader = util.initReader(defin, data_file, modelProps, util.getTestSchema1(), util.getTestSchema5());

		modelProps.outputAction.setValue(TSalesforceBulkExecProperties.OutputAction.INSERT);
		modelProps.contentType.setValue(TSalesforceBulkExecProperties.ContentType.JSON);

				List<String> ids = new ArrayList<String>();
				List<String> sids = new ArrayList<String>();
				try {
					IndexedRecordConverter<Object, ? extends IndexedRecord> factory = null;

					final List<Map<String, String>> result = new ArrayList<Map<String, String>>();

					for (boolean available = reader.start(); available; available = reader.advance()) {

						IndexedRecord data = IndexedRecord.class.cast(reader.getCurrent());

						Assert.assertTrue("true".equals(data.get(5)));//schema column 5 -> Success
					}
				} finally {
					reader.close();
				}
	}

	@Test
	public void testDelete() throws Throwable {
		List<String> ids = util.createTestData();

		final List<Map<String, String>> testData = new ArrayList<Map<String, String>>();
		for (String id : ids) {
			Map<String, String> row = new HashMap<String, String>();
			row.put("Id", id);
			testData.add(row);
		}
		String data_file = tempFolder.newFile("delete.json").getAbsolutePath();
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(new File(data_file),testData);

		// bulkexec part
		TSalesforceBulkExecDefinition defin = (TSalesforceBulkExecDefinition) getComponentService()
				.getComponentDefinition(TSalesforceBulkExecDefinition.COMPONENT_NAME);
		TSalesforceBulkExecProperties modelProps = (TSalesforceBulkExecProperties) defin.createRuntimeProperties();
		Reader reader = util.initReader(defin, data_file, modelProps, util.getTestSchema1(), util.getTestSchema5());
		modelProps.contentType.setValue(TSalesforceBulkExecProperties.ContentType.JSON);

		modelProps.outputAction.setValue(TSalesforceBulkExecProperties.OutputAction.DELETE);

		try {
			IndexedRecordConverter<Object, ? extends IndexedRecord> factory = null;

			List<String> resultIds = new ArrayList<String>();
			for (boolean available = reader.start(); available; available = reader.advance()) {
				try {
					Object data = reader.getCurrent();

					factory = initCurrentData(factory, data);
					IndexedRecord record = factory.convertToAvro(data);
					String id = (String) record.get(4);//record ids
					resultIds.add(id);
				} catch (Exception e) {
					Assert.fail(e.getMessage());
				}
			}

			Assert.assertEquals(ids, resultIds);
		} finally {
			try {
				reader.close();
			} finally {
				try{
					util.deleteTestData(ids);
				}catch (AssertionError e){
					// do nothing
				}
			}
		}
	}


	@Test
	public void testUpdate() throws Throwable {
		List<String> ids = util.createTestData();

		String id = ids.get(0);

		final List<Map<String, String>> testData = new ArrayList<Map<String, String>>();
		Map<String, String> datarow = new HashMap<String, String>();
		datarow.put("Id", id);
		datarow.put("FirstName", "Wei");
		datarow.put("LastName", "Wang");
		datarow.put("Phone", "010-89492686");// update the field
		testData.add(datarow);

		datarow = new HashMap<String, String>();
		datarow.put("Id", "not_exist");// should reject
		datarow.put("FirstName", "Who");
		datarow.put("LastName", "Who");
		datarow.put("Phone", "010-89492686");
		testData.add(datarow);

		String data_file = tempFolder.newFile("update.json").getAbsolutePath();
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(new File(data_file),testData);

		// bulkexec part
		TSalesforceBulkExecDefinition defin = (TSalesforceBulkExecDefinition) getComponentService()
				.getComponentDefinition(TSalesforceBulkExecDefinition.COMPONENT_NAME);
		Schema testSchema4 = util.getTestSchema6();
		testSchema4.addProp("include-all-fields","true");
		TSalesforceBulkExecProperties modelProps = (TSalesforceBulkExecProperties) defin.createRuntimeProperties();
		Reader reader = util.initReader(defin, data_file, modelProps, testSchema4, testSchema4);
		modelProps.contentType.setValue(TSalesforceBulkExecProperties.ContentType.JSON);
		modelProps.outputAction.setValue(TSalesforceBulkExecProperties.OutputAction.UPDATE);

		try {
			IndexedRecordConverter<Object, ? extends IndexedRecord> factory = null;

			for (boolean available = reader.start(); available; available = reader.advance()) {
				try {
					Object data = reader.getCurrent();

					factory = initCurrentData(factory, data);
					IndexedRecord record = factory.convertToAvro(data);
					String phone = String.valueOf( record.get(4));

					Assert.assertTrue("true".equals(record.get(5)));//schema column 6 -> Success
					Assert.assertEquals("010-89492686", phone);

				} catch (DataRejectException e) {
					Map<String, Object> info = e.getRejectInfo();
					Object data = info.get("talend_record");
					String err = (String) info.get("error");

					factory = initCurrentData(factory, data);
					IndexedRecord record = factory.convertToAvro(data);
					String resultid = (String) record.get(1);
					String firstname = (String) record.get(2);
					String lastname = (String) record.get(3);
					String phone = (String) record.get(4);

					// id should not null, it should be keep as in bulk file.
					Assert.assertEquals("not_exist",resultid);
					Assert.assertEquals("Who", firstname);
					Assert.assertEquals("Who", lastname);
					Assert.assertEquals("010-89492686", phone);
					Assert.assertTrue("false".equals(record.get(5)));//schema column 6 -> Success
					Assert.assertTrue(err != null);
				}
			}
		} finally {
			try {
				reader.close();
			} finally {
				util.deleteTestData(ids);
			}
		}
	}

	@Test
	public void testUpsert() throws Throwable {
		List<String> ids = util.createTestData();

		String id = ids.get(0);

		final List<Map<String, String>> testData = new ArrayList<Map<String, String>>();
		Map<String, String> datarow = new HashMap<String, String>();
		datarow.put("Id", id);// should update
		datarow.put("FirstName", "Wei");
		datarow.put("LastName", "Wang");
		datarow.put("Phone", "010-89492686");// update the field
		testData.add(datarow);

		datarow = new HashMap<String, String>();
		datarow.put("Id", null);// should insert
		datarow.put("FirstName", "Who");
		datarow.put("LastName", "Who");
		datarow.put("Phone", "010-89492686");
		testData.add(datarow);

		String data_file = tempFolder.newFile("upsert.json").getAbsolutePath();
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(new File(data_file),testData);

		// bulkexec part
		TSalesforceBulkExecDefinition defin = (TSalesforceBulkExecDefinition) getComponentService()
				.getComponentDefinition(TSalesforceBulkExecDefinition.COMPONENT_NAME);
		TSalesforceBulkExecProperties modelProps = (TSalesforceBulkExecProperties) defin.createRuntimeProperties();
		Reader reader = util.initReader(defin, data_file, modelProps, util.getTestSchema6(), util.getTestSchema6());

		modelProps.outputAction.setValue(TSalesforceBulkExecProperties.OutputAction.UPSERT);
		modelProps.upsertKeyColumn.setValue("Id");
		modelProps.contentType.setValue(TSalesforceBulkExecProperties.ContentType.JSON);

		try {
			IndexedRecordConverter<Object, ? extends IndexedRecord> factory = null;

			int index = -1;
			for (boolean available = reader.start(); available; available = reader.advance()) {
				try {
					Object data = reader.getCurrent();

					factory = initCurrentData(factory, data);
					IndexedRecord record = factory.convertToAvro(data);
					index++;
					if (index == 0) {
						Assert.assertTrue("false".equals(record.get(5)));//schema column 5 -> salesforce_created
					} else if (index == 1) {
						Assert.assertTrue("true".equals(record.get(5)));//schema column 5 -> salesforce_created
					}
				} catch (DataRejectException e) {
					Assert.fail(e.getMessage());
				}
			}
		} finally {
			try {
				reader.close();
			} finally {
				util.deleteTestData(ids);
			}
		}
	}

    private IndexedRecordConverter<Object, ? extends IndexedRecord> initCurrentData(
            IndexedRecordConverter<Object, ? extends IndexedRecord> factory, Object data) {
        if (factory == null) {
            factory = (IndexedRecordConverter<Object, ? extends IndexedRecord>) new AvroRegistry()
                    .createIndexedRecordConverter(data.getClass());
        }

        // IndexedRecord unenforced = factory.convertToAvro(data);
        // current.setWrapped(unenforced);
        return factory;
    }

}
