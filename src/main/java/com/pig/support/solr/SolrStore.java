package com.pig.support.solr;



import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Create a lucene index
 *
 */
public class SolrStore extends StoreFunc implements StoreMetadata {

	private static final String SCHEMA_SIGNATURE = "solr.output.schema";
	static Logger log=LoggerFactory.getLogger(SolrStore.class);
	ResourceSchema schema;
	String udfSignature;
	RecordWriter<Writable, SolrInputDocument> writer;
	//solr的链接地址
	String address;

	public SolrStore(String address) {
		this.address = address;
	}

	public void storeStatistics(ResourceStatistics stats, String location,
			Job job) throws IOException {
	}

	public void storeSchema(ResourceSchema schema, String location, Job job)
			throws IOException {
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		UDFContext udfc = UDFContext.getUDFContext();
		Properties p = udfc.getUDFProperties(this.getClass(),
				new String[] { udfSignature });
		p.setProperty(SCHEMA_SIGNATURE, s.toString());
	}

	public OutputFormat<Writable, SolrInputDocument> getOutputFormat()
			throws IOException {
		// not be used
		return new SolrOutputFormat(address);
	}

	/**
	 * Not used
	 */
	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		this.udfSignature = signature;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
		UDFContext udc = UDFContext.getUDFContext();
		String schemaStr = udc.getUDFProperties(this.getClass(),
				new String[] { udfSignature }).getProperty(SCHEMA_SIGNATURE);

		if (schemaStr == null) {
			throw new RuntimeException("Could not find udf signature");
		}

		schema = new ResourceSchema(Utils.getSchemaFromString(schemaStr));

	}

	/**
	 * Shamelessly copied from : https://issues.apache.org/jira/secure/attachment/12484764/NUTCH-1016-2.0.patch
	 * @param input
	 * @return
	 */
	private static String stripNonCharCodepoints(String input) {
		StringBuilder retval = new StringBuilder(input.length());
		char ch;

		for (int i = 0; i < input.length(); i++) {
			ch = input.charAt(i);

			// Strip all non-characters
			// http://unicode.org/cldr/utility/list-unicodeset.jsp?a=[:Noncharacter_Code_Point=True:]
			// and non-printable control characters except tabulator, new line
			// and carriage return
			if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step
											// 0x10000
					ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
					(ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
					(ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {

				retval.append(ch);
			}
		}

		return retval.toString();
	}

	@Override
	public void putNext(Tuple t) throws IOException {

		final SolrInputDocument doc = new SolrInputDocument();

		final ResourceFieldSchema[] fields = schema.getFields();
		int docfields = 0;

		for (int i = 0; i < fields.length; i++) {
			final Object value = t.get(i);
				//值不能为null，必须有值的情况下 才添加索引
			if (value != null&&value.toString().trim().length()>0) {
				docfields++;
				//取到索引值
				String indexValue=stripNonCharCodepoints(value.toString()).trim();
//				if(fields[i].getName().equals("cpyNatureCode")){//多值域
//					for(String mv:indexValue.split("#")){
//						doc.addField(fields[i].getName().trim(), mv);
//					}
//				}else{//非多值域
					doc.addField(fields[i].getName().trim(), indexValue);
//				}

			}

		}

		try {
			if (docfields > 0)
				writer.write(null, doc);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}

	}

}
