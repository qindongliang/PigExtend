package com.pig.support.elasticsearch;


import com.google.common.collect.Maps;
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
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

/**
 *
 * Create a elasticsearch index
 *
 */
public class EsStore extends StoreFunc implements StoreMetadata {

	private static final String SCHEMA_SIGNATURE = "es.output.schema";
	static Logger log=LoggerFactory.getLogger(EsStore.class);
	ResourceSchema schema;
	String udfSignature;
	RecordWriter<Writable, IndexRequest> writer;

	String clusterName;//集群名
	String indexName;//索引名
	String typeName;//类别名
	String address;//ip地址，多个以#号隔开
	int port;//端口

	public EsStore(String address,String indexName,String typeName,int port,String clusterName) {
		this.address = address;
		this.indexName=indexName;
		this.typeName=typeName;
		this.port=port;
		this.clusterName=clusterName;

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

	public OutputFormat<Writable, IndexRequest> getOutputFormat()
			throws IOException {
		// not be used
		return new EsOutputFormat(address,port,clusterName);
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
		//参数分别是索引名，类型名，和主键id
		final IndexRequest doc=new IndexRequest(indexName,typeName,t.get(0)+"");
		final ResourceFieldSchema[] fields = schema.getFields();
		HashMap<String,Object> datas= Maps.newHashMap();
		for (int i = 1; i < fields.length; i++) {
			final Object value = t.get(i);
				//值不能为null，必须有值的情况下 才添加索引
			if (value != null&&value.toString().trim().length()>0) {
				//取到索引值
				String indexValue=stripNonCharCodepoints(value.toString()).trim();
				datas.put(fields[i].getName().trim(), indexValue);
			}

		}
		doc.source(datas);//封装数据
		try {
			if (!datas.isEmpty()) {//非空值才写入索引
				writer.write(null, doc);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}

	}

}
