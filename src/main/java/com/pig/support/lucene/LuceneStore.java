package com.pig.support.lucene;



import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pig.support.tools.Business;

/**
 *
 * Create a lucene index
 * 定义支持Lucene索引存储的UDF函数
 *
 *
 */
public class LuceneStore extends StoreFunc implements StoreMetadata {

	private static final String SCHEMA_SIGNATURE = "lucene.output.schema";
	private static final String FIELDS_DESC = "lucene.fields.desc";
	static Logger log=LoggerFactory.getLogger(LuceneStore.class);
	ResourceSchema schema;
	String udfSignature;
	RecordWriter<Writable, Document> writer;
	/**枚举的业务类型*/
	Business business;
	String location;
	private Map<String, FieldDesc> fieldDescMap;
	/**字段的shcmal描述*/
	String fieldDesc;

//	public LuceneStore() {
//	}

	/**
	 * @param fieldDesc
	 *            format is: fieldName:[store{true/false}]:tokenize[true/false]
	 */
//	public LuceneStore(String fieldDesc) {
//		UDFContext udfc = UDFContext.getUDFContext();
//		Properties p = udfc.getUDFProperties(this.getClass(),new String[] { udfSignature });
//		p.setProperty(FIELDS_DESC, fieldDesc);
//	}

	/**
	 * @param fieldDesc
	 *            format is: fieldName:[store{true/false}]:tokenize[true/false]
	 * @param business 建索引的业务类型
	 */
	public LuceneStore(String fieldDesc,String business) {
//		UDFContext udfc = UDFContext.getUDFContext();
//		Properties p = udfc.getUDFProperties(this.getClass(),new String[] { udfSignature });
//		p.setProperty(FIELDS_DESC, fieldDesc);
		this.fieldDesc=fieldDesc;
		//给业务类型赋值
		this.business=Business.getBusiness(business);
		Log.info("初始化LuceneStorage获取的值： {}  获取后的枚举的值：{} ",business,this.business.getValue());
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
		Properties p = udfc.getUDFProperties(this.getClass(),new String[] { udfSignature });
		p.setProperty(SCHEMA_SIGNATURE, s.toString());
	}

	@Override
	public OutputFormat<Writable, Document> getOutputFormat()
			throws IOException {
		return new LuceneOutputFormat(location,business);
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
		this.location = location;
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		this.udfSignature = signature;
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
		UDFContext udc = UDFContext.getUDFContext();
		String schemaStr = udc.getUDFProperties(this.getClass(),new String[] { udfSignature }).getProperty(SCHEMA_SIGNATURE);
//		String fieldsDesc = udc.getUDFProperties(this.getClass(),new String[] { udfSignature }).getProperty(FIELDS_DESC);
		if (schemaStr == null) {
			throw new RuntimeException("Could not find udf signature");
		}

		schema = new ResourceSchema(Utils.getSchemaFromString(schemaStr));
		fieldDescMap = FieldDesc.parseFieldsDesc(this.fieldDesc);
		log.info("解析schemal了是{},传过来的schemaStr是{}",fieldDescMap,schemaStr);
	}

	@Override
	public void putNext(Tuple t) throws IOException {
		final Document doc = new Document();

		final ResourceFieldSchema[] fields = schema.getFields();
		int docfields = 0;

		for (int i = 0; i < fields.length; i++) {
			final Object value = t.get(i);
			//忽略空值
			if (value != null&&value.toString().trim().length()>0){

				//企业信息库
				if(business.equals(Business.CPY)){
				//copyField字段在此设置
				if(fields[i].getName().equals("cpyName")){
					//不分词的名字
					FieldType ft=new FieldType();
					ft.setTokenized(true);//分词
					ft.setStored(false);//存储
					ft.setIndexOptions(IndexOptions.DOCS);//索引
				    doc.add(new Field("sname",value.toString(),ft));

				    //拼音
					FieldType pinyin=new FieldType();
					pinyin.setTokenized(true);//分词
					pinyin.setStored(false);//存储
					pinyin.setIndexOptions(IndexOptions.DOCS);//索引
				    doc.add(new Field("cpyNamePy",value.toString(),pinyin));
				}
				}

				final IndexableField field = getField(fieldDescMap, fields[i], value);
				if(field != null){
					doc.add(field);
					docfields++;
				}


			}
		}

		try {
			if(docfields > 0)
				writer.write(null, doc);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}

	}

	/**
	 * Returns the correct IndexableField instance depending on the
	 * ResourceFieldSchema.type
	 *
	 * @param schemaField
	 * @param value
	 * @return
	 */
	private static final IndexableField getField(final Map<String, FieldDesc> fieldDescMap,final ResourceFieldSchema schemaField, final Object value) {

		final IndexableField field;
		final byte type = schemaField.getType();

		final FieldDesc fieldDesc = fieldDescMap.get(schemaField.getName());
		final Field.Store store;
		final boolean tokenize;

		if (fieldDesc == null) {
			tokenize = true;
			store = Field.Store.YES;
		} else {
			store = (fieldDesc.store) ? Field.Store.YES : Field.Store.NO;
			tokenize = fieldDesc.tokenize;
		}

		if (type == DataType.CHARARRAY) {
			final String strval = value.toString().trim();
			if(strval.length() < 1)
				field = null;
			else
			 field = (tokenize) ? new TextField(schemaField.getName(),
					strval, store) : new StringField(
					schemaField.getName(), strval, store);

		} else if (type == DataType.BOOLEAN) {
			field = new StringField(schemaField.getName(), value.toString().toLowerCase(), store);
		} else if (type == DataType.BYTE) {
			field = new IntField(schemaField.getName(), (Byte) value, store);
		} else if (type == DataType.INTEGER) {
			field = new IntField(schemaField.getName(),
					((Number) value).intValue(), store);
		} else if (type == DataType.LONG) {
			field = new LongField(schemaField.getName(),((Number) value).longValue(), store);
		} else if (type == DataType.FLOAT) {
			field = new FloatField(schemaField.getName(),((Number) value).floatValue(), store);
		} else if (type == DataType.DOUBLE) {
			field = new DoubleField(schemaField.getName(),((Number) value).doubleValue(), store);
		} else {
			throw new RuntimeException("The data type: " + type
					+ " is not supported");
		}

		return field;
	}

	static class FieldDesc {
		final boolean store;
		final boolean tokenize;

		public FieldDesc(boolean store, boolean tokenize) {
			super();
			this.store = store;
			this.tokenize = tokenize;
		}




		@Override
		public String toString() {
			return "FieldDesc [store=" + store + ", tokenize=" + tokenize + "]";
		}




		/**
		 * Parse fields as name:store[yes/no]:tokenize[yes/no]
		 *
		 * @param fieldsDesc
		 * @return
		 */
		public static final Map<String, FieldDesc> parseFieldsDesc(String fieldsDesc) {
			Map<String, FieldDesc> map = new HashMap<String, FieldDesc>();

			if (fieldsDesc != null) {
				for (String fieldpart : fieldsDesc.split(",")) {
					//name:true:true;
					String[] parts = fieldpart.split(":");
					if (parts.length > 3)
						throw new RuntimeException(
								"Field Description definition format error: "
										+ fieldsDesc
										+ " format must be, field:store[true/false]:tokenize[true/false]");

					if (parts.length == 3)

						map.put(parts[0],new FieldDesc(Boolean.parseBoolean(parts[1]),Boolean.parseBoolean(parts[2])));
					else
						map.put(parts[0],new FieldDesc(Boolean.parseBoolean(parts[1]),false));
				}
			}else{
				log.error("fieldsDesc 是 null ？？？？？？");
			}
			return map;
		}

	}

}
