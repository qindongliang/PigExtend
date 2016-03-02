package com.pig.support.elasticsearch;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * @author qindongliang
 * 支持ElasticSearch的EsOutputFormat
 * */
public class EsOutputFormat extends FileOutputFormat<Writable, IndexRequest> {

	static Logger log=LoggerFactory.getLogger(EsOutputFormat.class);
	  //es集群的地址，格式ip#ip#ip ，端号默认是9300
	  static  String address;
	  //es端口号
	  static int port=9300;
	  //es的集群名
	  static String clusterName;
	  //ip地址分隔符
	  static final String DELIMITER="#";

	public EsOutputFormat(String address,int port,String clusterName) {
		this.address = address;
		this.port=port;
		this.clusterName=clusterName;

	}

	@Override
	public RecordWriter<Writable, IndexRequest> getRecordWriter(
			TaskAttemptContext ctx) throws IOException, InterruptedException {
		return new EsRecordWriter(ctx);
	}


	@Override
	public synchronized OutputCommitter getOutputCommitter(
			TaskAttemptContext arg0) throws IOException {
		return new OutputCommitter(){

			@Override
			public void abortTask(TaskAttemptContext ctx) throws IOException {

			}

			@Override
			public void commitTask(TaskAttemptContext ctx) throws IOException {

			}

			@Override
			public boolean needsTaskCommit(TaskAttemptContext arg0)
					throws IOException {
				return true;
			}

			@Override
			public void setupJob(JobContext ctx) throws IOException {

			}

			@Override
			public void setupTask(TaskAttemptContext ctx) throws IOException {

			}


		};
	}


	/**
	 * Write out the LuceneIndex to a local temporary location.<br/>
	 * On commit/close the index is copied to the hdfs output directory.<br/>
	 *
	 */
	static class EsRecordWriter extends RecordWriter<Writable, IndexRequest> {
		//es的链接地址
		Client client;
		BulkProcessor bulkProcessor;//使用es的bulk
		TaskAttemptContext ctx;

		ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
		/**
		 * Opens and forces connect to EsServer
		 *
		 * @param ctx
		 */
		public EsRecordWriter(final TaskAttemptContext ctx) {
			try {
				this.ctx = ctx;
				Settings settings = ImmutableSettings.settingsBuilder()
						.put("cluster.name",clusterName)//设置集群名字
						.build();
				client=new TransportClient(settings);
				for(String ip:address.split(DELIMITER)){
					((TransportClient)client).addTransportAddress(new InetSocketTransportAddress(ip,port));
					log.info("添加ip地址：",ip);
				}
				initBulk();//初始化es连接
				exec.scheduleWithFixedDelay(new Runnable(){
					public void run(){
						ctx.progress();
					}
				}, 1000, 1000, TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			}
		}

		/***
		 * 初始化ElasticSearch的链接池处理
		 * @throws Exception
         */
		public void initBulk()throws  Exception{
			bulkProcessor=BulkProcessor.builder(client, new BulkProcessor.Listener() {
				@Override public void beforeBulk(long id, BulkRequest req) {
//				System.out.println("id: "+id+" req: "+req);//发送请求前，可以做一些事情
				}
				@Override public void afterBulk(long id, BulkRequest req, Throwable cause) {
					log.warn("fail request:  id: {}  req: {}  cause: {} ",id,req,cause.getMessage());//发送请求失败，可以做一些事情
				}
				@Override public void afterBulk(long id, BulkRequest req, BulkResponse rep) {
//			 System.out.println("id: "+id+"  req: "+req+"  rep: "+rep);//发送请求成功后，可以做一些事情
				}
			}).setBulkActions(20000)//达到批量3万请求处理一次
					.setBulkSize(new ByteSizeValue(100, ByteSizeUnit.MB))//达到20M批量处理一次
					.setConcurrentRequests(3)//设置多少个并发处理线程
					.setFlushInterval(TimeValue.timeValueSeconds(50))//设置flush索引周期
					.build();//构建BulkProcessor
		}



		/**
		 * On close we commit
		 */
		@Override
		public void close(final TaskAttemptContext ctx) throws IOException, InterruptedException {
			try {
				bulkProcessor.flush();//刷新到磁盘
				bulkProcessor.awaitClose(3,TimeUnit.MINUTES);//关闭连接资源
				log.info("job{}索引结束",ctx.getJobID().getId());
			} catch (Exception e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			} finally {
				client.close();//断开连接
				exec.shutdownNow();
			}

		}



		/**
		 * We add the indexed documents without commit
		 */
		@Override
		public void write(Writable key, IndexRequest doc)  throws IOException, InterruptedException {
			try {
			 	bulkProcessor.add(doc);
			} catch (Exception e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			}
		}

	}
}
