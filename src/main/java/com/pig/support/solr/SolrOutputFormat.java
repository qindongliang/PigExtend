package com.pig.support.solr;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author qindongliang
 * 支持SOlr的SolrOutputFormat
 * */
public class SolrOutputFormat extends FileOutputFormat<Writable, SolrInputDocument> {

	static Logger log=LoggerFactory.getLogger(SolrOutputFormat.class);
	//solr的url地址
	final String address;

	public SolrOutputFormat(String address) {
		this.address = address;
	}

	@Override
	public RecordWriter<Writable, SolrInputDocument> getRecordWriter(
			TaskAttemptContext ctx) throws IOException, InterruptedException {
		return new SolrRecordWriter(ctx, address);
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
	static class SolrRecordWriter extends RecordWriter<Writable, SolrInputDocument> {
		/**Solr的地址*/
//		ConcurrentUpdateSolrClient server;

	   SolrClient server;


		/**批处理提交的数量**/
		int batch = 10000;

		TaskAttemptContext ctx;

		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(batch);
		ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
		/**
		 * Opens and forces connect to CloudSolrServer
		 *
		 * @param address
		 */
		public SolrRecordWriter(final TaskAttemptContext ctx, String address) {
			try {
				this.ctx = ctx;
				server = new ConcurrentUpdateSolrClient(address, 10000, 10);
//				server=new HttpSolrClient(address);
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


		/**
		 * On close we commit
		 */
		@Override
		public void close(final TaskAttemptContext ctx) throws IOException,
				InterruptedException {

			try {

				if (docs.size() > 0) {
					server.add(docs);
					docs.clear();
				}

				server.commit();
			} catch (SolrServerException e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			} finally {
				server.close();
				exec.shutdownNow();
			}

		}

		/**
		 * We add the indexed documents without commit
		 */
		@Override
		public void write(Writable key, SolrInputDocument doc)
				throws IOException, InterruptedException {
			try {
				docs.add(doc);
				if (docs.size() >= batch) {
					server.add(docs);
					docs.clear();
				}
			} catch (SolrServerException e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			}
		}

	}
}
