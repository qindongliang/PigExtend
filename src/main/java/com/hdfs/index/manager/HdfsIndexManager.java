//package com.hdfs.index.manager;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.atomic.AtomicLong;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.lucene.index.DirectoryReader;
//import org.apache.lucene.index.IndexWriter;
//import org.apache.lucene.index.IndexWriterConfig;
//import org.apache.lucene.index.IndexWriterConfig.OpenMode;
//import org.apache.solr.store.hdfs.HdfsDirectory;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
///**
// * HDFS索引管理类，作为扩展，以后可能用到
// *
// * **/
//public class HdfsIndexManager {
//
//	static Logger log=LoggerFactory.getLogger(HdfsIndexManager.class);
//
//	static Configuration conf=new  Configuration();
//	static FileSystem fs=null;
//	static{
//
//	try {
//		checkWintils();//监测winutils.exe 文件
//	    //默认的hadoop的fs.defaultFS的端口号为8020，这里需要跟集群里面的配置一致
//	    conf.set("fs.defaultFS","hdfs://h1:8020/");
//	    fs=FileSystem.get(conf);
//	} catch (Exception e) {
//	    log.error("初始化FileSystem失败！",e);
//	}
//	}
//
//	/**
//	 * 获取一个conf
//	 * **/
//	public static Configuration getConf(){
//		Configuration conf=new  Configuration();
//		conf.set("fs.defaultFS","hdfs://10.171.98.238:8020/");
//		return conf;
//	}
//
//
//	static void checkWintils()throws Exception{
//		if (System.getProperty("os.name").contains("Windows")) {
//            File workaround = new File(".");
//            System.getProperties().put("hadoop.home.dir",workaround.getAbsolutePath());
//            File dir = new File("./bin");
//            if (!dir.exists()) {
//                dir.mkdirs();
//            }
//            File exe = new File("./bin/winutils.exe");
//            if (!exe.exists()) {
//                exe.createNewFile();
//            }
//        }
//	}
//
//	public static List<String> ll(String path)throws Exception{
//
//		List<String> paths=new ArrayList<String>();
//		//获取globStatus
//		FileStatus[] status = fs.globStatus(new Path(path));
//		for(FileStatus f:status){
//			System.out.println(f.getPath().toString());
//		    paths.add(f.getPath().toString());
//		}
//		return paths;
//	}
//
//
//	static AtomicLong count=new AtomicLong();
//	public static void main(String[] args) throws Exception{
//		String path="/user/webmaster/pig/lucene_index_company2/index*";
////		ll("/user/webmaster/pig/lucene_index_company2/index*");
////		opearHdfsIndex();
//
////		showIndexCount(ll(path));
//		String OneIndexDir="hdfs://h1:8020/user/webmaster/pig/total_index";
//		mergeIndex(1, ll(path), OneIndexDir);
//	}
//
//	public static void opearHdfsIndex()throws Exception{
//
//
//
//
//	}
//
//
//	public static  void showIndexCount(List<String> paths)throws Exception{
//		HdfsDirectory directory=null;
//		DirectoryReader  reader=null;
//		for(String path:paths){
//
//		 directory=new HdfsDirectory(new Path(path),conf);
////		 IndexWriter iw=new IndexWriter(directory, new IndexWriterConfig(null));
//		 reader=DirectoryReader.open(directory);
//		 System.out.println("路径："+path+"   "+"索引数量："+reader.numDocs());
//		 System.out.println("索引总数量： "+count.addAndGet(reader.numDocs()));
//		}
//		reader.close();
//		directory.close();
//	}
//
//	/***
//	 * 合并多个小索引成一份大的索引
//	 * @param sengments_count 合并段的数量
//	 * @param dir	生成的总的索引目录
//	 * @param childs 多个子索引目录
//	 */
//	public static void mergeIndex(int sengments_count,List<String> childs,String dir)throws Exception{
//
//		Path path=new Path(dir);//目标目录
////		//判断目录是否存在，存在的话，先删除
////		if(fs.exists(path)){
////			fs.delete(path,true);
////			System.out.println("目录"+dir+"存在，已删除！");
////		}
////
//		System.out.println("合并索引开始.................");
//		long start=System.currentTimeMillis();
//		//合并后的索引目录
//		HdfsDirectory directory=new HdfsDirectory(path, conf);
//		//索引配置项
//		IndexWriterConfig config=new IndexWriterConfig(null);
//		config.setOpenMode(OpenMode.CREATE);
//		//索引写入流
//		IndexWriter writer=new IndexWriter(directory,config);
//		//所有的子索引存储数组
////		HdfsDirectory[] indexs=new HdfsDirectory[childs.size()];
////		for(int i=0;i<childs.size();i++){
////			HdfsDirectory childDirectory=new HdfsDirectory(new Path(childs.get(i)), getConf());
////			indexs[i]=childDirectory;//给数组赋值
////		}
//
//		//合并所有的子索引
////		writer.addIndexes(new HdfsDirectory(new Path(childs.get(0)),getConf()));
//		//压缩段的数量
////		writer.forceMerge(sengments_count);;
////		DirectoryReader reader=DirectoryReader.open(directory);
////		long count=reader.numDocs();
////		reader.close();//关闭读取读取流
//
////		writer.close();//关闭写入流
////		directory.close();//关闭资源
////		long end=System.currentTimeMillis();
////		System.out.println("本次压缩"+childs.size()+"个索引目录 ，共： "+count+"条记录 ,总耗时："+(end-start)/1000/60+" 秒！ ");
//
//	}
//
//
//
//}
