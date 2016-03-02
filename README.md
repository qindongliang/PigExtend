# PigExtend
Apache Pig+MapReduce给Lucene/Solr/ElasticSearch构建索引

####项目简介
主要是利用了Pig框架简化了自己写Hadoop MapReduce程序来构建大规模并行索引的问题，里面封装了主流的全文检索框架，如Lucene，Solr和ElasticSearch
并且支持SolrCloud集群和ElasticSearch集群的分布式索引构建。
这个框架里面通过Pig的UDF函数封装了Store方法，只要Pig能读的任何数据源，我们都可以通过Store函数，将结果轻而易举的存储到我们的索引里面，

####使用步骤
（1）下载源码后，根据自己的业务情况，稍作修改，然后重新build，成一个jar包 <br/>
（2）上传至pig脚本同级目录<br/>
（3）在脚本注册jar包，方便在MR程序运行时，能够加载到相关的依赖<br/>



####如何给Lucene构建索引
````pig
--设置任务名
set job.name 'build-index-lucene' ;
--新建目录并删除目录防止报错
mkdir /user/webmaster/search/monitor/index-data;
rmf  /user/webmaster/search/monitor/index-data;
--注册UDF的jar包
REGISTER ./pig-udf-extend-1.0-SNAPSHOT-jar-with-dependencies.jar;
--lucene格式配置，1，是否存储，2是否分词
--LuceneStore(1=isStore,2=isTokenize)
--加载一个数据源从HDFS上
a = load '/user/webmaster/crawldb/monitor/' using PigStorage('\\u001')  as (rowkey:chararray,title:chararray,content:chararray,isdel:chararray,t1:chararray,t2:chararra
y,t3:chararray,dtime:long)  ;
--使用pig做一些过滤清洗
a = filter a by isdel is null or isdel == ''  ;
--构建索引并存储到指定目录
store a into  '/user/webmaster/search/monitor/index-data'  using com.pig.support.lucene.LuceneStore('rowkey:true:false,title:false:true,content:false:true,isdel:true:f
alse,dtime:false:false,t1:false:false,t2:false:false,t3:false:false','webpage');
````

####如何给SolrCloud集群构建索引，单节点solr请使用SolrStore与SolrCloudStore大同小异
````pig
--设置job名
set job.name 'build-index-solrcloud' ;
--新建目录并删除目录防止报错
mkdir /user/webmaster/search/monitor/index-data;
rmf  /user/webmaster/search/monitor/index-data;

--注册依赖的jar包
REGISTER ./pig-udf-extend-1.0-SNAPSHOT-jar-with-dependencies.jar;
REGISTER ./httpclient-4.4.1.jar;
REGISTER ./httpcore-4.4.1.jar;

--加载数据
a = load '/user/webmaster/crawldb/monitor/' using PigStorage('\\u001')  as (rowkey:chararray,title:chararray,content:chararray,isdel:chararray,t1:chararray,t2:chararra
y,t3:chararray,dtime:long)  ;
--etl过滤
a = filter a by isdel is null or isdel == ''  ;
--参数分别是ip地址，collection名字，和批处理提交的数量
store a into '/user/webmaster/search/monitor/index-data' using com.pig.support.solrcloud.SolrCloudStore('192.168.1.187:2181,192.168.1.184:2181,192.168.1.186:2181','big
_search','20000');
````
####如何给ElasticSearch集群构建索引
````pig
--设置job名
set job.name 'rebuild-index-elasticsearch' ;
--新建目录并删除目录防止报错
mkdir /user/webmaster/search/es/index-data;
rmf  /user/webmaster/search/es/index-data;
--注册依赖jar
REGISTER ./pig-udf-extend-1.0-SNAPSHOT-jar-with-dependencies.jar;
--加载依赖数据
a = load '/user/webmaster/crawldb/monitor' using PigStorage('\\u001')  as (rowkey:chararray,mtitle:chararray,mcontent:chararray,isdel:chararray,t1:chararray,t2:chararr
ay,t3:chararray,dtime:long)  ;

--存储索引到ElasticSearch，构造参数解释：
--1,集群ip
--2,索引名
--3,类型名
--4,端口号
--5,集群名
store a into  '/user/webmaster/search/es/index-data'  using com.pig.support.elasticsearch.EsStore('192.168.1.187#192.168.1.186#192.168.1.184','Index','type',9300,'search');
````
####温馨提示
本项目主要所用技术有关pig,lucene,solr或者elasticsearch集群的基本知识，如有不不熟悉者，
可以先从散仙的博客入门一下：
[我的Iteye博客](http://qindongliang.iteye.com/) <br/>


#### QQ搜索技术交流群：206247899   公众号：我是攻城师（woshigcs） 如有问题，可在后台留言咨询
