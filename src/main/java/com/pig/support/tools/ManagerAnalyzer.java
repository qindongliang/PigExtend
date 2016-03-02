package com.pig.support.tools;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.lucene.IKAnalyzer;
import org.wltea.analyzer.lucene.IKHtmlAnalyzer;

import com.keyword.regex.KwPatternAnalyzer;
import com.keyword.regex.PatternAnalyzer;
import com.simple.pinyin.PYAnalyzer;

/***
 * 管理多分词器的中心配置
 * 针对Lucene索引，需要做处理,其他的都在服务端分词不需要
 * @author qindongliang
 *
 */
public class ManagerAnalyzer {

	static Logger log=LoggerFactory.getLogger(ManagerAnalyzer.class);

	private ManagerAnalyzer() {
		// TODO Auto-generated constructor stub
	}
	private static PerFieldAnalyzerWrapper pers=null;


	/***
	 *
	 * @param business 根据枚举选择合适的分词器组合实例
	 * @return
	 */
	public static PerFieldAnalyzerWrapper getAnalyzerByBusiness(Business business){
		log.info("在ManagerAnalyzer里，传过来的分词器类型 ： {} ",business.value);
		PerFieldAnalyzerWrapper analyzer=null;
		switch (business) {
		case  CPY:
			analyzer=getAnalyzerInstance_Cpy();
			log.info("进入cpy选择！");
			break;
		case  WEBPAGE:
			analyzer=getAnalyzerInstance_WebPage();
			log.info("进入webpage选择！");
			break;
		default:
			log.info("进入默认？？？");
			break;
		}
		return analyzer;
	}


	/***
	 * 针对定制企业信息的业务
	 * 定制的分词器处理
	 * **/
	 static  PerFieldAnalyzerWrapper getAnalyzerInstance_Cpy(){
		if(pers==null){
		    Map<String, Analyzer> 	analyzers=new HashMap<String, Analyzer>();
			IKAnalyzer ik=new IKAnalyzer(false);//分词的公司名
			PYAnalyzer py=new PYAnalyzer("jpone");//拼音
			KwPatternAnalyzer kw=new KwPatternAnalyzer();//不分词的公司名
			PatternAnalyzer pa=new PatternAnalyzer("#");//对多值域处理的一种方式
			analyzers.put("cpyName", ik);
			analyzers.put("sname", kw);
			analyzers.put("cpyNamePy", py);
			analyzers.put("cpyNatureCode", pa);
			pers=new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),analyzers);
		}
		return pers;
	}

	/***
	 * 针对定制网页的业务
	 * 定制的分词器处理
	 * **/
	 static  PerFieldAnalyzerWrapper getAnalyzerInstance_WebPage(){
		if(pers==null){
		    Map<String, Analyzer> 	analyzers=new HashMap<String, Analyzer>();
			IKAnalyzer title=new IKAnalyzer(true);//标题
			IKHtmlAnalyzer content=new IKHtmlAnalyzer(true);//正文
			analyzers.put("title", title);
			analyzers.put("content", content);
			pers=new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),analyzers);
		}
		return pers;
	}




}
