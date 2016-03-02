package com.ananlyzer.test;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Test;
import org.wltea.analyzer.lucene.IKAnalyzer;
import org.wltea.analyzer.lucene.IKHtmlAnalyzer;
import org.wltea.analyzer.lucene.SynIKAnalyzer;

public class AnalyzerTest {

	@Test
	public  void start()throws Exception {

//		IKAnalyzer ik=new IKAnalyzer(true);
		//analyzer(ik);
		SynIKAnalyzer ik=new SynIKAnalyzer(true);
		//KwPatternAnalyzer  ik=new KwPatternAnalyzer();
//		IKHtmlAnalyzer ik=new IKHtmlAnalyzer();
//		StandardAnalyzer ik=new StandardAnalyzer();
		String text="中国公司的产品还不错";
		analyzer(ik,text);


	}

	/***
	 *
	 * @param analyzer 分词器
	 * @param text  分词句子
	 * @throws Exception
	 */
	public static void analyzer(Analyzer analyzer,String text)throws Exception{
		        TokenStream ts = analyzer.tokenStream("name",text);
		        CharTermAttribute term=ts.addAttribute(CharTermAttribute.class);
		        ts.reset();
		        while(ts.incrementToken()){
		            System.out.println(term.toString());
		        }
		        ts.end();
		        ts.close();
	}


}
