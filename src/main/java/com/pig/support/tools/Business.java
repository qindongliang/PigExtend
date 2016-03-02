package com.pig.support.tools;
/****
 * 业务定义的枚举的集合
 * @author qindongliang
 *
 */
public enum Business {
	
	CPY("cpy"),//企业信息业务
	WEBPAGE("webpage");//网页信息业务
	public final String value;

	public String getValue() {
		return value;
	}

	private Business(String value) {
		this.value=value;
	}
	
	/****
	 * 根据一个值获取该值
	 * @param value
	 * @return
	 */
	public static Business getBusiness(String value){
		  for(Business color : Business.values()){
	            if(color.value.equals(value)){
	            	return color;
	            }
	        }
		  return null;
	}
	
	
	
	

}
