package com.example.esdemo.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.List;


/**
 * 
 * @ClassName: JsonUtils 
 * @Description: json转换工具
 * @author shanlong 
 * @date 2015年9月17日 下午4:25:32
 */
public class JsonUtils {
	/**
	 * 
	 * @Title: toJson    
	 * @Description: json对象转字符串
	 * @param bean 对象
	 * @return       
	 * String        
	 * @throws
	 */
	public static String toJson(Object bean) {
		return JSON.toJSONString(bean);
	}
	/**
	 * 
	 * @Title: toJson    
	 * @Description: 根据数据字符串和key截取字符串
	 * @param data 字符串数据
	 * @param key 键
	 * @return       
	 * String        
	 * @throws
	 */
	public static  String toJson(String data,String key){
		 JSONObject object = JSON.parseObject(data);  
		 return object.get(key)+"";
	}
	
	/**
	 * 
	 * @Title: toObject    
	 * @Description: 字符串转json
	 * @param data 字符串数据
	 * @param clazz 类型
	 * @return       
	 * T        
	 * @throws
	 */
	public static  <T> T toObject(String data,Class<T> clazz){
		return JSON.parseObject(data,clazz);
	}
	/**
	 * 
	 * @Title: toArray    
	 * @Description: 字符串转换为json对象数组
	 * @param data 数据字符串
	 * @param clazz 对象类型
	 * @return       
	 * List<T>        
	 * @throws
	 */
	public static  <T> List<T> toArray(String data,Class<T> clazz){
		return JSON.parseArray(data,clazz);  
	}
	
	
	
}
