package com.eurlanda.datashire.utility.objectsql;

import cn.com.jsoft.jframe.utils.BeanUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * 模板解析
 * @date 2014-1-17
 * @author jiwei.zhang
 *
 */
public class TemplateParser {
	private  Map<String,Object> env=new HashMap<String,Object>();
	

	public Map<String, Object> getParams() {
		return env;
	}
	public void setParams(Map<String, Object> env) {
		this.env = env;
	}
	
	public TemplateParser() {
		super();
	}
	public TemplateParser(Map<String, Object> paramsMap) {
		env = paramsMap;
	}
	/**
	 * 重置模板解析器,清除环境变量
	 * @date 2014-1-17
	 * @author jiwei.zhang
	 */
	public void reset(){
		env.clear();
	}
	/**
	 * 添加一个参数。
	 * @date 2014-1-17
	 * @author jiwei.zhang
	 * @param key
	 * @param value
	 */
	public void addParam(String key,String value){
		if(key!=null){
			env.put(key, value);
		}
	}
	/**
	 * 将bean里面的属性放入map中
	 * @date 2014-1-17
	 * @author jiwei.zhang
	 * @param bean
	 */
	public void addBeanParam(Object bean){
		this.env.putAll(BeanUtils.transBean2Map(bean));
	}
	/**
	 * 解析模板。
	 * @date 2014-1-13
	 * @author jiwei.zhang
	 * @param template
	 * @param env
	 * @return
	 */
	
	public String parseTemplate(String template){
		if(template==null) return null;
		Pattern patten = Pattern.compile("\\$\\{(.*?)\\}");
		Matcher matcher =patten.matcher(template);
		String ret =template;
		while(matcher.find()){
			String var = matcher.group(1);
			Object val = env.get(var);
			if(val==null) val="";
			ret  = ret.replace(matcher.group(), val.toString());
		}
		return ret;
	}
}
