package com.eurlanda.datashire.utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * 2014-12-10
 * @author Akachi
 * @E-Mail zsts@hotmail.com
 */
public class StringUtil {
	/**
	 * 是否包含
	 * 2014-12-11
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param toContains
	 * @param delimiter
	 * @return
	 */
	public static boolean contains(String toContains,String delimiter){
		if(toContains.indexOf(delimiter)!=-1){
			return true;
		}
		return false;
	}
	/**
	 * 拆分by字符串而非正则表达式
	 * 2014-12-10
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param toSplit
	 * @param delimiter
	 * @return
	 */
	public static String[] split(String toSplit, String delimiter) {
		if (!hasLength(toSplit) || !hasLength(delimiter)) {
			return null;
		}
		int offset = toSplit.indexOf(delimiter);
		if (offset < 0) {
			return new String[]{toSplit};
		}
		String beforeDelimiter = toSplit.substring(0, offset);
		String afterDelimiter = toSplit.substring(offset + delimiter.length());
		if(-1!=afterDelimiter.indexOf(delimiter)){
			String[] tos = split(afterDelimiter,  delimiter);
			String[] s = new String[tos.length+1];
			s[0]=beforeDelimiter;
			for (int i =1;i<=tos.length;i++) {
				s[i]=tos[i-1];
			}
			return s;
		}
		return new String[] {beforeDelimiter, afterDelimiter};
	}
	/**
	 * 替换
	 * 2014-12-10
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param toReplace
	 * @param displace
	 * @param value
	 * @return
	 */
	public static String replace(String toReplace,String displace,String value){
		String ret = "";
		String[] ss = split(toReplace,displace);
		for (int i=0;i<ss.length;i++) {
			if(i!=0){
				ret+=value+ss[i];
			}else{
				ret+=ss[i];
			}
		}
		return ret;
	}
	public static boolean hasLength(CharSequence str) {
		return (str != null && str.length() > 0);
	}

    /**
     * 获取开后结尾中的字符串
     * 2014-12-10
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     * @param source
     * @param begin
     * @param end
     * @return
     */
    public static String[] getParam(String source,String begin,String end){
    	String[] begins = StringUtil.split(source,begin);
    	if(begins==null||begins.length<=1){
    		return new String[0];
    	}
    	String[] param = new String[begins.length-1];
    	for (int i =0;i<begins.length-1;i++) {
    		String s = begins[i+1];
    		param[i]=split(s,end)[0];
    		if(param[i]==null){
    			param[i]="";
    		}
		}
    return param;	
    }

    private static void dd() {
		String sql = "a>'@@c' and c<@b or c<@@b or @c<10 or 'dd\\\"'' @c 22dd'+1<2 or 'dd@d22dd+1<2 or @d22dd asfa '+1<2";
		//sql = toSingleQuotes(sql);
		List<String> variableNames = new ArrayList<String>();
		System.out.println("1:"+sql);
		sql = ReplaceVariableForName(sql, variableNames);
		System.out.println("2:"+sql);
		System.out.println("list:"+variableNames.toString());
	}
    
    private static void ff() {
		String sql = " s_table_1 . column = 1 or s_table1_1.column = 2 or s_table_1. column = 3 or 's_table_1 .column =1' and s_table_1.column =2";
		//sql = toSingleQuotes(sql);
		System.out.println("1:"+sql);
		sql = ReplaceExpressionForParams(sql, "s_table_1", "s_xxx");
		System.out.println("2:"+sql);
	}

	// placeholder ?
	public static String ReplaceVariableForName(String str,List<String> variableNames){
		
		Map<String, Object> map = new HashMap<String, Object>();
		//提取字符串里面的内容 进行替换
		Pattern pattern = Pattern.compile("\\'[^\\']*\\'");
		Matcher m = pattern.matcher(" " + str + " ");
		StringBuffer sb = new StringBuffer();
		//使用find()方法查找第一个匹配的对象 
		boolean result = m.find();
		while (result) {
			String guid = StringUtils.generateGUID();
			m.appendReplacement(sb, guid);
			map.put(guid, m.group());
//			System.out.println("匹配后sb的内容是："+m.group());
			result = m.find();
		}
		m.appendTail(sb);
//		System.out.println("第一次的结果："+sb.toString());
		
		Pattern pattern1 = Pattern.compile("\\W@([_A-Za-z0-9]*)\\W");
		Matcher m1 = pattern1.matcher(sb.toString());
		StringBuffer sb1 = new StringBuffer();
		//使用find()方法查找第一个匹配的对象 
		boolean result1 = m1.find();
		while (result1) {
			String s1 = m1.group();
			if (!s1.contains("@@")){
				String key = s1.replaceAll("\\W", "");
				variableNames.add(key);
				s1 = s1.replace("@"+key, "?");
				m1.appendReplacement(sb1, s1);
				//System.out.println("匹配后sb1的内容是："+m.group());
			}
			result1 = m1.find();
		}
		m1.appendTail(sb1);
//		System.out.println(sb1.toString());
		String temp = sb1.toString();
		if(map!=null&&map.keySet().size()>0){
			for(String ss : map.keySet()){
				temp = temp.replace(ss, map.get(ss)+"");
			}
		}
		if (!StringUtils.isEmpty(temp)){
			temp = temp.substring(1);
			temp = temp.substring(0, temp.length()-1);
		}
		//System.out.println(sb.toString());
		return temp;
	}

	public static String ReplaceVariableForNameToJep(String str,List<String> variableNames){

		Map<String, Object> map = new HashMap<String, Object>();
		//提取字符串里面的内容 进行替换
		Pattern pattern = Pattern.compile("\\'[^\\']*\\'");
		Matcher m = pattern.matcher(" " + str + " ");
		StringBuffer sb = new StringBuffer();
		//使用find()方法查找第一个匹配的对象
		boolean result = m.find();
		while (result) {
			String guid = StringUtils.generateGUID();
			m.appendReplacement(sb, guid);
			map.put(guid, m.group());
//			System.out.println("匹配后sb的内容是："+m.group());
			result = m.find();
		}
		m.appendTail(sb);
//		System.out.println("第一次的结果："+sb.toString());

		Pattern pattern1 = Pattern.compile("\\W@([_A-Za-z0-9]*)\\W");
		Matcher m1 = pattern1.matcher(sb.toString());
		StringBuffer sb1 = new StringBuffer();
		//使用find()方法查找第一个匹配的对象
		boolean result1 = m1.find();
		while (result1) {
			String s1 = m1.group();
			if (!s1.contains("@@")){
				String key = s1.replaceAll("\\W", "");
				variableNames.add(key);
				s1 = s1.replace("@"+key, "\\$"+key);
				m1.appendReplacement(sb1, s1);
				//System.out.println("匹配后sb1的内容是："+m.group());
			}
			result1 = m1.find();
		}
		m1.appendTail(sb1);
//		System.out.println(sb1.toString());
		String temp = sb1.toString();
		if(map!=null&&map.keySet().size()>0){
			for(String ss : map.keySet()){
				temp = temp.replace(ss, map.get(ss)+"");
			}
		}
		if (!StringUtils.isEmpty(temp)){
			temp = temp.substring(1);
			temp = temp.substring(0, temp.length()-1);
		}
		//System.out.println(sb.toString());
		return temp;
	}

	public static String ReplaceExpressionForParams(String expression, String target, String replacement){
		
		Map<String, Object> map = new HashMap<String, Object>();
		//提取字符串里面的内容 进行替换
		Pattern pattern = Pattern.compile("\\'[^\\']*\\'");
		Matcher m = pattern.matcher(" " + expression + " ");
		StringBuffer sb = new StringBuffer();
		//使用find()方法查找第一个匹配的对象 
		boolean result = m.find();
		while (result) {
			String guid = StringUtils.generateGUID();
			m.appendReplacement(sb, guid);
			map.put(guid, m.group());
			System.out.println("匹配后sb的内容是："+m.group());
			result = m.find();
		}
		m.appendTail(sb);
		System.out.println("第一次的结果："+sb.toString());
		
		Pattern pattern1 = Pattern.compile(target+"(\\b|\\.)");
		Matcher m1 = pattern1.matcher(sb.toString());
		StringBuffer sb1 = new StringBuffer();
		//使用find()方法查找第一个匹配的对象 
		boolean result1 = m1.find();
		while (result1) {
			String guid = StringUtils.generateGUID();
			m1.appendReplacement(sb1, guid);
			if (m1.group().indexOf(".")==-1){
				map.put(guid, replacement);
			}else{
				map.put(guid, replacement+".");
			}
			System.out.println("匹配后sb1的内容是："+m1.group());
			result1 = m1.find();
		}
		m1.appendTail(sb1);
		System.out.println(sb1.toString());
		String temp = sb1.toString();
		if(map!=null&&map.keySet().size()>0){
			for(String ss : map.keySet()){
				temp = temp.replace(ss, map.get(ss)+"");
			}
		}
		if (!StringUtils.isEmpty(temp)){
			temp = temp.substring(1);
			temp = temp.substring(0, temp.length()-1);
		}
		return temp;
	}

	public static void main(String[] args) {
		/*String toReplace="\\sdffs\\sdfsddegse\\sgxxxx\\gegegee\\sdfesg";
		System.out.println(replace(toReplace,"\\","|"));*/

		ff();
	}
}
