package com.eurlanda.datashire.utility;

import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.entity.ReportSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.InfoMessagePacket;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.packet.ListMessagePacket;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import net.sf.json.util.PropertyFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.*;

/**
 * 
 * <p>
 * Title :
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :何科敏 Aug 26, 2013
 * </p>
 * <p>
 * update :何科敏 Aug 26, 2013
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class JsonUtil {

    private static Logger logger = LoggerFactory.getLogger(JsonUtil.class);

    private static final JsonConfig jsonConfig;

	/***
	 * 将List对象序列化为JSON文本
	 */
	public static <T> String toJSONString(List<T> list) {
		String data = JSON.toJSONString(list);
		return data;
	}

	/***
	 * 将对象序列化为JSON文本
	 * 
	 *
	 * @param returnMap
	 * @param user
	 *@param object  @return
	 */
	public static String toJSONString(Map<String, Object> returnMap, DSObjectType user, Object object) {
		JSONArray jsonArray = JSONArray.fromObject(object);
		return jsonArray.toString();
	}

	/***
	 * 将JSON对象数组序列化为JSON文本
	 * 
	 * @param jsonArray
	 * @return
	 */
	public static String toJSONString(JSONArray jsonArray) {
		return jsonArray.toString();
	}

	/***
	 * 将JSON对象序列化为JSON文本
	 * 
	 * @param jsonObject
	 * @return
	 */
	public static String toJSONString(JSONObject jsonObject) {
		return jsonObject.toString();
	}

	/***
	 * 将对象转换为List对象
	 * 
	 * @param object
	 * @return
	 */
	public static List toArrayList(Object object) {
		List arrayList = new ArrayList();

		JSONArray jsonArray = JSONArray.fromObject(object);

		Iterator it = jsonArray.iterator();
		while (it.hasNext()) {
			JSONObject jsonObject = (JSONObject) it.next();
			Iterator keys = jsonObject.keys();
			while (keys.hasNext()) {
				Object key = keys.next();
				Object value = jsonObject.get(key);
				arrayList.add(value);
			}
		}

		return arrayList;
	}

	/***
	 * 将对象转换为Collection对象
	 * 
	 * @param object
	 * @return
	 */
	public static Collection toCollection(Object object) {
		JSONArray jsonArray = JSONArray.fromObject(object);

		return JSONArray.toCollection(jsonArray);
	}

	/***
	 * 将对象转换为JSON对象数组
	 * 
	 * @param object
	 * @return
	 */
	public static JSONArray toJSONArray(Object object) {
		return JSONArray.fromObject(object);
	}

	/***
	 * 将对象转换为JSON对象
	 * 
	 * @param object
	 * @return
	 */
	public static JSONObject toJSONObject(Object object) {
		return JSONObject.fromObject(object);
	}

	/***
	 * 将对象转换为HashMap
	 * 
	 * @param object
	 * @return
	 */
	public static HashMap toHashMap(Object object) {
		HashMap<String, Object> data = new HashMap<String, Object>();
		JSONObject jsonObject = JsonUtil.toJSONObject(object);
		Iterator it = jsonObject.keys();
		while (it.hasNext()) {
			String key = String.valueOf(it.next());
			Object value = jsonObject.get(key);
			if (value instanceof JSONObject){
				data.put(key, jsonObject.getJSONObject(key));
			}else{
				data.put(key, value);
			}
		}
		return data;
	}
	
	/***
	 * 将对象转换为HashMap
	 * 
	 * @param object
	 * @return
	 */
	public static HashMap toHashMap(String object) {
		HashMap<String, Object> data = new HashMap<String, Object>();
		com.alibaba.fastjson.JSONObject jsonObject = com.alibaba.fastjson.JSONObject.parseObject(object);
		for (String key : jsonObject.keySet()){
			Object value = jsonObject.get(key);
			if (value instanceof JSONObject){
				data.put(key, jsonObject.getJSONObject(key));
			}else{
				data.put(key, value);
			}
		}
		return data;
	}

	/***
	 * 将对象转换为List<Map<String,Object>>
	 * 
	 * @param object
	 * @return
	 */
	// 返回非实体类型(Map<String,Object>)的List
	public static List<Map<String, Object>> toList(Object object) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			JSONArray jsonArray = JSONArray.fromObject(object);
			for (Object obj : jsonArray) {
				JSONObject jsonObject = (JSONObject) obj;
				Map<String, Object> map = new HashMap<String, Object>();
				Iterator it = jsonObject.keys();
				while (it.hasNext()) {
					String key = (String) it.next();
					Object value = jsonObject.get(key);
					map.put((String) key, value);
				}
				list.add(map);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/***
	 * 将JSON对象数组转换为传入类型的List
	 * 
	 * @param <T>
	 * @param jsonArray
	 * @param objectClass
	 * @return
	 */
	public static <T> List<T> toList(JSONArray jsonArray, Class<T> objectClass) {
		return JSONArray.toList(jsonArray, objectClass);
	}

	/***
	 * 将对象转换为传入类型的List
	 * 
	 * @param <T>
	 * @param object
	 * @param objectClass
	 * @return
	 */
	public static <T> List<T> toList(Object object, Class<T> objectClass) {
		JSONArray jsonArray = JSONArray.fromObject(object);
		return JSONArray.toList(jsonArray, objectClass);
	}

	/***
	 * 将JSON对象转换为传入类型的对象
	 * 
	 * @param <T>
	 * @param jsonObject
	 * @param beanClass
	 * @return
	 */
	public static <T> T toBean(JSONObject jsonObject, Class<T> beanClass) {
		return (T) JSONObject.toBean(jsonObject, beanClass);
	}

	/***
	 * 将对象转换为传入类型的对象
	 * 
	 * @param <T>
	 * @param object
	 * @param beanClass
	 * @return
	 */
	public static <T> T toBean(Object object, Class<T> beanClass) {
		JSONObject jsonObject = JSONObject.fromObject(object);
		return (T) JSONObject.toBean(jsonObject, beanClass);
	}

	/***
	 * 将JSON文本反序列化为主从关系的实体
	 * 
	 * @param <T>
	 *            主实体类型
	 * @param jsonString
	 *            JSON文本
	 * @param mainClass
	 *            主实体类型
	 * @param detailClass
	 *            存放了多个从实体在主实体中属性名称和类型
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T toBean(String jsonString, Class<T> mainClass, Map<String, Class> detailClass) {
		try {
			JSONObject jsonObject = JSONObject.fromObject(jsonString);
			return (T)JSONObject.toBean(jsonObject, mainClass, detailClass);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 解析ID
	 * 
	 * @param cond
	 * @return
	 */
	public static Integer getId(String cond) {
		// json解析对象
		try {
			JSONObject json = JSONObject.fromObject(cond); 
			String conds = json.getString("id");
			return Integer.parseInt(conds);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * <p>
	 * 作用描述：根据串直接反序列化对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param <T>
	 *@param jsonString
	 *@param mainClass
	 *@return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T toBean(String jsonString, Class<T> mainClass) {
		try {
			JSONObject jsonObject = JSONObject.fromObject(jsonString);
			return (T)JSONObject.toBean(jsonObject, mainClass);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * <p>
	 * 作用描述：根据串直接反序列化对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param <T>
	 *@param jsonString
	 *@param mainClass
	 *@return
	 */
	public static <T> T  toGsonBean(String jsonString, Class<T> mainClass){
//		Gson gson = new GsonBuilder()
//	    // .registerTypeAdapter(Id.class, new IdTypeAdapter())
//	     .enableComplexMapKeySerialization()
//	   //  .serializeNulls()
//	     //.setDateFormat(DateFormat.LONG)
//	     .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
//	     //.setPrettyPrinting()	   //格式化输出  
//	     .setVersion(1.0)
//	     .create();
//		  T object = gson.fromJson( jsonString , mainClass); 
		  //T object = gson.fromJson( jsonString , mainClass); 
		  T object = JSON.parseObject(jsonString, mainClass);
		  return object;
		
	}
	public static <T> T  toGsonBean22(String jsonString, Class<T> mainClass){
		Gson gson = new GsonBuilder()
	    // .registerTypeAdapter(Id.class, new IdTypeAdapter())
	     .enableComplexMapKeySerialization()
	   //  .serializeNulls()
	     //.setDateFormat(DateFormat.LONG)
	     .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
	     //.setPrettyPrinting()	   //格式化输出  
	     .setVersion(1.0)
	     .create();
//		  T object = gson.fromJson( jsonString , mainClass); 
		  T object = gson.fromJson( jsonString , mainClass); 
		
		  return object;
		
	}
	public static String toGsonString(Object object) {
		Gson gson = new GsonBuilder()
	    // .registerTypeAdapter(Id.class, new IdTypeAdapter())
	     .enableComplexMapKeySerialization()
	    // .serializeNulls()
	     //.setDateFormat(DateFormat.LONG)
	     .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
	     //.setPrettyPrinting()	   //格式化输出  
	     .setVersion(1.0).disableHtmlEscaping()
	     .create();
		
		//JSONArray jsonArray = JSONArray.fromObject(object);
		return gson.toJson(object);
	}
	/***
	 * 将List对象序列化为JSON文本
	 */
	public static <T>List<T>  toGsonList(String list,Class<T> mainClass) {
/*		//TODO
		Gson gson = new GsonBuilder()
	    // .registerTypeAdapter(Id.class, new IdTypeAdapter())
	     .enableComplexMapKeySerialization()
	   //  .serializeNulls()
	     //.setDateFormat(DateFormat.LONG)
	     .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
	     //.setPrettyPrinting()	   //格式化输出  
	     .setVersion(1.0)
	     .create();
	    JsonParser parser = new JsonParser(); 
	    JsonArray Jarray = parser.parse(list).getAsJsonArray(); 

	    ArrayList<T> lcs = new ArrayList<T>(); 

	    for(JsonElement obj : Jarray ){ 
	        T cse = gson.fromJson( obj , mainClass); 
	        lcs.add(cse); 
	    }*/
	    // updated by bo.dang
		return JSON.parseArray(list, mainClass);
	}
	
	/** 将JSON串反序列化为java对象(add by dang.lu 2013.9.13) */
	public static <T> T json2Object(String jsonStr,Class<T> mainClass) {
		return StringUtils.isNull(jsonStr)?null:new GsonBuilder()
	     .enableComplexMapKeySerialization()
	     //.serializeNulls()
	     .setVersion(1.0)
	     .create().fromJson(jsonStr , mainClass);
	}
	/** 将java对象序列化为JSON串(add by dang.lu 2013.9.13) */
	public static String object2Json(Object obj) {
		//设置disbaleHtmlEscaping主要是防止一些特殊字符被转义
		return obj==null?null:new GsonBuilder()
	     .enableComplexMapKeySerialization()
	     .excludeFieldsWithModifiers(Modifier.TRANSIENT)
				.disableHtmlEscaping()
	     .setDateFormat("yyyy-MM-dd HH:mm:ss")
	     .setVersion(1.0)
	     .create().toJson(obj);
	}

	/**
	 * 转换成json，允许为空
	 * @param obj
	 * @return
	 */
	public static String object2JsonWitnNull(Object obj){
		return obj==null?null:new GsonBuilder()
				.enableComplexMapKeySerialization()
				.serializeNulls()
				.excludeFieldsWithModifiers(Modifier.TRANSIENT)
				.disableHtmlEscaping()
				.setDateFormat("yyyy-MM-dd HH:mm:ss")
				.setVersion(1.0)
				.create().toJson(obj);
	}
	public static String object2FmtJson(Object obj) {
		return obj==null?null:new GsonBuilder()
	     .enableComplexMapKeySerialization()
	     .excludeFieldsWithModifiers(Modifier.TRANSIENT)
	    // .serializeNulls()
	     .setVersion(1.0)
	     .setPrettyPrinting() // 格式化
	     .create().toJson(obj);
	}
	
	/** 将JSON串反序列化为java对象(add by dang.lu 2013.9.13) */
	public static <T> T json2Object(String jsonStr,Class<T> mainClass, FieldNamingPolicy p) {
		return StringUtils.isNull(jsonStr)?null:new GsonBuilder()
	     .enableComplexMapKeySerialization()
	     //.serializeNulls()
	     .setFieldNamingPolicy(p)
	     .setVersion(1.0)
	     .create().fromJson(jsonStr , mainClass);
	}
	/** 将java对象序列化为JSON串(add by dang.lu 2013.9.13) */
	public static String object2Json(Object obj, FieldNamingPolicy p) {
		return obj==null?null:new GsonBuilder()
	     .enableComplexMapKeySerialization()
	     .excludeFieldsWithModifiers(Modifier.TRANSIENT)
	     //.addDeserializationExclusionStrategy(new SuperclassExclusionStrategy())
	     //.addSerializationExclusionStrategy(new SuperclassExclusionStrategy())
	     //.serializeNulls()
	     .setFieldNamingPolicy(p)
	     .setVersion(1.0)
	     .create().toJson(obj);
	}
	
	public static final String toString(List obj, DSObjectType type, MessageCode out){
		if(out==null||out!=MessageCode.SUCCESS){
			logger.warn(MessageFormat.format("Service Execute Failed! (result-size = {0}, msg-code = {1}, msg-info = {2})", obj==null?-1:obj.size(), out==null?-1:out.value(), out));
		}
		ListMessagePacket listMessage = new ListMessagePacket();
		listMessage.setCode(out.value());
		listMessage.setDataList(obj);
		listMessage.setType(type);
//		return JSONObject.fromObject(listMessage, jsonConfig).toString();
		return JSON.toJSONString(listMessage);
		//return JsonUtil.object2Json(listMessage);
	}
	
	public static final String toString(List obj, DSObjectType type, ReturnValue out){
		return toString(obj, type, out.getMessageCode());
	}
	
	public static final String toString(Object obj, DSObjectType type, MessageCode out){
		if(out==null||out!=MessageCode.SUCCESS){
			logger.warn(MessageFormat.format("Service Execute Failed! (msg-code = {0}, msg-info = {1})", out==null?-1:out.value(), out));
		}
		InfoMessagePacket infoMessagePacket = new InfoMessagePacket();
		infoMessagePacket.setCode(out.value());
		infoMessagePacket.setInfo(obj);
		infoMessagePacket.setType(type);
		return JSONObject.fromObject(infoMessagePacket, jsonConfig).toString();
		//return JsonUtil.object2Json(infoMessagePacket);
	}
	
	@SuppressWarnings({ "rawtypes"})
	public static final String toJsonString(Object obj, DSObjectType type, MessageCode out){
		if(out==null||out!=MessageCode.SUCCESS){
			logger.warn(MessageFormat.format("Service Execute Failed! (msg-code = {0}, msg-info = {1})", out==null?-1:out.value(), out));
		}
		InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket();
		infoNewMessagePacket.setCode(out.value());
		/*if (obj instanceof HashMap){
			Map<String, Object> map = (HashMap<String, Object>)obj;
			if (map.size()>0){
				String objStr = toGsonString(obj);
				infoNewMessagePacket.setInfo(objStr);
			}
		}*/
		//String objStr = toGsonString(obj);
		infoNewMessagePacket.setInfo(obj);
		infoNewMessagePacket.setType(type);
		//if (infoNewMessagePacket.getDesc()==null) infoNewMessagePacket.setDesc("");
		//if (infoNewMessagePacket.getToken()==null) infoNewMessagePacket.setToken("");
		//logger.info(JsonUtil.object2Json(infoNewMessagePacket));
		//return JSONObject.fromObject(infoNewMessagePacket, jsonConfig).toString();
		String str = JsonUtil.object2Json(infoNewMessagePacket);
		return str;
	}

	/**
	 * 允许空值
	 * @param obj
	 * @param type
	 * @param out
	 * @return
	 */
	public static final String toJsonStringWithNull(Object obj, DSObjectType type, MessageCode out){
		if(out==null||out!=MessageCode.SUCCESS){
			logger.warn(MessageFormat.format("Service Execute Failed! (msg-code = {0}, msg-info = {1})", out==null?-1:out.value(), out));
		}
		InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket();
		infoNewMessagePacket.setCode(out.value());
		infoNewMessagePacket.setInfo(obj);
		infoNewMessagePacket.setType(type);
		return JsonUtil.object2JsonWitnNull(infoNewMessagePacket);
	}
	@SuppressWarnings({ "rawtypes"})
	public static final String toJsonString(Object obj, DSObjectType type, MessageCode out, int tt){
		if(out==null||out!=MessageCode.SUCCESS){
			logger.warn(MessageFormat.format("Service Execute Failed! (msg-code = {0}, msg-info = {1})", out==null?-1:out.value(), out));
		}
		InfoNewMessagePacket infoNewMessagePacket = new InfoNewMessagePacket();
		infoNewMessagePacket.setCode(out.value());
		/*if (obj instanceof HashMap){
			Map<String, Object> map = (HashMap<String, Object>)obj;
			if (map.size()>0){
				String objStr = toGsonString(obj);
				infoNewMessagePacket.setInfo(objStr);
			}
		}*/
		//String objStr = toGsonString(obj);
		infoNewMessagePacket.setInfo(obj);
		infoNewMessagePacket.setType(type);
		//if (infoNewMessagePacket.getDesc()==null) infoNewMessagePacket.setDesc("");
		//if (infoNewMessagePacket.getToken()==null) infoNewMessagePacket.setToken("");
		//logger.info(JsonUtil.object2Json(infoNewMessagePacket));
		//return JSONObject.fromObject(infoNewMessagePacket, jsonConfig).toString();
		//return JsonUtil.object2Json(infoNewMessagePacket);
		return JSON.toJSONString(infoNewMessagePacket);
	}
	
	public static final String toString(Object obj, DSObjectType type, ReturnValue out){
		return toString(obj, type, out.getMessageCode());
	}
	
	public static <T>List<T> object2List(Object object, Class<T> mainClass){
		return JsonUtil.toGsonList(JsonUtil.object2Json(object), mainClass); 
	}
	
	public static <T> T object2HashMap(String jsonStr, Class<T> mainClass){
        Map<String, Object> map = null;
        try {
            map = JsonUtil.toHashMap(jsonStr);
        } catch (Exception e) {
            logger.error("解析Json异常", e);
            logger.error("解析Json异常,jsonStr:{}", jsonStr);
            throw e;
        }
		return toGsonBean(map.get(mainClass.getSimpleName())+"", mainClass);
	}

	/**
	 * 单对象转换成Json格式 作用描述： 修改说明：
	 * 
	 * @param <T>
	 * @return *@deprecated 请参考 JsonUtil.toString(...)
	 */
	public static <T> String infoNewMessagePacket(T object, DSObjectType type,
			ReturnValue out) {
		return JsonUtil.toJsonString(object, type, out.getMessageCode());
	}

	static {
		jsonConfig = new JsonConfig();   
		jsonConfig.setJsonPropertyFilter(new PropertyFilter() {
		    @Override  // 属性为空的不序列化（null or empty string）
		    public boolean apply(Object source, String name, Object value)   
		    {   
		        return value == null;   
		    }
		});
		jsonConfig.setIgnoreTransientFields(true);//设置为不序列化transient的字段

	}
	public static void main(String[] args) {
		String json = "";
		ReportSquid rs = JSON.parseObject(json, ReportSquid.class);
		
	}
	public static void main2(String[] args) {
		//System.out.println(toString(new Group(), DSObjectType.UNKNOWN, new ReturnValue()));
		//String info = "{\"SquidLink\":{\"From_squid_id\":167,\"To_squid_id\":171,\"Squid_flow_id\":4,\"Line_color\":\"\",\"Line_type\":0,\"Arrows_style\":0,\"Endmiddle_x\":0,\"Endmiddle_y\":0,\"Startmiddle_x\":0,\"Startmiddle_y\":0,\"Id\":0,\"Key\":\"049bdaa1-c8e7-4059-89b1-d49aa41cf52a\",\"Name\":\"\",\"Status\":0,\"Type\":11}}";
		//String info = "{\"NodeName\":null,\"OrderNumber\":0,\"ColumnAttribute\":null,\"NodeType\":0,\"AddDate\":null,\"ParentNodeId\":0,\"SquidId\":0,\"SquidFLowId\":0,\"MetadataNodeList\":[],\"AttributeMap\":{},\"ColumnList\":[],\"Id\":0,\"Key\":\"b9c2c8ef-5615-41d5-9f01-419ad86baba9\",\"Name\":\"\",\"Status\":1,\"Type\":23}";
		//MetadataNode obj = JsonUtil.toGsonBean(info, MetadataNode.class);
		//Map<String, Object> map = JsonUtil.toHashMap(info);
		//SquidLink obj = JsonUtil.toGsonBean(map.get("SquidLink")+"", SquidLink.class);
		//SquidLink squidLink = (SquidLink)JSONObject.toBean(JSONObject.fromObject(info), SquidLink.class) ;
		//Gson gson = new Gson();
		//SquidLink squidLink = gson.fromJson(info, SquidLink.class);
		//System.out.println(obj.getType());
		//System.out.println(squidLink.getFrom_squid_id()+","+squidLink.getType());
		//System.out.println(SquidLink.class.getSimpleName());
		InfoNewMessagePacket infoMessagePacket = new InfoNewMessagePacket();
		infoMessagePacket.setCode(1);
		Squid squid = new Squid();
		squid.setId(10);
		String objStr = toGsonString(squid);
		System.out.println(objStr.toString());
		infoMessagePacket.setInfo(objStr);
		String str = JSONObject.fromObject(infoMessagePacket, jsonConfig).toString();
		System.out.println(str);
	}
}
