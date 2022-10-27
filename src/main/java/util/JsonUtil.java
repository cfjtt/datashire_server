package util;

import com.eurlanda.datashire.utility.StringUtils;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

	/***
	 * 将List对象序列化为JSON文本
	 */
	public static <T> String toJSONString(List<T> list) {
		JSONArray jsonArray = JSONArray.fromObject(list);
		return jsonArray.toString();
	}

	/***
	 * 将对象序列化为JSON文本
	 *
	 * @param object
	 * @return
	 */
	public static String toJSONString(Object object) {
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
			data.put(key, value);
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
	 * @param jsonArray
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
	 *            泛型T 代表主实体类型
	 * @param <D>
	 *            泛型D 代表从实体类型
	 * @param jsonString
	 *            JSON文本
	 * @param mainClass
	 *            主实体类型
	 * @param detailName
	 *            从实体类在主实体类中的属性名称
	 * @param detailClass
	 *            从实体类型
	 * @return
	 */
	public static <T, D> T toBean_old(String jsonString, Class<T> mainClass, String detailName, Class<D> detailClass) {
		JSONObject jsonObject = JSONObject.fromObject(jsonString);
		JSONArray jsonArray = (JSONArray) jsonObject.get(detailName);

		T mainEntity = JsonUtil.toBean(jsonObject, mainClass);
		List<D> detailList = JsonUtil.toList(jsonArray, detailClass);

		try {
			//BeanUtils.setProperty(mainEntity, detailName, detailList);
		} catch (Exception ex) {
			throw new RuntimeException("主从关系JSON反序列化实体失败！");
		}

		return mainEntity;
	}

	/***
	 * 将JSON文本反序列化为主从关系的实体
	 *
	 * @param <T>泛型T 代表主实体类型
	 * @param <D1>泛型D1 代表从实体类型
	 * @param <D2>泛型D2 代表从实体类型
	 * @param jsonString
	 *            JSON文本
	 * @param mainClass
	 *            主实体类型
	 * @param detailName1
	 *            从实体类在主实体类中的属性
	 * @param detailClass1
	 *            从实体类型
	 * @param detailName2
	 *            从实体类在主实体类中的属性
	 * @param detailClass2
	 *            从实体类型
	 * @return
	 */
	public static <T, D1, D2> T toBean_old(String jsonString, Class<T> mainClass, String detailName1,
			Class<D1> detailClass1, String detailName2, Class<D2> detailClass2) {
		JSONObject jsonObject = JSONObject.fromObject(jsonString);
		JSONArray jsonArray1 = (JSONArray) jsonObject.get(detailName1);
		JSONArray jsonArray2 = (JSONArray) jsonObject.get(detailName2);

		T mainEntity = JsonUtil.toBean(jsonObject, mainClass);
		List<D1> detailList1 = JsonUtil.toList(jsonArray1, detailClass1);
		List<D2> detailList2 = JsonUtil.toList(jsonArray2, detailClass2);

		try {
			//BeanUtils.setProperty(mainEntity, detailName1, detailList1);
			//BeanUtils.setProperty(mainEntity, detailName2, detailList2);
		} catch (Exception ex) {
			throw new RuntimeException("主从关系JSON反序列化实体失败！");
		}

		return mainEntity;
	}

	/***
	 * 将JSON文本反序列化为主从关系的实体
	 *
	 * @param <T>泛型T 代表主实体类型
	 * @param <D1>泛型D1 代表从实体类型
	 * @param <D2>泛型D2 代表从实体类型
	 * @param jsonString
	 *            JSON文本
	 * @param mainClass
	 *            主实体类型
	 * @param detailName1
	 *            从实体类在主实体类中的属性
	 * @param detailClass1
	 *            从实体类型
	 * @param detailName2
	 *            从实体类在主实体类中的属性
	 * @param detailClass2
	 *            从实体类型
	 * @param detailName3
	 *            从实体类在主实体类中的属性
	 * @param detailClass3
	 *            从实体类型
	 * @return
	 */
	public static <T, D1, D2, D3> T toBean_old(String jsonString, Class<T> mainClass, String detailName1,
			Class<D1> detailClass1, String detailName2, Class<D2> detailClass2, String detailName3,
			Class<D3> detailClass3) {
		JSONObject jsonObject = JSONObject.fromObject(jsonString);
		JSONArray jsonArray1 = (JSONArray) jsonObject.get(detailName1);
		JSONArray jsonArray2 = (JSONArray) jsonObject.get(detailName2);
		JSONArray jsonArray3 = (JSONArray) jsonObject.get(detailName3);

		T mainEntity = JsonUtil.toBean(jsonObject, mainClass);
		List<D1> detailList1 = JsonUtil.toList(jsonArray1, detailClass1);
		List<D2> detailList2 = JsonUtil.toList(jsonArray2, detailClass2);
		List<D3> detailList3 = JsonUtil.toList(jsonArray3, detailClass3);

		try {
			//BeanUtils.setProperty(mainEntity, detailName1, detailList1);
			//BeanUtils.setProperty(mainEntity, detailName2, detailList2);
			//BeanUtils.setProperty(mainEntity, detailName3, detailList3);
		} catch (Exception ex) {
			throw new RuntimeException("主从关系JSON反序列化实体失败！");
		}

		return mainEntity;
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
		Gson gson = new GsonBuilder()
	    // .registerTypeAdapter(Id.class, new IdTypeAdapter())
	     .enableComplexMapKeySerialization()
	     .serializeNulls()
	     //.setDateFormat(DateFormat.LONG)
	     .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
	     //.setPrettyPrinting()	   //格式化输出
	     .setVersion(1.0)
	     .create();
		  T object = gson.fromJson( jsonString , mainClass);

		  return object;

	}
	public static String toGsonString(Object object) {
		Gson gson = new GsonBuilder()
	    // .registerTypeAdapter(Id.class, new IdTypeAdapter())
	     .enableComplexMapKeySerialization()
	     .serializeNulls()
	     //.setDateFormat(DateFormat.LONG)
	     .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
	     //.setPrettyPrinting()	   //格式化输出
	     .setVersion(1.0)
	     .create();

		//JSONArray jsonArray = JSONArray.fromObject(object);
		return gson.toJson(object);
	}
	/***
	 * 将List对象序列化为JSON文本
	 */
	public static <T>List<T>  toGsonList(String list,Class<T> mainClass) {
		//TODO
		Gson gson = new GsonBuilder()
	    // .registerTypeAdapter(Id.class, new IdTypeAdapter())
	     .enableComplexMapKeySerialization()
	     .serializeNulls()
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
	    }
		return lcs;
	}

	/** 将JSON串反序列化为java对象(add by dang.lu 2013.9.13) */
	public static <T> T json2Object(String jsonStr,Class<T> mainClass) {
		return StringUtils.isNull(jsonStr)?null:new GsonBuilder()
	     .enableComplexMapKeySerialization()
	     .serializeNulls()
	     .setVersion(1.0)
	     .create().fromJson(jsonStr , mainClass);
	}
	/** 将java对象序列化为JSON串(add by dang.lu 2013.9.13) */
	public static String object2Json(Object obj) {
		return obj==null?null:new GsonBuilder()
	     .enableComplexMapKeySerialization()
	     .serializeNulls()
	     .setVersion(1.0)
	     .create().toJson(obj);
	}
	/** 将JSON串反序列化为java对象(add by dang.lu 2013.9.13) */
	public static <T> T json2Object(String jsonStr,Class<T> mainClass, FieldNamingPolicy p) {
		return StringUtils.isNull(jsonStr)?null:new GsonBuilder()
	     .enableComplexMapKeySerialization()
	     .serializeNulls()
	     .setFieldNamingPolicy(p)
	     .setVersion(1.0)
	     .create().fromJson(jsonStr , mainClass);
	}
	/** 将java对象序列化为JSON串(add by dang.lu 2013.9.13) */
	public static String object2Json(Object obj, FieldNamingPolicy p) {
		return obj==null?null:new GsonBuilder()
	     .enableComplexMapKeySerialization()
	     .serializeNulls()
	     .setFieldNamingPolicy(p)
	     .setVersion(1.0)
	     .create().toJson(obj);
	}

}
