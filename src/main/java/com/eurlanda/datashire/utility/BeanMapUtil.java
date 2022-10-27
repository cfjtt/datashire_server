package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.annotation.ColumnMpping;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * JavaBean Map 转换Util
 * 2012-10-20
 * @author Akachi
 * @E-Mail zsts@hotmail.com
 */
public class BeanMapUtil {
	/** 
     * 将一个 Map 对象转化为一个 JavaBean 
     * 2012-10-11
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
     * @param type 要转化的类型 
     * @param map 包含属性值的 map 
     * @return 转化出来的 JavaBean 对象 
     * @throws IntrospectionException 如果分析类属性失败 
     * @throws IllegalAccessException 如果实例化 JavaBean 失败 
     * @throws InstantiationException 如果实例化 JavaBean 失败 
     * @throws InvocationTargetException 如果调用属性的 setter 方法失败 
     */  
    @SuppressWarnings("rawtypes")  
    public static Object convertMap(Class type, Map map)  
            throws IntrospectionException, IllegalAccessException,  
            InstantiationException, InvocationTargetException {  
        BeanInfo beanInfo = Introspector.getBeanInfo(type); // 获取类属性  
        Object obj = type.newInstance(); // 创建 JavaBean 对象  
  
        // 给 JavaBean 对象的属性赋值  
        PropertyDescriptor[] propertyDescriptors =  beanInfo.getPropertyDescriptors();  
        for (int i = 0; i< propertyDescriptors.length; i++) {  
            PropertyDescriptor descriptor = propertyDescriptors[i];  
            String propertyName = descriptor.getName();  
  
            if (map.containsKey(propertyName)) {  
                // 下面一句可以 try 起来，这样当一个属性赋值失败的时候就不会影响其他属性赋值。  
                Object value = map.get(propertyName);  
  
                Object[] args = new Object[1];  
                args[0] = value;  
  
                descriptor.getWriteMethod().invoke(obj, args);  
            }  
        }  
        return obj;  
    }

    public static Object convertSQLMap(Class type, Map map)
            throws IllegalAccessException, InstantiationException, IntrospectionException,
            InvocationTargetException {

        List<Field> fields = getAllFields(type);
        Object obj = type.newInstance(); // 创建 JavaBean 对象

        for(Field field : fields) {
            ColumnMpping cm = field.getAnnotation(ColumnMpping.class);
            if(cm != null && cm.type() != Types.NULL) {
                String columnName = cm.name().toUpperCase();
                if(map.containsKey(columnName)) {
                    PropertyDescriptor pd = new PropertyDescriptor(field.getName(), type);
                    Method method = pd.getWriteMethod();
                    // boolean 类型，数据库中保存的是 Y,N
                    if(field.getType() == boolean.class || field.getType() == Boolean.class) {
                        String value = String.valueOf(map.get(columnName));
                        if(value == null) {
                            method.invoke(obj, null);
                        } else {
                            if("Y".equalsIgnoreCase(value)) {
                                method.invoke(obj, true);
                            } else if("N".equalsIgnoreCase(value)) {
                                method.invoke(obj, false);
                            }
                        }
                    } else {
                        try {
                            // 判断数据类型是否为 int,long, 数据是否为null
                            Object o = map.get(columnName);
                            if(o == null) {
                                if (field.getType() == int.class || field.getType() == long.class) {

                                }
                            } else {
                                method.invoke(obj, map.get(columnName));
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        /**
        BeanInfo beanInfo = Introspector.getBeanInfo(type); // 获取类属性
        Object obj = type.newInstance(); // 创建 JavaBean 对象

        // 给 JavaBean 对象的属性赋值
        PropertyDescriptor[] propertyDescriptors =  beanInfo.getPropertyDescriptors();
        for (int i = 0; i< propertyDescriptors.length; i++) {
            PropertyDescriptor descriptor = propertyDescriptors[i];
            String propertyName = descriptor.getName();

            if (map.containsKey(propertyName)) {
                // 下面一句可以 try 起来，这样当一个属性赋值失败的时候就不会影响其他属性赋值。
                Object value = map.get(propertyName);

                Object[] args = new Object[1];
                args[0] = value;

                descriptor.getWriteMethod().invoke(obj, args);
            }
        }
         */
        return obj;
    }

    public static List<Field> getAllFields(Class type) {
        List<Field> list = new ArrayList<>();
        while(type != Object.class) {
            Field[] fields = type.getDeclaredFields();
            for (Field f : fields) {
                list.add(f);
            }
            type = type.getSuperclass();
        }
        return list;
    }

    /** 
     * 将一个 JavaBean 对象转化为一个  Map 
     * 2012-10-11
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     * @param bean 要转化的JavaBean 对象 
     * @return 转化出来的  Map 对象 
     * @throws IntrospectionException 如果分析类属性失败 
     * @throws IllegalAccessException 如果实例化 JavaBean 失败 
     * @throws InvocationTargetException 如果调用属性的 setter 方法失败 
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })  
    public static Map convertBean(Object bean)  
            throws IntrospectionException, IllegalAccessException, InvocationTargetException {  
        Class type = bean.getClass();  
        Map returnMap = new HashMap();  
        BeanInfo beanInfo = Introspector.getBeanInfo(type);  
  
        PropertyDescriptor[] propertyDescriptors =  beanInfo.getPropertyDescriptors();  
        for (int i = 0; i< propertyDescriptors.length; i++) {  
            PropertyDescriptor descriptor = propertyDescriptors[i];  
            String propertyName = descriptor.getName();  
            if (!propertyName.equals("class")) {  
                Method readMethod = descriptor.getReadMethod();  
                Object result = readMethod.invoke(bean, new Object[0]);  
                if (result != null) {
                	if(result instanceof String){
                        returnMap.put(propertyName, result); 
                	}else if(result instanceof List){
                		List<Object> listObject = new ArrayList<Object>();
                		for (Object obj : (List)result) {
                			listObject.add(convertBean(obj));
						}
//                		Map map = new HashMap<String,Object>();
//                		String name =bean.getClass().getName();
//                		name = name.substring(name.lastIndexOf(".")+1, name.length());
//                		
//                		map.put(name, listObject);
//                		returnMap.put(propertyName, map);
                		returnMap.put(propertyName, listObject);
                	}else{
                		Map theMap = convertBean(result);
                        returnMap.put(propertyName, theMap); 
                	}
                } else {  
                    returnMap.put(propertyName, "");  
                }  
            }  
        }  
//		Map map = new HashMap<String,Object>();
//		String name =bean.getClass().getName();
//		name = name.substring(name.lastIndexOf(".")+1, name.length());
//		
//		map.put(name, returnMap);
        
        return returnMap;  
    }  
}
