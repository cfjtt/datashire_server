package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.DSBaseModel;
import org.apache.log4j.Logger;
import org.hsqldb.jdbc.JDBCClobClient;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * 注解辅助工具类
 * 
 * @author dang.lu 2013.10.11
 *
 */
public class AnnotationHelper {

	private static final Logger logger = Logger.getLogger(AnnotationHelper.class); 
	
	/**
	 * 结果集-对象映射
	 * 		支持无限极继承关系注解
	 * 
	 * @param map
	 * @param c
	 * @return
	 * @throws Exception
	 */
	public static <T> T result2Object(Map<String, Object> map, Class<T> c) throws Exception{
		logger.trace("[结果集-对象映射] "+c.getSimpleName()+"\t"+map);
		if(map==null || map.isEmpty()){
			return null;
		}
		final T obj = c.newInstance(); // object
		set(map, c, obj);
		return obj;
	}
	
	private static void set(Map<String, Object> map, Class<?> c, Object obj) 
			throws IllegalArgumentException, IllegalAccessException{
		if(c==null || Object.class.equals(c)){
			return;
		}
		logger.trace("[结果集-对象映射] "+c.getSimpleName());
		Field[] fs = c.getDeclaredFields();
		if(fs!=null){
			ColumnMpping cm=null;
			for(int j=fs.length-1; j>=0; j--){
				cm = fs[j].getAnnotation(ColumnMpping.class);
				if(cm!=null && map.get(cm.name().toUpperCase())!=null){
					fs[j].setAccessible(true);
					// char(1)->boolean [true:'Y', false:'N']
					if(Boolean.TYPE.equals(fs[j].getType())&&(cm.type()==Types.CHAR||cm.type()==Types.VARCHAR)&&cm.precision()==1){
						//之前设计的非常不合理 入库用 Y 字符来作为true
						fs[j].set(obj, "Y".equals(map.get(cm.name().toUpperCase())) || "1".equals(map.get(cm.name().toUpperCase()))?Boolean.TRUE.booleanValue():Boolean.FALSE.booleanValue());
					}
					// Timestamp->String
					else if(String.class.equals(fs[j].getType())&&cm.type()==Types.TIMESTAMP){
						fs[j].set(obj, CommonConsts.FORMAT_TIMESTAMP.format(map.get(cm.name().toUpperCase())));
					}
					//TIME->String
					else if(String.class.equals(fs[j].getType())&&cm.type()==Types.TIME)
					{
						fs[j].set(obj, CommonConsts.FORMAT_TIME.format(map.get(cm.name().toUpperCase())));
					}
					//DATE->String
					else if(String.class.equals(fs[j].getType())&&cm.type()==Types.DATE)
					{
						fs[j].set(obj, CommonConsts.FORMAT_DATE.format(map.get(cm.name().toUpperCase())));
					}
					else if(Integer.TYPE.equals(fs[j].getType())
							&& StringUtils.isNotNull(map.get(cm.name().toUpperCase()))){
						fs[j].set(obj, Integer.parseInt(map.get(cm.name().toUpperCase()).toString(), 10));
					}
					else if(Short.TYPE.equals(fs[j].getType())
							&& StringUtils.isNotNull(map.get(cm.name().toUpperCase()))){
						fs[j].set(obj, Short.parseShort(map.get(cm.name().toUpperCase()).toString(), 10));
					}
					else if(Byte.TYPE.equals(fs[j].getType())
							&& StringUtils.isNotNull(map.get(cm.name().toUpperCase()))){
						fs[j].set(obj, Byte.parseByte(map.get(cm.name().toUpperCase()).toString(), 10));
					}
	                else if(Float.TYPE.equals(fs[j].getType())
	                            && StringUtils.isNotNull(map.get(cm.name().toUpperCase()))){
	                        fs[j].set(obj, Float.parseFloat(map.get(cm.name().toUpperCase()).toString()));
	                }
	                else if(Long.TYPE.equals(fs[j].getType())
                               && StringUtils.isNotNull(map.get(cm.name().toUpperCase()))){
                           fs[j].set(obj, Long.parseLong(map.get(cm.name().toUpperCase()).toString()));
                   }
					// added by bo.dang
					else if(String.class.equals(fs[j].getType()) && cm.type()==Types.CLOB
							&& StringUtils.isNotNull(map.get(cm.name().toUpperCase()))){
						Object clob = map.get(cm.name().toUpperCase());
						if (clob instanceof JDBCClobClient){
							org.hsqldb.jdbc.JDBCClobClient cf = (JDBCClobClient) clob;
							String clobData = null;
							try {
								Reader re = cf.getCharacterStream();
								char[] ch = new char[(int)cf.length()];
								re.read(ch);
								clobData = new String(ch);
								re.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							fs[j].set(obj, clobData);
						}else if (clob instanceof String){
							fs[j].set(obj, String.valueOf(clob));
						}
					}
					else{
						fs[j].set(obj, map.get(cm.name().toUpperCase()));
					}
					logger.trace("[结果集-对象映射] "+cm.name()+" -> "+map.get(cm.name().toUpperCase()));
				}
			}
		}
		set(map, c.getSuperclass(), obj);
	}
	
	/**
	 * 获取实体类对应数据库表名
	 *   多张表情况：主表在前，从表在后，添加更新多张表一起维护，查询、删除只操作从表
	 * @param c
	 * @return
	 */
	public static final String[] getTableName(Class<?> c){
//		MultitableMapping mm = c.getAnnotation(MultitableMapping.class);
//		if(mm!=null){
//			return mm.name();
//		}
//		TableMapping m = c.getAnnotation(TableMapping.class);
//		if(m!=null){
//			return new String[]{m.name()};
//		}
		return c.getAnnotation(MultitableMapping.class).name();
	}
	
	
	/**
	 * 获取实体类对应数据库表列名
	 * @param c
	 * @return
	 */
	public static final List<String> cols2List(Class<?> c){
		List<String> columnList = new ArrayList<String>();
		Field[] f = c.getDeclaredFields();
		ColumnMpping cm=null;
		// get columns from column-mapping
		for(int i=0; i<f.length; i++){
			cm = f[i].getAnnotation(ColumnMpping.class);
			if(cm==null||cm.type()==Types.NULL) continue;
			columnList.add(cm.name());
		}
		if(c!=null && !Object.class.equals(c)){
			columnList.addAll(cols2List(c.getSuperclass()));
		}
		return columnList;
	}
	
	/**
	 * 获取实体类对应数据库表列名
	 * @param c
	 * @return
	 */
	public static final String cols2Str(Class<? extends DSBaseModel> c){
		List<String> columnList = cols2List(c);
		if(true||columnList==null||columnList.isEmpty()){
			return "*"; // 没有映射或者
		}else{
			int len=columnList.size()-1;
			StringBuffer buffer = new StringBuffer();
			for(int i=0; i<=len; i++){
				buffer.append(columnList.get(i));
				if(i!=len)buffer.append(",");
			}
			return buffer.toString();
		}
	}
	
}
