package com.eurlanda.datashire.utility;

import org.apache.poi.POIXMLDocument;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.xml.sax.InputSource;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XmlUtil {

	/**
	 * 通过对象生成XML
	 * @author Akachi
	 * 2013-12-20
	 * @param o
	 * @return
     * @throws IntrospectionException 如果分析类属性失败 
     * @throws IllegalAccessException 如果实例化 JavaBean 失败 
     * @throws InvocationTargetException 如果调用属性的 setter 方法失败 
	 */
	public static String getRequest(Object o) throws IntrospectionException, IllegalAccessException, InvocationTargetException{
		Map<String,Object> map = BeanMapUtil.convertBean(o);
		String request = MapXmlUtil.map2XmlString(map);
		return request;
	}
	
	/**
	 * 通过xml字符串获得元素
	 * @author Akachi
	 * 2013-12-20
	 * @param xmlString
	 * @return
	 * @throws IOException 
	 * @throws JDOMException 
	 */
	public static Element xmlStringToElement(String xmlString) throws JDOMException, IOException{
		StringReader sr = new StringReader(xmlString);   
		InputSource is = new InputSource(sr);
		 SAXBuilder sb=new SAXBuilder();
		    Document doc=sb.build(is);
		    Element root=doc.getRootElement();
		return root;
	}
	
	public static List<Object> setRsToList(List<Element> rs,Class clazz) throws SQLException, InstantiationException, IllegalAccessException{
		List<Object> list = new ArrayList<Object>();
		for (Element element : rs) {
			list.add(setRsToList(element,clazz));
		}
		return list;
	}
	
	
	public static <T> T setRsToList(Element rs, Class clazz) throws SQLException,
			InstantiationException, IllegalAccessException {
		Field[] fields = null;
		String[] fdNames = null;
		Class[] fdClass=null;
		int len = 0;
		try {

			fields = clazz.getDeclaredFields();
			len = fields.length;
			fdNames = new String[len];
			fdClass = new Class[len];
			for (int i = 0; i < len; i++) {
				fdNames[i] = fields[i].getName();
				fdClass[i] = fields[i].getType();
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Object obj = null;
		obj = clazz.newInstance();
		List<Element> listEmelent2 = rs.getChildren();
		List<Attribute> listattr = rs.getAttributes();
		if(listattr != null && listattr.size()>0) {
			for(int i =0;i <listattr.size(); i++) {
				Attribute attr = listattr.get(i);
				Element elt = new Element(attr.getName());
				elt.setText(attr.getValue());
				listEmelent2.add(elt);
			}
		}
		if(listEmelent2.size() ==0) {
			return (T) rs.getText();
		}
		for (int i = 0; i < fdNames.length; i++) {
			Object fdVaule = null;
			try {
				for (Element element2 : listEmelent2) {
					if (fdNames[i] != null
							&& element2.getName() != null
							&& fdNames[i].toUpperCase().equals(
									element2.getName().toUpperCase())) {
						if(element2.getChildren()!=null&&element2.getChildren().size()>0){
							Type types = fields[i].getGenericType();
							ParameterizedType pType  = (ParameterizedType)types;
							Class eClass = (Class)pType.getActualTypeArguments()[0];
							List list  = new ArrayList<Object>();
							fdVaule = list;
							for (Element element : (List<Element>)element2.getChildren()) {
								Object obj2=setRsToList(element, eClass);
								((List)fdVaule).add(obj2);
							}
							break;
						}else{
							if(element2.getValue().trim().length() ==0) {
								fdVaule = null;
							} else {
								fdVaule = element2.getValue();
							}
							break;
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			String fdSetMethod = getSetName(fdNames[i]);
			if(fdVaule == null) {
				continue;
			}
			if(fdVaule instanceof List){
				setter(obj, fdSetMethod, fdVaule,List.class);
			}else{
				setter(obj, fdSetMethod, fdVaule,fdVaule.getClass());
			}
		}
		return (T) obj;
	}
	

	private static String getSetName(String fName) {
		String setName = fName.substring(0, 1).toUpperCase() + fName.substring(1);
		return setName;
	}

	private static void setter(Object obj, String setMethod, Object fdVaule ,Class theClass) {
		try {
			Method method = obj.getClass().getMethod("set" + setMethod,theClass);
			method.invoke(obj, fdVaule);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    public static Workbook create(InputStream in) throws
            IOException,InvalidFormatException {
        if (!in.markSupported()) {
            in = new PushbackInputStream(in, 8);
        }

        if (POIFSFileSystem.hasPOIFSHeader(in)) {
            return new HSSFWorkbook(in);
        }
        if (POIXMLDocument.hasOOXMLHeader(in)) {
            return new XSSFWorkbook(OPCPackage.open(in));
        }
        throw new IllegalArgumentException("你的excel版本目前poi解析不了");
    }
}
