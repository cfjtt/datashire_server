package com.eurlanda.datashire.utility;

import org.apache.commons.betwixt.io.BeanReader;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.xml.sax.SAXException;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
/**
 * MAP XML 转换Util
 * 2012-10-20
 * @author Akachi
 * @E-Mail zsts@hotmail.com
 */
public class MapXmlUtil {
	private static final boolean IS_INITIAL_UPPERCASE=false;
	
	
	public static void main(String[] args) {
		Map map = new HashMap<String,Object>();
		map.put("testNodeString", "isTrue");
		List<Object> listObject = new ArrayList<Object>();
		map.put("testNodeList", listObject);
		listObject.add("testNodeListString");
		Map map2 = new HashMap<String,Object>();
		listObject.add(map2);
		map2.put("testNodeListMapString", "test");
		Map<String,Object> map3 = new HashMap<String,Object>();
		map2.put("testNodeListMapMap", map3);
		map3.put("testNodeListMapMapString", "1");
		map3.put("testNodeListMapMapString2", "2");
		map3.put("testNodeListMapMapString3", "3");
		map3.put("testNodeListMapMapNumber", 4l);
		map3.put("testNodeListMapMapDate", new Date(new Date().getTime()+36000000));
		String s = map2XmlString(map);
		System.out.println(s);
	}
	/**
	 * 创建
	 * 2013-12-20
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param xmlMap
	 * @return
	 */
	public static String map2XmlString(Map<String,Object> xmlMap){
		List<Element> listE = createElement(xmlMap);

		Format format = Format.getCompactFormat();
		format.setEncoding("UTF-16");
		format.setIndent("\t");
		format.setExpandEmptyElements(true);
		XMLOutputter xMLOut = new XMLOutputter(format); 
		String xmlString= xMLOut.outputString(listE);
		return xmlString;
	}

	private static List<Element> createElement(Map<String,Object> eMap){
		List<Element> listElement = new ArrayList<Element>();
		for (Entry<String, Object> entry : eMap.entrySet()) {
//			IS_INITIAL_UPPERCASE
			String elementNodeName = entry.getKey();
			if(IS_INITIAL_UPPERCASE){
//				elementNodeName=elementNodeName.
				if(elementNodeName.length()>0){
					elementNodeName=elementNodeName.substring(0,1).toUpperCase()+elementNodeName.substring(1);
				}
			}
			Element element = new Element(elementNodeName);
			analyzeElement(element,entry.getValue());
			listElement.add(element);
		}
		return listElement;
	}
	private static void analyzeElement(Element e,Object o ){
		if(o instanceof List){
			List<Object> listO = (List)o;
			for (Object object : listO) {
				analyzeElement(e,object);
			}
		}else if(o instanceof Map){
			List<Element> listElement = createElement((Map)o);
			for (int i=listElement.size()-1;i>=0;i--) {
				//Element element : listElement
				e.addContent(listElement.get(i));
			}
		}else if(o instanceof String||o instanceof Number){
			e.setText(o.toString());
		}else if(o instanceof Date){
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String dateString = df.format(o);
			e.setText(dateString);
		}
	}
	   
    public static Map<String, Object> Dom2Map(Document doc){  
        Map<String, Object> map = new HashMap<String, Object>();  
        if(doc == null)  
            return map;  
        Element root = doc.getRootElement();  
        for (Iterator iterator = root.getChildren().iterator(); iterator.hasNext();) {  
            Element e = (Element) iterator.next();  
            //System.out.println(e.getName());  
            List list = e.getChildren();  
            if(list.size() > 0){  
                map.put(e.getName(), Dom2Map(e));  
            }else  
                map.put(e.getName(), e.getText());  
        }  
        return map;  
    }  
	/**
	 * 将xml字符串转化为对象
	 * 2012-10-20
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param xmlStringxml字符串
	 * @param className类得全称（包名+类名）字符串
	 * @param cl对象的class名称
	 * @return 转化成的对象
	 */
	public static Object xmlString2Object(String xmlString, String className, Class cl) {
		// 创建一个读取xml文件的流
		StringReader xmlReader = new StringReader(xmlString);
		// 创建一个BeanReader实例，相当于转化器
		BeanReader beanReader = new BeanReader();
		// 配置BeanReader实例
		beanReader.getXMLIntrospector().setAttributesForPrimitives(false);
//		beanReader.getBindingConfiguration().setMapIDs(false); // 不自动生成ID
		// 注册要转换对象的类，并指定根节点名称
		try {
			// beanReader.registerBeanClass("SelectUserIDListBean",
			// SelectUserIDListBean.class);
			beanReader.registerBeanClass(className, cl);
		} catch (IntrospectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// 将XML解析Java Object
		Object obj = null;
		try {
			obj = beanReader.parse(xmlReader);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		return obj;
	}
  
    public  static Map Dom2Map(Element e){  
        Map map = new HashMap();  
        List list = e.getChildren();  
        if(list.size() > 0){  
            for (int i = 0;i < list.size(); i++) {  
                Element iter = (Element) list.get(i);  
                List mapList = new ArrayList();  
                  
                if(iter.getChildren().size() > 0){  
                    Map m = Dom2Map(iter);  
                    if(map.get(iter.getName()) != null){  
                        Object obj = map.get(iter.getName());  
                        if(!obj.getClass().getName().equals("java.util.ArrayList")){  
                            mapList = new ArrayList();  
                            mapList.add(obj);  
                            mapList.add(m);  
                        }  
                        if(obj.getClass().getName().equals("java.util.ArrayList")){  
                            mapList = (List) obj;  
                            mapList.add(m);  
                        }  
                        map.put(iter.getName(), mapList);  
                    }else  
                        map.put(iter.getName(), m);  
                }  
                else{  
                    if(map.get(iter.getName()) != null){  
                        Object obj = map.get(iter.getName());  
                        if(!obj.getClass().getName().equals("java.util.ArrayList")){  
                            mapList = new ArrayList();  
                            mapList.add(obj);  
                            mapList.add(iter.getText());  
                        }  
                        if(obj.getClass().getName().equals("java.util.ArrayList")){  
                            mapList = (List) obj;  
                            mapList.add(iter.getText());  
                        }  
                        map.put(iter.getName(), mapList);  
                    }else  
                        map.put(iter.getName(), iter.getText());  
                }  
            }  
        }else  
            map.put(e.getName(), e.getText());  
        return map;  
    }  
}
