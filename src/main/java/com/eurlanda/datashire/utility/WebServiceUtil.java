package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.operation.WebServiceFunction;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;

import javax.xml.rpc.ServiceException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * webService To Function Util 2014-11-28
 * @author Akachi
 * @E-Mail zsts@hotmail.com
 */
public class WebServiceUtil {
	public static final String XML_TYPE_WSDL = "wsdl";
	public static final String XML_TYPE_WADL = "wadl";

	public static void main(String[] args) throws HttpException, IOException,
			ServiceException, JDOMException {
//		System.out.println("<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\n<soap:Body><ns2:addResponse xmlns:ns2=\"http://test.com/\">\n<return>2</return></ns2:addResponse></soap:Body></soap:Envelope>");
		
		// try{
		List<WebServiceFunction> listWebServiceFunction = getWebServiceFunctionByUrl(
				"http://192.168.137.119:8080/datashire_sw_bak2/cxf/calculatorService?wsdl",
				XML_TYPE_WSDL);
		List<SourceTable> souceTable;
		souceTable = converWebServiceFunction2Source(listWebServiceFunction);
//		URL url = new URL(
//				"http://192.168.137.119:8080/datashire_sw_bak2/cxf/calculatorService");
		String soapRequestData = souceTable.get(1).getWsdlRequest();
		
		System.out.println(soapRequestData);

//		PostMethod postMethod = new PostMethod(
//				"http://epacketws.pushauction.net/v3/orderservice.asmx?wsdl");
		PostMethod postMethod = new PostMethod(
				"http://192.168.137.119:8080/datashire_sw_bak2/cxf/calculatorService?wsdl");
		// 然后把Soap请求数据添加到PostMethod中
		byte[] b = soapRequestData.getBytes("utf-8");
		InputStream is = new ByteArrayInputStream(b, 0, b.length);
		RequestEntity re = new InputStreamRequestEntity(is, b.length,
				"application/soap+xml; charset=utf-8");
		postMethod.setRequestEntity(re);

		// 最后生成一个HttpClient对象，并发出postMethod请求
		HttpClient httpClient = new HttpClient();
		int statusCode = httpClient.executeMethod(postMethod);
		if (statusCode == 200) {
			System.out.println("调用成功！");
			String soapResponseData = postMethod.getResponseBodyAsString();
			System.out.println(soapResponseData);
		} else {
			System.out.println("调用失败！错误码：" + statusCode);
		}

	}
	/**
	 * 解析returnXml for wadl
	 * 2014-12-11
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param encoded
	 * @param methodName
	 * @param retStr
	 * @return
	 * @throws IOException 
	 * @throws JDOMException 
	 */
	public static String[] getReturnParam(String retStr) throws JDOMException, IOException{
		if(retStr==null){
			return new String[0];
		}
		if(StringUtil.replace(retStr, "\n", "").replace(" ", "").substring(0, 1).equals("{")){
			return getReturnParamToJson(retStr);
		}else if(StringUtil.replace(retStr, "\n", "").replace(" ", "").substring(0, 1).equals("<")){
			return getReturnParamToXml(retStr);
		}
		return new String[0];
	}
	private static String[] getReturnParamToJson(String json){
		Map<String,Object> map=JsonUtil.toHashMap(json);
		String[] ss = new String[map.entrySet().size()];
		int i = 0;
		for (Entry<String, Object> entry : map.entrySet()) {
			ss[i]=entry.getKey();
			i++;
		}
		return ss;
	}
	
	private static String[] getReturnParamToXml(String xml) throws JDOMException, IOException{
		Element e= XmlUtil.xmlStringToElement(xml);
		List<Element> listElement = e.getChildren();
		Map<String,Object> map = new HashMap<String,Object>();
		for (Element element : listElement) {
			if(!map.containsKey(element.getName())){
				map.put(element.getName(), element.getValue());
			}
		}
		String[] ss = new String[map.entrySet().size()];
		int i =0;
		for (Entry<String, Object> entry : map.entrySet()) {
			ss[i]=entry.getKey();
			i++;
		}
		return ss;
	}
	
	/**
	 * 获取wsdl请求参数 
	 * 2014-12-4
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param wsf
	 * @return
	 */
	private static String[] getWsdlRequestXml(WebServiceFunction wsf) {
		String s[] = new String[3];
		s[0]="<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
				+"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
				+"@header;"
				+"@body;"
				+"</soapenv:Envelope>\n";
		s[2] = "<soapenv:Body>\n" + "<add xmlns=\"http://test.com/\">\n";
		for (java.util.Map.Entry<String, String> entry : wsf.getParameter()
				.entrySet()) {
			s[2] += "<" + entry.getKey() + " xmlns=\"\">@" + entry.getKey()
					+ ";</" + entry.getKey() + ">\n";
		}
		s[2] += "</add>\n" + "</soapenv:Body>\n";

		return s;
	}
	
	/**
	 * 将webService转换成SourceTableFunction
	 * 2014-12-4
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param wsfList
	 * @return
	 */
	public static List<SourceTable> converWebServiceFunction2Source(
			List<WebServiceFunction> wsfList) {
		List<SourceTable> list = new ArrayList<SourceTable>();

		for (WebServiceFunction wsf : wsfList) {
			SourceTable sourceTable = new SourceTable();
			if(wsf.getMethod()!=null){
				if(wsf.getMethod().toUpperCase().equals("GET")){
					sourceTable.setMethod(0);
				}else if(wsf.getMethod().toUpperCase().equals("POST")){
					sourceTable.setMethod(1);
				}else if(wsf.getMethod().toUpperCase().equals("PUT")){
					sourceTable.setMethod(2);
				}else if(wsf.getMethod().toUpperCase().equals("DELETE")){
					sourceTable.setMethod(3);
				}
			}
			sourceTable.setTableName(wsf.getFunctionName());
			String[] ss = StringUtil.getParam(wsf.getFunctionUrl(), "{", "}");
			String url = wsf.getFunctionUrl();
			for (String string : ss) {
				url=StringUtil.replace(url, "{"+string+"}", "@"+string+";");
			}
			sourceTable.setUrl(url);
			if (wsf.getType().equals(XML_TYPE_WADL)) {
				for (java.util.Map.Entry<String, String> entry : wsf
						.getParameter().entrySet()) {
					String s = "";
					if (wsf.getFunctionUrl() != null
							&& StringUtil.contains(wsf.getFunctionUrl(), "@"
									+ entry.getKey() + ";")) {// 判断是否存在于URL中
					} else {
						s += "@" + entry.getKey() + ";";
						if (!"".equals(s)) {
							sourceTable.setPost_params(s);
						}
					}
				}
			} else {
				sourceTable.setTemplet_xml(getWsdlRequestXml(wsf)[0]);
				sourceTable.setHeader_xml(getWsdlRequestXml(wsf)[1]);
				sourceTable.setParams_xml(getWsdlRequestXml(wsf)[2]);
			}
			sourceTable.setReturnParam(wsf.getReturnTypes());
			list.add(sourceTable);
		}
		return list;

	}

	/**
	 * 获取WebService远程方法 By Wadl or Wsdl 
	 * 2014-11-28
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param address
	 * @param type
	 * @return
	 * @throws IOException
	 * @throws ServiceException
	 * @throws JDOMException
	 */
	public static List<WebServiceFunction> getWebServiceFunctionByUrl(
			String address, String type) throws IOException, ServiceException,
			JDOMException {
		if (type != null && type.equals(XML_TYPE_WSDL)) {
			return getWsdlFunction(address);
		} else if (type != null && type.equals(XML_TYPE_WADL)) {
			return getWadlFunction(address);
		}
		return null;
	}

	/**
	 * 获取wadl所有方法 2014-11-28
	 * 
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param address
	 * @return
	 * @throws IOException
	 * @throws ServiceException
	 * @throws JDOMException
	 */
	private static List<WebServiceFunction> getWadlFunction(String address)
			throws IOException, ServiceException, JDOMException {
		String wsdlStr = getWsdl(address);
		Element e = XmlUtil.xmlStringToElement(wsdlStr);
		Element rootResource = getChild("resource", getChild("resources", e));
		List<WebServiceFunction> listWebServiceFunction = new ArrayList<WebServiceFunction>();
		for (Element e1 : getChildList("resource", rootResource)) {
			WebServiceFunction wsf = new WebServiceFunction();
			wsf.setType(XML_TYPE_WADL);
			wsf.setRequestXml(elementTOXmlString(e1, "", ""));
			wsf.setFunctionUrl(getAttribute("path", e1).getValue());
			wsf.setMethod(getAttribute("name", getChild("method", e1)).getValue());//获取method
			wsf.setFunctionName(wsf.getFunctionUrl());
			listWebServiceFunction.add(wsf);
			wsf.setParameter(new HashMap<String, String>());
			for (Element e2 : (List<Element>) getChildList("param", e1)) {
				wsf.getParameter().put(getAttribute("name", e2).getValue(),
						getAttribute("type", e2).getValue().replace("xs:", ""));
			}
			if (getChild("representation",
					getChild("response", getChild("method", e1))) != null) {
				Element returnElement = getChild(
						"param",
						getChild("representation",
								getChild("response", getChild("method", e1))));
				if (returnElement != null) {
					wsf.setReturnType(getAttribute("type", returnElement)
							.getValue().replace("xs:", ""));
				}
			}
		}
		return listWebServiceFunction;
	}

	/**
	 * 获取wsdl所有方法 2014-11-28
	 * 
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param address
	 * @return
	 * @throws IOException
	 * @throws ServiceException
	 * @throws JDOMException
	 */
	private static List<WebServiceFunction> getWsdlFunction(String address)
			throws IOException, ServiceException, JDOMException {
		String wsdlStr = getWsdl(address);
		Element e = XmlUtil.xmlStringToElement(wsdlStr);
		Element portType = getChild("portType", e);
		Element types = getChild("types", e);
		List<WebServiceFunction> listWebServiceFunction = new ArrayList<WebServiceFunction>();
		for (Element element : (List<Element>) portType.getChildren()) {
			WebServiceFunction wsf = new WebServiceFunction();
			wsf.setType(XML_TYPE_WSDL);
			wsf.setXmlns_tns(e.getAttributeValue("targetNamespace"));
			wsf.setFunctionName(getAttribute("name", element).getValue());
			listWebServiceFunction.add(wsf);
			Attribute functionAttribute = null;
			if(getAttribute("name", getChild("input", element))!=null){
				functionAttribute=getAttribute("name", getChild("input", element));
			}else if(getAttribute("message", getChild("input", element))!=null){
				functionAttribute=getAttribute("message", getChild("input", element));
			}
			wsf.setParameter(getComplexType(functionAttribute.getValue(),
					types, wsf));
			String returnBeanName=null;
			if(getChild("output", element)!=null&&getAttribute("name", getChild("output", element))!=null){
				returnBeanName = getAttribute("name", getChild("output", element)).getValue();
			}
			List<String[]> returnType = getComplexType(returnBeanName, types);
			if (returnType != null && returnType.size() != 0) {
				Map<String, String> map = new HashMap<String, String>();
				String[] strings = returnType.get(0);
				if (strings[0] != null && strings[0].equals("tns")) {// 如果是复杂内心就多执行一成
					wsf.setReturnType(strings[1]);
					for (String[] s : getComplexType(strings[1], types)) {
						map.put(s[2], s[1]);
					}
				} else {
					map.put("return","string");
					wsf.setReturnType("simple");
				}
				wsf.setReturnTypes(map);
			}
			wsf.setMethod("post");
		}
		return listWebServiceFunction;
	}

	private static List<String[]> getComplexType(String name, Element e) {
		List<String[]> listMap = new ArrayList<String[]>();
//		Map<String, String> map = new HashMap<String, String>();
		for (Element e1 : (List<Element>) getChildList("complexType",
				getChild("schema", e))) {
			if (getAttribute("name", e1).getValue().equals(name)) {
				for (Element e2 : (List<Element>) getChildList("element",
						getChild("sequence", e1))) {
					String[] returnType = new String[3];
					String[] type = getAttribute("type", e2).getValue().split(":");
					returnType[0]=type[0];
					returnType[1]=type[1];
					returnType[2]=getAttribute("name", e2).getValue();
					listMap.add(returnType);
//					map.put(getAttribute("name", e2).getValue(),returnType[1]);
//					map.put("type", returnType[0]);
				}
				break;
			}
		}
		return listMap;
	}

	private static Map<String, String> getComplexType(String name, Element e,
			WebServiceFunction wsf) {
		Map<String, String> map = new HashMap<String, String>();
		for (Element e1 : (List<Element>) getChildList("complexType",
				getChild("schema", e))) {
			if (getAttribute("name", e1).getValue().equals(name)) {
				wsf.setRequestXml(elementTOXmlString(e1, "", "xs:"));
				for (Element e2 : (List<Element>) getChildList("element",
						getChild("sequence", e1))) {
					map.put(getAttribute("name", e2).getValue(),
							getAttribute("type", e2).getValue().replace("xs:",
									""));
				}
				break;
			}
		}
		return map;
	}

	private static Attribute getAttribute(String name, Element e) {
		for (Attribute attribute : (List<Attribute>) e.getAttributes()) {
			if (name.equals(attribute.getName()))
				return attribute;
		}
		return null;
	}

	private static Element getChild(String name, Element e) {
		for (Element element : (List<Element>) e.getChildren()) {
			if (name.equals(element.getName())) {
				return element;
			}
		}
		return null;
	}

	private static List<Element> getChildList(String name, Element e) {
		List<Element> elements = new ArrayList<Element>();
		for (Element element : (List<Element>) e.getChildren()) {
			if (name.equals(element.getName())) {
				elements.add(element);
			}
		}
		return elements;
	}

	private static String getWsdl(String wsdlAddress) throws IOException {
		URL url = new URL(wsdlAddress);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				url.openStream()));
		String returnString = "";
		String inputLine;
		while ((inputLine = in.readLine()) != null)
			returnString += inputLine;
		in.close();
		return returnString;
	}

	private static String elementTOXmlString(Element e, String indentation,
			String prefix) {
		String xmlString = indentation + "<" + prefix + e.getName() + " ";
		for (Attribute attribute : (List<Attribute>) e.getAttributes()) {
			xmlString += attribute.getName() + "=\"" + attribute.getValue()
					+ "\" ";
		}
		if (e.getChildren() == null || e.getChildren().size() == 0) {
			xmlString += "/";
		}
		xmlString += ">\n";

		if (e.getChildren() != null && e.getChildren().size() > 0) {
			for (Element e2 : (List<Element>) e.getChildren()) {
				xmlString += elementTOXmlString(e2, indentation + " ", prefix);
			}
			xmlString += indentation + "</" + prefix + e.getName() + ">\n";
		}
		return xmlString;
	}
}
