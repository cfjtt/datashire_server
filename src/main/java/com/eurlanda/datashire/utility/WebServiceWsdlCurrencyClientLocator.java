package com.eurlanda.datashire.utility;

import javax.xml.rpc.ServiceException;
import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

public class WebServiceWsdlCurrencyClientLocator extends org.apache.axis.client.Service{
	/**
	 * 
	 * 2014-12-3
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param endpointURL url
	 * @param domainName &lt;xs:schema xmlns:tns="http://test.com/"/&gt;
	 * @param methodName &lt;xs:complexType name="subtract"&gt;
	 * @param paramDefin &lt;xs:element name="arg0" type="xs:long"/><xs:element name="arg1" type="xs:long"/&gt;
	 * @param returnType &lt;xs:element name="return" type="xs:long"/&gt;
	 * @param portName &lt;wsdl:port name="CalculatorImplPort"&gt;
	 * @return
	 * @throws javax.xml.rpc.ServiceException
	 */
    public WebServiceWsdlCurrencyClient getWebServiceWsdlCurrencyClientPort(URL endpointURL,String domainName,String methodName,Map<String,Object> paramDefin,Class returnType,String portName) throws javax.xml.rpc.ServiceException {
        try {
        	WebServiceWsdlCurrencyClient _stub = new WebServiceWsdlCurrencyClient(endpointURL, this,domainName,methodName,paramDefin,returnType);
            _stub.setPortName(portName);
            return _stub;
        }
        catch (org.apache.axis.AxisFault e) {
            return null;
        }
    }
    public static void main(String[] args) throws MalformedURLException, ServiceException, RemoteException {
    	WebServiceWsdlCurrencyClientLocator wswccl = new WebServiceWsdlCurrencyClientLocator();
    	URL url = new URL("http://127.0.0.1:8080/datashire_sw_bak2/cxf/calculatorService?wsdl");
    	String domainName="http://test.com/";
    	String methodName="add";
    	Class returnType = Long.class;
    	String portName = "CalculatorImplPort";
    	Map<String,Object> paramDefin = new HashMap<String,Object>();
    	paramDefin.put("arg0", new Long(1l));
    	paramDefin.put("arg1", new Long(5l));
    	WebServiceWsdlCurrencyClient wswcc =wswccl.getWebServiceWsdlCurrencyClientPort(url, domainName, methodName, paramDefin, returnType, portName);
    	wswcc.setUsername("fuck");
    	wswcc.setPassword("fuck");
    	Object o =wswcc.execute();
    	try{
    	System.out.println(o.toString()+":"+(o.getClass()));
    	}catch (Exception e) {
    		e.printStackTrace();
			// TODO: handle exception
		}
	}
}
