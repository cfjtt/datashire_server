package com.eurlanda.datashire.utility;

import org.apache.axis.NoEndPointException;
import org.apache.axis.client.Call;
import org.apache.axis.description.OperationDesc;
import org.apache.axis.description.ParameterDesc;

import javax.xml.namespace.QName;
import javax.xml.rpc.Service;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Map.Entry;

public class WebServiceWsdlCurrencyClient extends org.apache.axis.client.Stub {

	static OperationDesc[] _operations;
	private String methodName;
	private String domainName;
	private Call _call;
	private Map<String, Object> param;

	public WebServiceWsdlCurrencyClient(URL endpointURL, Service service,
			String domainName, String methodName,
			Map<String, Object> paramDefin, Class returnType)
			throws org.apache.axis.AxisFault {
		this.param=paramDefin;
		this.methodName = methodName;
		this.domainName = domainName;
		if (service == null) {
			super.service = new org.apache.axis.client.Service();
		} else {
			super.service = service;
		}
		((org.apache.axis.client.Service) super.service)
				.setTypeMappingVersion("1.2");
		super.cachedEndpoint = endpointURL;
		_operations = new OperationDesc[1];
		this.init(methodName, paramDefin);
	}

	private void init(String methodName, Map<String, Object> paramDefine) {
		OperationDesc oper;
		ParameterDesc param;
		oper = new OperationDesc();
		oper.setName(methodName);
		for (Entry<String, Object> paramentry : paramDefine.entrySet()) {
			QName qNameParamName = new QName("", paramentry.getKey());
			QName qNameParamType = new QName(
					"http://www.w3.org/2001/XMLSchema", paramentry.getValue()
							.getClass().getName());
			param = new ParameterDesc(qNameParamName, ParameterDesc.IN,
					qNameParamType, paramentry.getValue().getClass(), false,
					false);
			oper.addParameter(param);
		}
		_operations[0] = oper;
	}


	public Object execute() throws RemoteException {
		return this.execute(param);	
	}
	/**
	 * 
	 * 2014-12-3
	 * 
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param map
	 *            参数
	 * @return
	 * @return
	 * @throws RemoteException
	 */
	public Object execute(Map<String, Object> param) throws RemoteException {
		if (super.cachedEndpoint == null) {
			throw new NoEndPointException();
		}
		if(_call==null){
			_call = renovateCall();
		}
		_call.setOperation(_operations[0]);
		_call.setUseSOAPAction(true);
		_call.setSOAPActionURI("");
		_call.setEncodingStyle(null);
		_call.setProperty(Call.SEND_TYPE_ATTR, Boolean.FALSE);
		_call.setProperty(org.apache.axis.AxisEngine.PROP_DOMULTIREFS,
				Boolean.FALSE);
		_call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
		_call.setOperationName(new javax.xml.namespace.QName(domainName,
				methodName));
//		_call.setReturnClass(returnType);
		setRequestHeaders(_call);
		setAttachments(_call);
		try {
			Object[] objects = new Object[param.size()];
			int i = 0;
			for (Entry<String, Object> entry : param.entrySet()) {
				objects[i] = entry.getValue();
				i++;
			}
			Object _resp = _call.invoke(objects);

			if (_resp instanceof RemoteException) {
				throw (RemoteException) _resp;
			} else {
				extractAttachments(_call);
				return _resp;
//				try {
//					return _resp;
//				} catch (java.lang.Exception _exception) {
//					return org.apache.axis.utils.JavaUtils.convert(_resp, long.class);
//				}
			}
		} catch (org.apache.axis.AxisFault axisFaultException) {
			throw axisFaultException;
		}
	}

	/**
	 * 创建消息 2014-12-3
	 * 
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @return
	 * @throws RemoteException
	 */
	public Call renovateCall() throws RemoteException {
		try {
			Call _call = super._createCall();
			if (super.maintainSessionSet) {
				_call.setMaintainSession(super.maintainSession);
			}
			if (super.cachedUsername != null) {
				_call.setUsername(super.cachedUsername);
			}
			if (super.cachedPassword != null) {
				_call.setPassword(super.cachedPassword);
			}
			if (super.cachedEndpoint != null) {
				_call.setTargetEndpointAddress(super.cachedEndpoint);
			}
			if (super.cachedTimeout != null) {
				_call.setTimeout(super.cachedTimeout);
			}
			if (super.cachedPortName != null) {
				_call.setPortName(super.cachedPortName);
			}
			java.util.Enumeration keys = super.cachedProperties.keys();
			while (keys.hasMoreElements()) {
				java.lang.String key = (java.lang.String) keys.nextElement();
				_call.setProperty(key, super.cachedProperties.get(key));
			}
			return _call;
		} catch (java.lang.Throwable _t) {
			throw new org.apache.axis.AxisFault(
					"Failure trying to get the Call object", _t);
		}
	}

}
