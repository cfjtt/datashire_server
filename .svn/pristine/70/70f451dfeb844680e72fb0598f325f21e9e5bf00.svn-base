package com.eurlanda.datashire.utility;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class HttpUtil {
	/**
	 * 发送HTTP请求 2014-12-12
	 * 
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param url
	 * @param param
	 * @param requestMethod
	 * @param application_type
	 * @return
	 * @throws Exception 
	 */
	public static String send(String url, String param, int requestMethod,String application_type,ReturnValue out)
			throws Exception {
		byte[] b = null;
		InputStream is = null;
		RequestEntity re = null;
		int statusCode = 0;
		HttpClient httpClient = new HttpClient();
		if (param == null) {
			param = "";
		}
		HttpMethodBase hmb = null;
		b = param.getBytes("utf-8");
		is = new ByteArrayInputStream(b, 0, b.length);
		if (requestMethod == 0) {
			GetMethod method = new GetMethod(url);
			statusCode = httpClient.executeMethod(method);
			httpClient.getParams().setContentCharset("UTF-8");
			hmb = method;
		} else if (requestMethod == 1) {// application/octet-stream
			re = new InputStreamRequestEntity(is, b.length,application_type);
			PostMethod pm = new PostMethod(url);
			httpClient.getParams().setContentCharset("UTF-8");
			pm.setRequestEntity(re);
			statusCode = httpClient.executeMethod(pm);
			hmb = pm;
		} else if (requestMethod == 2) {
			re = new InputStreamRequestEntity(is, b.length,application_type);
			PutMethod pm = new PutMethod(url);
			pm.setRequestEntity(re);
			statusCode = httpClient.executeMethod(pm);
			httpClient.getParams().setContentCharset("UTF-8");
			hmb = pm;
//			URL urls = new URL(url);
//			HttpURLConnection httpConn = (HttpURLConnection) urls.openConnection();
//			httpConn.setDoOutput(true);// 使用 URL 连接进行输出   
//			httpConn.setDoInput(true);// 使用 URL 连接进行输入   
//			httpConn.setUseCaches(false);// 忽略缓存   
//			httpConn.setRequestMethod("PUT");// 设置URL请求方法   
//			byte[] requestStringBytes = param.getBytes("UTF-8");   
//			httpConn.setRequestProperty("Content-Type", "*/*");
//			httpConn.setRequestProperty("Accept", "application/xml");
//			httpConn.setRequestProperty("User-Agent", "Apache CXF 2.6.16");
//			httpConn.setRequestProperty("Cache-Control", "no-cache");
//			httpConn.setRequestProperty("Pragma", "no-cache");
//			String ip = StringUtil.getParam(url, "http://", ":")[0];
//			String port = StringUtil.getParam(url, "http://"+ip+":", "/")[0];
//			httpConn.setRequestProperty("Host", ip+port);
//			httpConn.setRequestProperty("Connection","keep-alive");
//			httpConn.setRequestProperty("Content-Length",b.length+"");
//			httpConn.setRequestProperty("", URLEncoder.encode(param, "UTF-8"));
//			
//			// 建立输出流，并写入数据   
//			OutputStream outputStream = httpConn.getOutputStream();   
//			outputStream.write(requestStringBytes);   
//			outputStream.close();   
//			// 获得响应状态   
//			int responseCode = httpConn.getResponseCode();   
//			statusCode=responseCode;
//			if (HttpURLConnection.HTTP_OK == responseCode) {// 连接成功
//				// 当正确响应时处理数据
//				StringBuffer sb = new StringBuffer();
//				String readLine;
//				BufferedReader responseReader;
//				// 处理响应流，必须与服务器响应流输出的编码一致
//				responseReader = new BufferedReader(new InputStreamReader(
//						httpConn.getInputStream(), "UTF-8"));
//				while ((readLine = responseReader.readLine()) != null) {
//					sb.append(readLine).append("\n");
//				}
//				responseReader.close();
//				System.out.println(sb.toString());
//			}
		} else if (requestMethod == 3) {
			DeleteMethod dm = new DeleteMethod(url);
			re = new InputStreamRequestEntity(is, b.length,application_type);
			statusCode = httpClient.executeMethod(dm);
			httpClient.getParams().setContentCharset("UTF-8");
			hmb = dm;
		} else {
			return null;
		}
		if (statusCode == 200||statusCode == 204) {
			System.out.println("调用成功！");
			String responseData = hmb.getResponseBodyAsString();
			
			return responseData;
		} else {
			System.out.println("调用失败！错误码：" + statusCode);
			out.setMessageCode(MessageCode.ERR_WEBSERVICE_CONNECTION);
			throw new Exception("调用失败");
		}
	}

	/**
	 * 以下使用POST方式
	 */
	private static void save2() throws Exception {
		String url = "http://127.0.0.1:8080/datashire_sw_bak2/cxf/rest/sample/postData";
		PostMethod pm = new PostMethod(url);
		HttpClient hc = new HttpClient();

		byte[] b = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><UserInfo><id>332</id><name>testname</name><email>testemail</email><address>testaddress</address></UserInfo>".getBytes("utf-8");
//		byte[] b = "id=223".getBytes();
		InputStream is = new ByteArrayInputStream(b, 0, b.length);
		RequestEntity re = new InputStreamRequestEntity(is, b.length,"application/xml");
		pm.setRequestEntity(re);
		//<?xml version="1.0" encoding="UTF-8" standalone="yes"?><UserInfo><id>213</id></UserInfo>
		hc.getParams().setContentCharset("UTF-8");// 设置编码,否则会返回中文乱码//TODO:切记
		int code = hc.executeMethod(pm);
		System.out.println("Post 方式的返回值是:" + code);
		if (code == 200||code==204) {
			String ss = pm.getResponseBodyAsString();
			System.out.println(">>:" + ss);
		}
		pm.releaseConnection();
	}
	
	private static void putData() throws HttpException, IOException{
		String url = "http://192.168.137.58:8080/datashire_sw_bak2/cxf/rest/sample/putData/332";		
		PutMethod pm = new PutMethod(url);
		HttpClient hc = new HttpClient();
		byte[] b= "".getBytes();
//		byte[] b = "id=223".getBytes();
		InputStream is = new ByteArrayInputStream(b, 0, b.length);
		RequestEntity re = new InputStreamRequestEntity(is, b.length);
		
		pm.setRequestEntity(re);
//		<?xml version="1.0" encoding="UTF-8" standalone="yes"?><UserInfo><id>213</id></UserInfo>
		hc.getParams().setContentCharset("UTF-8");// 设置编码,否则会返回中文乱码//TODO:切记
		int code = hc.executeMethod(pm);
		System.out.println("Post 方式的返回值是:" + code);
		if (code == 200||code==204) {
			String ss = pm.getResponseBodyAsString();
			System.out.println(">>:" + ss);
		}
		pm.releaseConnection();
	}
	public static void main(String[] args) {
		try{
			putData();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}