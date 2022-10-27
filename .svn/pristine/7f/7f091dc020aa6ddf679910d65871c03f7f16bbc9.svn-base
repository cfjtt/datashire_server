package com.eurlanda.datashire.utility;

import org.w3c.dom.Document;
import org.w3c.tidy.Configuration;
import org.w3c.tidy.Tidy;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HtmlToXmlUtil
{  
	public Document HTMLStringToDocument(String httpUrl, String param, int requestMethod,String application_type,ReturnValue errOut) throws Exception{
//		URL url = new URL("http://www.hao123.com/?tn=94418974_s_hao_pg");
		URL url = new URL(httpUrl);
		PrintWriter pwOut = null;
		InputStream is = null;
		HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
		int code=0;
		if (requestMethod!=0) {//post
			httpURLConnection.setDoOutput(true);//post必须设置这两项
            httpURLConnection.setDoInput(true);
			pwOut = new PrintWriter(httpURLConnection.getOutputStream());//
			pwOut.print(param);
//			BufferedReader in = new BufferedReader( new InputStreamReader(httpURLConnection.getInputStream()));
		}else{
//			is= (InputStream) url.getContent();
			httpURLConnection.setConnectTimeout(300000);
			httpURLConnection.setReadTimeout(300000);
			httpURLConnection.connect();
		}
		is=httpURLConnection.getInputStream();
		code = httpURLConnection.getResponseCode();
		if(!(code+"").startsWith("2")){
			System.out.println("调用HTTP_URL失败！错误码：" + code);
			errOut.setMessageCode(MessageCode.ERR_WEBSERVICE_CONNECTION);
			throw new Exception("ResponseCode is not begin with 2,code="+code);
        }else{
//			System.out.println("调用成功！");
        }
		BufferedInputStream bis = new BufferedInputStream(is, 1024);
//		String encoding = getEncodingOfStream(bis);//获得编码
		Tidy tidy = new Tidy();
//		tidy.getConfiguration().printConfigOptions(new PrintWriter(System.out), true);
		//output-xml:yes
		//clean:yes
		//doctype:omit
	    FileOutputStream out = null;
	    InputStream input = null;
		try {
			input = this.getClass().getResourceAsStream("/config/tidy.properties");
			Properties props = new Properties();
			props.load(input);
			tidy.setConfigurationFromProps(props);
			tidy.setDropFontTags(true); // 删除字体节点
			tidy.setDropEmptyParas(true); // 删除空段落
			tidy.setFixComments(true); // 修复注释
			tidy.setFixBackslash(true); // 修复反斜杆
			tidy.setMakeClean(true); // 删除混乱的表示
			tidy.setQuoteNbsp(false); // 将空格输出为 ?
			tidy.setQuoteMarks(false); // 将双引号输出为 "
			tidy.setQuoteAmpersand(true); // 将 & 输出为 &
			tidy.setShowWarnings(false); // 不显示警告信息
			tidy.setCharEncoding(Configuration.UTF8);
//			tidy.setInputEncoding(encoding);//指定编码
			out = new FileOutputStream("out.html");
			Document d= tidy.parseDOM(is, out);
			return d;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				input.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}
	
//	private void showNodeList(NodeList nodeList){
//		for (int i = 0; i < nodeList.getLength(); i++) {
//			Node node = nodeList.item(i);
//			System.out.println(node.getNodeValue());
//			showNodeList(node.getChildNodes());
//		}
//	}
	
	 /** 
     * 利用正则表达式匹配html输入流中的charset信息 
     * @param bin 由于用到了InputStream的mark()、reset()方法，<br> 
     * 所以需要BufferedInputStream 
     * @return 该html文件的字符编码，如果没找到则返回iso8859-1 
     * @throws IOException 
     */  
    public String getEncodingOfStream(BufferedInputStream bin) throws IOException {  
        byte[] bytes = new byte[1024];  //存放读入的信息，一次读入1024个字节  
        bin.mark(1024); //标记初始位置,设标记失效的最大字节数为1024  
        int len = bin.read(bytes);  
        String encoding;  
        String encoding_tag = "<meta([^<]*)charset=('|\")([^<]*)('|\")([^<]*)/>";   //使用正则表达式匹配charset  
        String detector = new String(bytes, 0, len, "iso8859-1");   //默认用iso8859-1，避免丢失信息  
        Pattern encodingPattern = Pattern.compile(encoding_tag, Pattern.CASE_INSENSITIVE);  
        Matcher m = encodingPattern.matcher(detector);  
        if (m.find()) {  
        	encoding = m.group(0).split("charset")[1].split("(=\")|(=')|(:)|(=)")[1].split("(\\s)|;|\"|'")[0];
        } else {  
            encoding = "iso8859-1"; //如果没找到就当做iso8859-1算了  
        }  
        bin.reset();  
        return encoding;  
    }  
	public static void main(String[] args) {
		HtmlToXmlUtil html2Xml = new HtmlToXmlUtil();
		try{
			ReturnValue rv = new ReturnValue();
			html2Xml.HTMLStringToDocument("http://www.hao123.com/", "tn=94418974_s_hao_pg", 1, "application/xml",rv);
//			html2Xml.HTMLStringToDocument("http://www.hao123.com/?tn=94418974_s_hao_pg");
			//http://technicolor.iteye.com/blog/730337
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}  