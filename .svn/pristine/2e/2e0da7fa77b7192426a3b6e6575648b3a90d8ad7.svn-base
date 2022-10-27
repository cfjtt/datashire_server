package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.DBSourceTable;
import com.eurlanda.datashire.entity.HttpExtractSquid;
import com.eurlanda.datashire.entity.HttpSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.ThirdPartyParams;
import com.eurlanda.datashire.entity.WebserviceExtractSquid;
import com.eurlanda.datashire.entity.WebserviceSquid;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractServicesub;
import com.eurlanda.datashire.utility.HtmlToXmlUtil;
import com.eurlanda.datashire.utility.HttpUtil;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtil;
import com.eurlanda.datashire.utility.WebServiceUtil;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WebserviceExtractSquidProcess {

	/**
	 * 记录SquidJoinProcess日志
	 */
	static Logger logger = Logger.getLogger(SquidJoinProcess.class);

	// 异常处理机制
	ReturnValue out = new ReturnValue();

	private String token;// 令牌根据令牌得到相应的连接信息

	public WebserviceExtractSquidProcess(String token) {
		this.token = token;
	}
	public Map<String,Object> getSourceDataByHttpExtractSquid(String info,ReturnValue out){
		Map<String, Object> returnMap = new HashMap<String,Object>();
		IRelationalDataManager adapter = null;
		try{
			adapter = DataAdapterFactory.getDefaultDataManager();
			adapter.openSession();
			HttpExtractSquid hes = JsonUtil.object2HashMap(info, HttpExtractSquid.class);
			Map<String,String> params = new HashMap<String,String>();
			params.put("id", hes.getSource_table_id()+"");
			DBSourceTable dst = adapter.query2Object2(true, params, DBSourceTable.class);
			params.clear();
			params.put("id",dst.getSource_squid_id()+"");
			HttpSquid hs = adapter.query2Object2(true,params, HttpSquid.class);
			//到此为止为查询相关bean
			String gang="/";
			if(dst.getUrl()!=null&&dst.getUrl().length()==0||dst.getUrl()!=null&&dst.getUrl().substring(0, 1).equals("/")){
				gang="";
			}
			String url = createWadlUrl("http://"+hs.getHost()+":"+hs.getPort()+gang, dst.getUrl(),hes.getUrlParams(),false);
			StringBuffer paramBuffre = new StringBuffer();
			if(hes.getContentParams()!=null){
				for (int i = 0; i < hes.getContentParams().size(); i++) {
					ThirdPartyParams tpp=hes.getContentParams().get(i);
						if(i!=0){
							paramBuffre.append("&");
						}
					paramBuffre.append(tpp.getName()).append("=").append(tpp.getVal());//这里是替换param
				}
			}
			//获得一级node 通过URL
//			String returnXml = HttpUtil.send(url, paramBuffre.toString(),dst.getMethod(),"application/xml",out);
//			returnXml=returnXml.replace("&", "&amp;");//是否需要
			HtmlToXmlUtil htxu = new HtmlToXmlUtil();
			Document document = htxu.HTMLStringToDocument(url, paramBuffre.toString(),dst.getMethod(),"application/xml",out);
			String[] returnNode=null;
			if(document!=null&&document.getChildNodes()!=null){
				NodeList nodeList = document.getChildNodes();
				returnNode=new String[nodeList.getLength()] ;
				for (int i = 0; i < nodeList.getLength(); i++) {
					Node nodeItem=nodeList.item(i);
					if(nodeItem.getNodeName()!=null&&"HTML".equals(nodeItem.getNodeName().toUpperCase())){
						returnNode = new String[nodeItem.getChildNodes().getLength()];
						for (int j = 0; j < returnNode.length; j++) {
							returnNode[j]=nodeItem.getChildNodes().item(j).getNodeName();
						}
						break;
					}else{
						returnNode[i]=nodeItem.getNodeName();
					}
				}
			}
			//column创建
			List<SourceColumn> listSourceColumn = new ArrayList<SourceColumn>();
			TransformationService ts = new TransformationService(token);
			// 先进行删除
			params.clear();
			params.put("source_table_id", dst.getId() + "");
			adapter.delete(params, SourceColumn.class);
			ExtractServicesub.deleteAllColumnByWebServiceExtract(hes, adapter);
			for (String string : returnNode) {
				SourceColumn sc = ts.initSourceColumn(
						SystemDatatype.NVARCHAR, -1, 0, string,
						true, false, false);
				sc.setSource_table_id(dst.getId());
				sc.setId(adapter.insert2(sc));
				listSourceColumn.add(sc);
			}
			ExtractServicesub.createColumnBySourceColumn(hes, listSourceColumn, adapter, token );
			ExtractService.setAllExtractSquidData(adapter, hes);
			returnMap.put("TransformationList",hes.getTransformations());
			returnMap.put("TransformationLinkList", hes.getTransformationLinks());
			returnMap.put("ColumnList", hes.getColumns());
			returnMap.put("ReferenceColumnList", hes.getSourceColumns());
		}catch (Exception e) {
			try {
				logger.error("createSquidJoin is error", e);
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败
				logger.fatal("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.ERR_WEBSERVICE_CONNECTION);
		} finally {
			adapter.closeSession();
		}
		return returnMap;
	}
	/**
	 * 获得返回值
	 * 2014-12-11
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> getSourceDataByWebserviceExtractSquid(String info , ReturnValue out){
		Map<String, Object> returnMap = new HashMap<String,Object>();
		IRelationalDataManager adapter = null;
		try {
			adapter = DataAdapterFactory.getDefaultDataManager();
			adapter.openSession();
			WebserviceExtractSquid wes = JsonUtil.object2HashMap(info, WebserviceExtractSquid.class);
			Map<String,String> params = new HashMap<String,String>();
			params.put("id", wes.getSource_table_id()+"");
			DBSourceTable dst = adapter.query2Object2(true, params, DBSourceTable.class);
			params.clear();
			params.put("id",dst.getSource_squid_id()+"");
			WebserviceSquid wss = adapter.query2Object2(true,params, WebserviceSquid.class);
			//到此为止为查询相关bean
			ExtractService.setDSFCS(adapter, wes);//装载参数
			if(wss.isIs_restful()){//这里只针对resful进行此操作
				String url = createWadlUrl(wss.getAddress(), dst.getUrl(),wes.getUrlParams(),true);
				String param=null;
				StringBuffer paramBuffre = new StringBuffer();
				if(dst.getParams_xml()!=null){
					param=dst.getParams_xml();
				}
				if(wes.getContentParams()!=null){
					for (int i = 0; i < wes.getContentParams().size(); i++) {
						ThirdPartyParams tpp=wes.getContentParams().get(i);
						if(tpp.getVal()==null||tpp.getVal().equals("")){//如果为空 异常异常
							//第三方参数并非值类型
							out.setMessageCode(MessageCode.ERR_WEBSERVICE_PARAMS_NULL);
							throw new Exception();
						}
						if(StringUtil.contains(dst.getUrl(), tpp.getName())){//由于WADL会将URL中的参数也写入参数中所以这里要将其剔除
							wes.getContentParams().remove(i);
							i--;
							continue;
						}
						if(dst.getParams_xml()!=null){
							param=StringUtil.replace(param, "@"+tpp.getName()+";", tpp.getVal());
						}else{
							if(i!=0){
								paramBuffre.append("&");
							}
							paramBuffre.append(tpp.getName()).append("=").append(tpp.getVal());//这里是替换param
						}
					}
				}
				if(param==null){
					param= paramBuffre.toString();
				}
				String returnXml = HttpUtil.send(url, param,dst.getMethod(),"application/xml",out);
				String[] returnNode = WebServiceUtil.getReturnParam(returnXml);
				//column创建
				List<SourceColumn> listSourceColumn = new ArrayList<SourceColumn>();
				TransformationService ts = new TransformationService(token);
				// 先进行删除
				params.clear();
				params.put("source_table_id", dst.getId() + "");
				adapter.delete(params, SourceColumn.class);
				ExtractServicesub.deleteAllColumnByWebServiceExtract(wes, adapter);
				for (String string : returnNode) {
					SourceColumn sc = ts.initSourceColumn(
							SystemDatatype.NVARCHAR, -1, 0, string,
							true, false, false);
					sc.setSource_table_id(dst.getId());
					sc.setId(adapter.insert2(sc));
					listSourceColumn.add(sc);
				}
				ExtractServicesub.createColumnBySourceColumn(wes, listSourceColumn, adapter, token );
			}else{
				ExtractService.setAllExtractSquidData(adapter, wes);
			}
			ExtractService.setAllExtractSquidData(adapter, wes);
			returnMap.put("TransformationList",wes.getTransformations());
			returnMap.put("TransformationLinkList", wes.getTransformationLinks());
			returnMap.put("ColumnList", wes.getColumns());
			returnMap.put("ReferenceColumnList", wes.getSourceColumns());
		} catch (Exception e) {
			try {
				logger.error("createSquidJoin is error", e);
				adapter.rollback();
			} catch (SQLException e1) { // 数据库回滚失败
				logger.fatal("rollback err!", e1);
			}
//			out.setMessageCode(MessageCode.ERR_CREATEJOIN);
		} finally {
			adapter.closeSession();
		}
		return returnMap;
	}
	/**
	 * 构造WADL URL
	 * 2014-12-11
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param wadlAddress
	 * @param methodUrl
	 * @param urlParams
	 * @param isRemove? 是否去掉?号
	 * @return
	 * @throws Exception
	 */
	private String createWadlUrl(String wadlAddress,String methodUrl,List<ThirdPartyParams> urlParams,boolean b) throws Exception{
		if(wadlAddress==null||methodUrl==null){
			return null;
		}
		String address="";
		if(b){
			address = StringUtil.split(wadlAddress, "?")[0];
		}else{
			address = wadlAddress;
		}
		if(urlParams!=null){
			for (ThirdPartyParams tpp: urlParams) {
				if(tpp.getVal()==null){
					tpp.setVal(" ");
				}
				if(tpp.getValue_type()!=0){
					out.setMessageCode(MessageCode.ERR_WEBSERVICE_PARAMS);
					throw new Exception("变量类型不为值类型");
				}
				methodUrl = StringUtil.replace(methodUrl, "@"+tpp.getName()+";",tpp.getVal() );
			}
		}
		address+=methodUrl;
		return address;
	}
}
