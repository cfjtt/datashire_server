package com.eurlanda.datashire.sprint7.service.workspace;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.RepoValidationResponse;
import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.plug.RepositoryValidationPlug;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.google.gson.FieldNamingPolicy;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Repository数据有效性校验业务逻辑实现，校验的具体内容为ds前缀的表(不包含ds_sys系统表),枚举类型的表，以及具有主外键约束的表
 * 
 * @version 1.0
 * @author lei.bin
 * @created 2013-09-17
 */
public class RepositoryValidationProcess {
	/**
	 * 记录RepositoryValidationProcess日志
	 */
	static Logger logger = Logger.getLogger(RepositoryValidationProcess.class);
	/**
	 * 校验用到的所有表名
	 */
	String[] tableName = new String[] {
			"DS_COLUMN", "DS_DATA_SQUID",
			"DS_DOC_EXTRACT", "DS_FILEFOLDER_CONNECTION",
			"DS_FTP_CONNECTION", "DS_HDFS_CONNECTION",
			"DS_INDEXES", "DS_JOIN", "DS_JOIN_TYPE",
			"DS_PROJECT", "DS_REFERENCE_COLUMN", "DS_REFERENCE_COLUMN_GROUP",
			"DS_REPORT_SQUID", "DS_SOURCE_TABLE", "DS_SOURCE_COLUMN",
			"DS_SQL_CONNECTION", "DS_SQUID",
			"DS_SQUID_FLOW" ,"DS_SQUID_LINK", "DS_SQUID_SCHEDULE", "DS_SQUID_TYPE",
			"DS_TRANSFORMATION", "DS_TRANSFORMATION_LINK", "DS_TRANSFORMATION_TYPE",
			"DS_TRAN_INPUTS", "DS_TRAN_INPUT_DEFINITION",
			"DS_URL", "DS_WEB_CONNECTION", "DS_WEIBO_CONNECTION"};
	/**
	 * DB_type枚举表的值(数据库类型)
	 */
	String[] EnumDB = new String[] { "SQLSERVER", "ORACLE", "MYSQL", "DB2",
			"H2", "SQLLITE", "SYBASE", "MSACCESS", "HSQLDB" };
	/**
	 * JOIN_type枚举表的值(例如squidjoin的连接方式)
	 */
	String[] EnumJOIN = new String[] { "BASETABLE", "INNERJOIN",
			"LEFTOUTERJOIN", "RIGHTOUTERJOIN", "FULLJOIN", "CROSSJOIN", "UNOIN", "UNOINALL"};
	/**
	 * TRANSFORMATION_type枚举表的值(转换类型)
	 */
	String[] EnumTRANSFORMATION = new String[] {
			"UNKNOWN","VIRTUAL","UPPER","CONCATENATE","LOWER","CONSTANT",
			"CHOICE","TERMEXTRACT","SPLIT","ASCII","UNICODE","SIMILARITY",
			"CHAR","PATTERNINDEX","REPLICATE","NUMERICTOSTRING",
			"STRINGTONUMERIC","REPLACE","LEFT","RIGHT","SUBSTRING","LENGTH",
			"REVERSE","LEFTTRIM","RIGHTTRIM","TRIM","ROWNUMBER","SYSTEMDATETIME",
			"STRINGTODATETIME","DATETIMETOSTRING","YEAR","MONTH","DAY",
			"DATEDIFF","FORMATDATE","ABS","RANDOM","ACOS","EXP","ROUND","ASIN",
			"FLOOR","SIGN","ATAN","LOG","SIN","LOG10","SQRT","CEILING","PI",
			"SQUARE","COS","POWER","TAN","COT","RADIANS","CALCULATION","MOD",
			"PROJECTID","PROJECTNAME","SQUIDFLOWID","SQUIDFLOWNAME","JOBID",
			"TOKENIZATION","PREDICT","NUMASSEMBLE","TRAIN"
	};
	/**
	 * DATA_TYPE枚举值(字段类型)
	 *//*
	String[] EnumDATA = new String[] { "INT", "STRING", "DATE", "BOOLEAN",
			"BYTE", "SHORT", "LONG", "FLOAT", "DOUBLE", "TIME", "TIMESTAMP",
			"DIGDECIMAL", "NCHAR", "NVARCHAR", "MONEY", "XML" };*/
	/**
	 * SQUID_type枚举表的值(squid的子类类型)
	 */
	String[] EnumSQUID = new String[] {
			"UNKNOWN","DBSOURCE","DBDESTINATION","EXTRACT","STAGE",
			"DIMENSION","FACT","REPORT","DOC_EXTRACT","XML_EXTRACT",
			"WEBLOGEXTRACT","WEBEXTRACT","WEIBOEXTRACT","FILEFOLDER",
			"FTP","HDFS","WEB","WEBLOG","WEIBO","EXCEPTION","LOGREG",
			"NAIVEBAYES","SVM","KMEANS","ALS","LINEREG","RIDGEREG",
			"QUANTIFY","DISCRETIZE","ASSOCIATION_RULES"
	};
	String[] EnumTRANSDEFINITION = new String[]{
			"VIRTUAL","CONSTANT","CHOICE","CONCATENATE","TERMEXTRACT",
			"SPLIT","ASCII","UNICODE","SIMILARITY","SIMILARITY","CHAR",
			"PATTERNINDEX","PATTERNINDEX","REPLICATE","NUMERICTOSTRING",
			"STRINGTONUMERIC","REPLACE","REPLACE","LEFT","RIGHT",
			"SUBSTRING","LENGTH","REVERSE","LOWER","UPPER","LEFTTRIM",
			"RIGHTTRIM","TRIM","ROWNUMBER","SYSTEMDATETIME",
			"STRINGTODATETIME","DATETIMETOSTRING","YEAR","MONTH","DAY",
			"DATEDIFF","DATEDIFF","FORMATDATE","ABS","RANDOM","ACOS",
			"EXP","ROUND","ASIN","FLOOR","SIGN","ATAN","LOG","SIN",
			"LOG10","SQRT","CEILING","PI","SQUARE","COS","POWER","TAN",
			"COT","RADIANS","CALCULATION","CALCULATION","MOD",
			"PROJECTID","PROJECTNAME","SQUIDFLOWID","SQUIDFLOWNAME",
			"JOBID","TOKENIZATION","PREDICT ","NUMASSEMBLE","TRAIN"
	};
	private String token;

	public RepositoryValidationProcess(String token) {
		this.token = token;
	}
	
	public static void main(String[] args) {
		DataAdapterFactory adapterFactory = DataAdapterFactory.newInstance();
		IRelationalDataManager adapter3 = adapterFactory.getDataManagerByDbName("test");
		adapter3.openSession();
		try {
			List<Map<String, Object>> list = adapter3.query2List(true,
					"select DESCRIPTION from ds_squid_type ", null);
			String temp = "";
			for (Map<String, Object> map : list) {
				String code = map.get("DESCRIPTION")+"";
				temp += ",\""+code+"\"";
			}
			if (temp!=""){
				temp = temp.substring(1);
			}
			System.out.println(temp);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} finally{
			adapter3.closeSession();
		}

	}

	/**
	 * 返回Repository数据有效性校验结果,如果有一个表校验错误,不在对其他表进行校验，直接返回错误，
	 * @param out
	 * @param key key值,后面推送数据需要用到
	 */
	public void checkAllTable(String info,ReturnValue out, String key) {

		logger.debug(String.format("checkAllTable-start========"));
		//定义一个取消状态，用户判断前端是否按下取消校验按钮
        boolean cancleValue=false;//将取消状态置为false
        //每次校验之前都将状态置为false
    	 ValidationCancleService.setCancleValue(false);;
		try {
			Repository repository=JsonUtil.json2Object(info, Repository.class,FieldNamingPolicy.UPPER_CAMEL_CASE);
			for (int i = 0; i < tableName.length; i++) {
				logger.debug("校验的表名为================================"
						+ tableName[i]);
				// 调用取消方法,判断是否按下取消按钮
				cancleValue = ValidationCancleService.isCancleValue();
				logger.debug("cancleValue==============================="
						+  ValidationCancleService.isCancleValue());
				//如果没有取消
				if (!cancleValue) {
					RepoValidationResponse repoValidationResponse = new RepoValidationResponse();
					logger.debug("校验表的正确性进来了");
					//校验所有ds前缀的表
					this.checkDSTable(repository,tableName[i], out, repoValidationResponse);
					if (null != repoValidationResponse
							&& repoValidationResponse.getResult_code() == 1) {
						if (tableName[i].equals("DS_JOIN_TYPE")
								|| tableName[i].equals("DS_TRANSFORMATION_TYPE")
								|| tableName[i].equals("DS_TRAN_INPUT_DEFINITION")
								|| tableName[i].equals("DS_SQUID_TYPE")) {
							logger.debug("校验枚举表进来了");
							//校验枚举表
							this.checkEnumTable(repository, tableName[i], out,
									repoValidationResponse);
							// 占时不进行验证外键
							/*if (null != repoValidationResponse
									&& repoValidationResponse.getResult_code() == 1) {
								logger.debug("校验外键");
								//校验外键关系
								this.checkForeignKey(repository, tableName[i], out,
										repoValidationResponse);
							}*/
						}
					}else{
						repoValidationResponse.setResult_code(out.getMessageCode().value());
						repoValidationResponse.setTable_name(tableName[i]);
					}
					//封装返回的数据
//					InfoMessagePacket<RepoValidationResponse> infoMessagePacket = new InfoMessagePacket<RepoValidationResponse>();
//					infoMessagePacket.setCode(1);
//					infoMessagePacket.setInfo(repoValidationResponse);
//					infoMessagePacket.setType(DSObjectType.REPOSITORY);
//					JSONObject jsonObject = JSONObject
//							.fromObject(infoMessagePacket);

					// 调用推送方法返回数据
//					PushMessagePacket pushMessagePacket = new PushMessagePacket();
					String commandId = "0006";
					String childCommandId = "0002";
//					pushMessagePacket.pushInformation(token, key,
//							jsonObject.toString(), DSObjectType.REPOSITORY,
//							commandId, childCommandId);
					List list = new ArrayList();
					list.add(repoValidationResponse);
					PushMessagePacket.push(list, DSObjectType.REPOSITORY, commandId, childCommandId, key, TokenUtil.getToken());
					logger.debug("pushInformation====%s"
							+ repoValidationResponse.toString());
				} else {
					break;// 终止循环
				}
			}

		} catch (Exception e) {
			logger.error("checkAllTable", e);
			out.setMessageCode(MessageCode.ERR_REPOSITORY_CHECK);
		}

	}

	/**
	 * 注册repository校验
	 * @param out 返回的信息
	 * @return boolean值
	 */
	public boolean checkAllTableStructure(Repository repository, ReturnValue out) {
		logger.debug(String.format("checkAllTable-start========"));
      boolean cancleValue=false;
		try {
			for (int i = 0; i < tableName.length; i++) {
				logger.debug("校验的表名为================================"
						+ tableName[i]);
				// 调用取消方法
				logger.debug("cancleValue==============================="
						+ new ValidationCancleService().isCancleValue());

				RepoValidationResponse repoValidationResponse = new RepoValidationResponse();
				logger.debug("校验表的正确性进来了");
				this.checkDSTable(repository,tableName[i], out, repoValidationResponse);
				if (null != repoValidationResponse
						&& repoValidationResponse.getResult_code() == 1) {
					if (tableName[i].equals("DS_DB_TYPE")
							|| tableName[i].equals("DS_JOIN_TYPE")
							|| tableName[i].equals("DS_TRANSFORMATION_TYPE")
							|| tableName[i].equals("DS_SQUID_TYPE")) {
						logger.debug("校验枚举表进来了");
						this.checkEnumTable(repository,tableName[i], out,
								repoValidationResponse);
						if (null != repoValidationResponse
								&& repoValidationResponse.getResult_code() == 1) {
							logger.debug("校验外键");
							this.checkForeignKey(repository,tableName[i], out,
									repoValidationResponse);
							cancleValue = true;
						}else{
							cancleValue = false;
							break;
						}
					}else{
						cancleValue = false;
						break;
					}
				}else{
					cancleValue = false;
					break;
				}


			}
			//将取消状态置为false
			new ValidationCancleService().setCancleValue(false);
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_REPOSITORY_CHECK);
		}
		return cancleValue;

	}

	/**
	 * 返回Repository中所有的DS_前缀(非DS_SYS)的表
	 * @return DS_前缀(非DS_SYS)的列表
	 */
	public List<RepoValidationResponse> queryAllTableName() {
		//定义返回结果
		int result_code = 1;
		List<RepoValidationResponse> list = new ArrayList<RepoValidationResponse>();
		for (int i = 0; i < tableName.length; i++) {
			RepoValidationResponse repoValidationResponse = new RepoValidationResponse();
			repoValidationResponse.setResult_code(result_code);
			repoValidationResponse.setTable_name(tableName[i]);
			list.add(repoValidationResponse);
		}
		return list;
	}

	/**
	 * 返回Repository中所有的DS_前缀(非DS_SYS)的表是否存在
	 * @param name 表名
	 * @param out  返回结果代码
	 * @param repoValidationResponse 返回的对象
	 */
	public void checkDSTable(Repository repository,String name, ReturnValue out,
			RepoValidationResponse repoValidationResponse) {
		try {
			String DBname=repository.getRepository_db_name();
			RepositoryValidationPlug plug = new RepositoryValidationPlug(DBname);
			//获取数据库中所有的DS前缀的表
			Map<String, Object> columnsValue = plug.getAllDSTable(name, out);
			logger.debug("查到的数据为==" + columnsValue);
			if (null == columnsValue) {
				// 无数据返回
				repoValidationResponse.setResult_code(MessageCode.ERR_TABLENAME_NULL.value());
			} else if (columnsValue.size() == 0) {
				//this.setResponseValue(name, repoValidationResponse);
				repoValidationResponse.setResult_code(MessageCode.NODATA.value());
			} else if (columnsValue.size() > 0) {
				repoValidationResponse.setResult_code(1);
				repoValidationResponse.setTable_name(name);
			}
		} catch (Exception e) {
			logger.error("checkDSTable", e);
			out.setMessageCode(MessageCode.ERR);
		}
	}

	/**
	 * 枚举表内容检查DS_CONNECTION_TYPE，DS_JOIN_TYPE，DS_TRANSFORMATION_TYPE
     ，DS_SQUID_TYPE
	 * @param name 表名
	 * @param out 返回的结果代码
	 * @param repoValidationResponse 返回的对象
	 */
	public void checkEnumTable(Repository repository,String name, ReturnValue out,
			RepoValidationResponse repoValidationResponse) {
		boolean result = false;// 比较结果
		String DBname=repository.getRepository_db_name();
		try {
			RepositoryValidationPlug plug = new RepositoryValidationPlug(DBname);
			List<Map<String, Object>> columnsValue = plug.getEnumTableValue(
					name, out);
			if (columnsValue == null) {
				out.setMessageCode(MessageCode.NODATA);
			}
			logger.debug("从表中获取的值为====" + columnsValue);
			String[] realEnumValue = new String[columnsValue.size()];
			for (int i = 0; i < columnsValue.size(); i++) {
				//System.out.println(name);
				if(name.equals("DS_TRANSFORMATION_TYPE")||name.equals("DS_TRAN_INPUT_DEFINITION")){
					realEnumValue[i] = columnsValue.get(i).get("CODE").toString();
				}else{
					realEnumValue[i] = columnsValue.get(i).get("DESCRIPTION").toString();
				}
				logger.debug(String.format("checkEnumTable-start=====%s",
						realEnumValue[i]));
			}
			// 比较从表中查出来的枚举值是否一样
			if (name.equals("DS_JOIN_TYPE")) {
				result = this.compare(realEnumValue, EnumJOIN, out);
			} else if (name.equals("DS_TRANSFORMATION_TYPE")) {
				result = this.compare(realEnumValue, EnumTRANSFORMATION, out);
			} else if (name.equals("DS_TRAN_INPUT_DEFINITION")){
				result  = this.compare(realEnumValue, EnumTRANSDEFINITION, out);
			} else if (name.equals("DS_SQUID_TYPE")) {
				result = this.compare(realEnumValue, EnumSQUID, out);
			}
			if (result) {
				repoValidationResponse.setResult_code(1);
				repoValidationResponse.setTable_name(name);
			} else {
				this.setEnumValue(name, repoValidationResponse);
			}
		} catch (Exception e) {
			logger.error("checkEnumTable", e);
			out.setMessageCode(MessageCode.NODATA);
		}
	}

	/**
	 * 数据一致性检查
	 * 具有ForeignKey 关系的表数据一致性检查，校验每一个外键的值在关联表中的存在性
	 * @param name 表名
	 * @param out 返回结果代码
	 * @param repoValidationResponse 返回的对象
	 */
	public void checkForeignKey(Repository repository,String name, ReturnValue out,
			RepoValidationResponse repoValidationResponse) {
	         String DBname= repository.getRepository_db_name();
		try {
			logger.debug("校验外键进来了");
			RepositoryValidationPlug plug = new RepositoryValidationPlug(DBname);
			if (name.equals("DS_SQUID_FLOW") || name.equals("DS_SQUID")
					|| name.equals("DS_REFERENCE_COLUMN")
					|| name.equals("DS_SOURCE_TABLE")
					|| name.equals("DS_COLUMN")
					|| name.equals("DS_TRANSFORMATION")
					|| name.equals("DS_SQUID_LINK") || name.equals("DS_JOIN")
					|| name.equals("DS_TRANSFORMATION_LINK")
					|| name.equals("DS_REFERENCE_COLUMN_GROUP")
					|| name.equals("DS_SQL_CONNECTION")
					|| name.equals("DS_DATA_SQUID")
					|| name.equals("DS_DATA_SQUID")) {
				logger.debug("具体的表");
				// 获取具有外键的表的记录
				List result = plug.getForeignKeyValue(name, out);
				if (null != result && result.size() > 0) {
					this.setForeignKey(name, repoValidationResponse);
				} else if (null == result || result.size() == 0) {
					logger.debug("查到的值正确");
					repoValidationResponse.setResult_code(1);
					repoValidationResponse.setTable_name(name);
				}
			} else {
				repoValidationResponse.setResult_code(1);
				repoValidationResponse.setTable_name(name);
			}

		} catch (Exception e) {
			logger.debug("查到checkForeignKey值正确", e);
			out.setMessageCode(MessageCode.NODATA);
		}
	}

	/**
	 * 根据表名封装对应的返回信息
	 *
	 * @param tableName
	 *            表名
	 * @param repoValidationResponse
	 *            返回对象
	 */
	public void setResponseValue(String tableName,
			RepoValidationResponse repoValidationResponse) {
		if (tableName.equals("DS_SQUID")) {
			repoValidationResponse.setResult_code(80044);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_SQUID_LINK")) {
			repoValidationResponse.setResult_code(80084);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_SQUID_FLOW")) {
			repoValidationResponse.setResult_code(80076);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_COLUMN")) {
			repoValidationResponse.setResult_code(80074);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_PROJECT")) {
			repoValidationResponse.setResult_code(80077);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_TRANSFORMATION")) {
			repoValidationResponse.setResult_code(80079);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_TRANSFORMATION_LINK")) {
			repoValidationResponse.setResult_code(80080);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_TRANSFORMATION_TYPE")) {
			repoValidationResponse.setResult_code(80081);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_JOIN")) {
			repoValidationResponse.setResult_code(80083);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_SOURCE_TABLE")) {
			repoValidationResponse.setResult_code(80073);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_SOURCE_COLUMN")) {
			repoValidationResponse.setResult_code(800126);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_SQL_CONNECTION")) {
			repoValidationResponse.setResult_code(800127);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_DATA_SQUID")) {
			repoValidationResponse.setResult_code(800128);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_DATA_SQUID")) {
			repoValidationResponse.setResult_code(800129);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_DB_TYPE")) {
			repoValidationResponse.setResult_code(80070);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_SQUID_TYPE")) {
			repoValidationResponse.setResult_code(80078);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_JOIN_TYPE")) {
			repoValidationResponse.setResult_code(80082);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_REFERENCE_COLUMN")) {
			repoValidationResponse.setResult_code(800130);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_REFERENCE_COLUMN_GROUP")) {
			repoValidationResponse.setResult_code(800131);
			repoValidationResponse.setTable_name(tableName);
		}
	}

	/**
	 * 封装枚举表错误返回值
	 * @param tableName 表名
	 * @param repoValidationResponse 返回对象
	 */
	public void setEnumValue(String tableName,
			RepoValidationResponse repoValidationResponse) {
		if (tableName.equals("DS_DB_TYPE")) {
			repoValidationResponse.setResult_code(80087);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_JOIN_TYPE")) {
			repoValidationResponse.setResult_code(80088);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_TRANSFORMATION_TYPE")) {
			repoValidationResponse.setResult_code(80089);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_SQUID_TYPE")) {
			repoValidationResponse.setResult_code(80090);
			repoValidationResponse.setTable_name(tableName);
		}
	}

    /**
     * 根据表名设置返回值的错误信息
     * @param tableName  表名
     * @param repoValidationResponse 返回对象
     */
	public void setForeignKey(String tableName,
			RepoValidationResponse repoValidationResponse) {
		if (tableName.equals("DS_SQUID_FLOW")) {
			repoValidationResponse.setResult_code(80092);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_SQUID")) {
			repoValidationResponse.setResult_code(80093);
			repoValidationResponse.setTable_name(tableName);
		}  else if (tableName.equals("DS_SOURCE_TABLE")) {
			repoValidationResponse.setResult_code(80095);
			repoValidationResponse.setTable_name(tableName);
		}else if (tableName.equals("DS_SOURCE_COLUMN")) {
			repoValidationResponse.setResult_code(800132);
			repoValidationResponse.setTable_name(tableName);
		}else if (tableName.equals("DS_COLUMN")) {
			repoValidationResponse.setResult_code(80096);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_TRANSFORMATION")) {
			repoValidationResponse.setResult_code(80097);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_SQUID_LINK")) {
			repoValidationResponse.setResult_code(80098);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_JOIN")) {
			repoValidationResponse.setResult_code(80099);
			repoValidationResponse.setTable_name(tableName);
		} else if (tableName.equals("DS_TRANSFORMATION_LINK")) {
			repoValidationResponse.setResult_code(80100);
			repoValidationResponse.setTable_name(tableName);
		}else if (tableName.equals("DS_REFERENCE_COLUMN")) {
			repoValidationResponse.setResult_code(800133);
			repoValidationResponse.setTable_name(tableName);
		}else if (tableName.equals("DS_REFERENCE_COLUMN_GROUP")) {
			repoValidationResponse.setResult_code(800134);
			repoValidationResponse.setTable_name(tableName);
		}else if (tableName.equals("DS_SQL_CONNECTION")) {
			repoValidationResponse.setResult_code(800135);
			repoValidationResponse.setTable_name(tableName);
		}else if (tableName.equals("DS_DATA_SQUID")) {
			repoValidationResponse.setResult_code(800136);
			repoValidationResponse.setTable_name(tableName);
		}else if (tableName.equals("DS_DATA_SQUID")) {
			repoValidationResponse.setResult_code(800137);
			repoValidationResponse.setTable_name(tableName);
		}
	}
	
	/**
	 * 比较2个数组对象值是否一样
	 * @param arr1  数组对象
	 * @param arr2 数组对象
	 * @param out  返回结果代码
	 * @return  比较结果
	 */
	public boolean compare(String[] arr1, String[] arr2, ReturnValue out) {
		boolean result = false;
		try {
			Arrays.sort(arr1);
			Arrays.sort(arr2);
			result = Arrays.equals(arr1, arr2);
		} catch (Exception e) {
			logger.error("compare",e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}
		return result;
	}
}
