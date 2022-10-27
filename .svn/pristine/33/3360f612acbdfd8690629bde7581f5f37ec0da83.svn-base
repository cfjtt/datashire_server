package com.eurlanda.datashire.sprint7.service.metadata;

import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.adapter.FileAdapter;
import com.eurlanda.datashire.adapter.FtpAdapter;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.adapter.db.INewDBAdapter;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.FileInfo;
import com.eurlanda.datashire.entity.MetadataNode;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.entity.operation.SquidDataSet;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.utility.ColumnAttributeUtils;
import com.eurlanda.datashire.utility.FilterUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.objectsql.SelectSQL;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 元数据导入、浏览等相关操作实现类
 * @author lei.bin
 * @version 1.0
 */
@Service
public class MetadataImportServiceImpl implements IMetadataImportService {
	private static Logger logger = Logger.getLogger(MetadataImportServiceImpl.class);// 记录日志
	private static INewDBAdapter adapter;
	protected String token;
	protected String key;
	
	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	@Autowired
	FileAdapter fileAdapter;
	@Autowired
	FtpAdapter ftpAdapter;

	@Override
	public void addNodeAttr(Map<String, String> nodeParams) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteNodeAttr(Integer nodeId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void modifyNodeAttr(Integer nodeId) {
		// TODO Auto-generated method stub

	}

	@Override
	/**
	 * 游览数据库具体信息
	 */
	public SquidDataSet getConnectionNodeData(MetadataNode node) {
		// TODO Auto-generated method stub
		SquidDataSet squidDataSet = new SquidDataSet();
		try {
			//当前页
			//int currentPage=Integer.parseInt(node.getAttributeMap().get("CurrentPage"));
			//每页显示的行数
			int count=Integer.parseInt(node.getAttributeMap().get("Count"));
			//String tableName=node.getAttributeMap().get("TableName");
			//通过数据源信息，获取数据库信息
			//根据node节点的attributeMap属性获取数据库连接信息
			DBConnectionInfo dbs = new DBConnectionInfo();
			//获取数据库类型
			dbs.setDbType(DataBaseType.parse(Integer.parseInt(node.getAttributeMap().get("Type")))  );
			//获取connectionString
			dbs.setConnectionString(node.getAttributeMap().get("ConnectionString"));
			//获取username
			dbs.setUserName(node.getAttributeMap().get("UserName"));
			//获取password
			dbs.setPassword(node.getAttributeMap().get("Password"));
			//主机名
			dbs.setHost(node.getAttributeMap().get("Host"));
			//数据库名
			dbs.setDbName(node.getAttributeMap().get("Database"));
			//端口
			dbs.setPort(Integer.parseInt(node.getAttributeMap().get("Port")));
			//表名
			String tableName=node.getNodeName();
			//获取adapter
			adapter = AdapterDataSourceManager.getAdapter(dbs);
			SelectSQL sql = new SelectSQL(tableName);
			if (count > 0) {
				sql.setMaxCount(count);
			} else {
				sql.setMaxCount(20);
			}
			List<Map<String, Object>>  dataList = adapter.queryForList(sql,null);
			if (dataList.size() < 1) {
				throw new SystemErrorException(MessageCode.ERR_DATEBASECONTENT,
						tableName + "没有数据");
			}
			List<Map<String, String>> stringMaps = new ArrayList<Map<String, String>>();
			for (Map<String, Object> objectMap : dataList) {
				Map<String, String> newMap = (Map) objectMap;
				stringMaps.add(newMap);
			}
			squidDataSet.setDataList(stringMaps);

		} catch (Exception e) {
			// TODO: handle exception
			logger.error(e);
			throw new SystemErrorException(MessageCode.ERR_DATEBASECONTENT, "游览数据库具体信息异常");
		} finally {
			adapter.close();
		}
		return squidDataSet;
	}

	@Override
	public String getFileNodeData(MetadataNode node) {
		// TODO Auto-generated method stub
		String fileContent = "";
		try {
			fileContent = fileAdapter.getFileContent(node);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e);
			throw new SystemErrorException(MessageCode.ERR_FILECONTENT, "读取文件信息异常");
		}
		return fileContent;
	}

	@Override
	public String getWebNodeData(Integer nodeId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	/**
	 * 浏览数据库连接节点
	 */
	public List<SourceTable> browseConnectionNode(MetadataNode node) {
		List<SourceTable> sourceTables = new ArrayList<SourceTable>();
		try {
			//根据node节点的attributeMap属性获取数据库连接信息
			DBConnectionInfo dbs = new DBConnectionInfo();
			//获取数据库类型
			dbs.setDbType(DataBaseType.parse(Integer.parseInt(node.getAttributeMap().get("Type")))  );
			//获取connectionString
			dbs.setConnectionString(node.getAttributeMap().get("ConnectionString"));
			//获取username
			dbs.setUserName(node.getAttributeMap().get("UserName"));
			//获取password
			dbs.setPassword(node.getAttributeMap().get("Password"));
			//主机名
			dbs.setHost(node.getAttributeMap().get("Host"));
			//数据库名
			dbs.setDbName(node.getAttributeMap().get("Database"));
			//端口
			//dbs.setPort(node.getAttributeMap().get("Database"));
			//获取adapter
			adapter = AdapterDataSourceManager.getAdapter(dbs);
			//获取表信息
			List<BasicTableInfo> tableList = adapter.getAllTables("");
			//获取列信息
			for(BasicTableInfo info:tableList)
			{
				SourceTable sourceTable=new SourceTable();
				Map<String, Column> map = new HashMap<String, Column>();
				List<ColumnInfo> columnList = adapter.getTableColumns(info.getTableName(),null);
				for(ColumnInfo columnInfo:columnList)
				{
					Column column = new Column();
					column.setName(columnInfo.getName());//列名
					column.setData_type(columnInfo.getSystemDatatype().value());//类型
					if(StringUtils.isNotBlank(columnInfo.getCollation()))
					{
						column.setCollation(Integer.parseInt(columnInfo.getCollation()));//字符集
					}
					column.setNullable(columnInfo.isNullable());//是否可空
					if(null!=columnInfo.getPrecision())
					{
						column.setPrecision(columnInfo.getPrecision());// 精度
					}
					if(null!=columnInfo.getLength())
					{
						column.setLength(columnInfo.getLength());// 长度
					}
					column.setFK(columnInfo.isForeign());//是否外键
					column.setIsPK(columnInfo.isPrimary());//是否主键
					//拼接列的属性
					column.setColumnAttribute(new ColumnAttributeUtils().getColumnAttribute(column));
					map.put(columnInfo.getName(), column);
				}
				
				sourceTable.setColumnInfo(map);
				sourceTable.setTableName(info.getTableName());
				sourceTables.add(sourceTable);
			}
		} catch (Exception e) {
			// TODO: handle exception
			logger.error(e);
			throw new SystemErrorException(MessageCode.ERR_BROWSECONNECTION, "游览数据库信息异常");
		}finally
		{
			adapter.close();
		}
		return sourceTables;
	}

	@Override
	/**
	 * 浏览本地文件信息
	 */
	public List<FileInfo> browseFileNode(MetadataNode node) {
		// TODO Auto-generated method stub
		List<FileInfo> fileInfos=new ArrayList<FileInfo>();
		try {
			//根据路径和过滤文件类型查询文件列表
			if(StringUtils.isNotBlank(node.getAttributeMap().get("Path")))
			{
				fileAdapter.setPath(node.getAttributeMap().get("Path"));//路径
			}
			if(StringUtils.isNotBlank(node.getAttributeMap().get("Filter")))
			{
				fileAdapter.setFileType(node.getAttributeMap().get("Filter"));//过滤条件
			}
			fileAdapter.refreshList();
			while (fileAdapter.nextFile()) {
				FileInfo fileInfo = new FileInfo();
				if (!fileAdapter.isDirectory()) {
					if (StringUtils.isNotBlank(fileAdapter.getFileType())) {
						FilterUtil filter = new FilterUtil(fileAdapter.getFileType());
						if (filter.check(fileAdapter.getFileName())) {
							fileInfo.setFileName(fileAdapter.getFileName());// 获取文件名
							fileInfo.setFileType(fileAdapter.getFileTypeDesc());// 文件类型
							fileInfo.setModifyDate(fileAdapter.getFileTimeStamp());// 修改时间
							fileInfo.setFilePath(fileAdapter.getfilePath());// 文件所在路径
							fileInfo.setFileSize(fileAdapter.getFileSize());// 文件大小
						}
					}else
					{
						fileInfo.setFileName(fileAdapter.getFileName());// 获取文件名
						fileInfo.setFileType(fileAdapter.getFileTypeDesc());// 文件类型
						fileInfo.setModifyDate(fileAdapter.getFileTimeStamp());// 修改时间
						fileInfo.setFilePath(fileAdapter.getfilePath());// 文件所在路径
						fileInfo.setFileSize(fileAdapter.getFileSize());// 文件大小
					}
				}else
				{//文件夹
					fileInfo.setFileName(fileAdapter.getFileName());// 获取文件名
					fileInfo.setFileType(fileAdapter.getFileTypeDesc());// 文件类型
					fileInfo.setModifyDate(fileAdapter.getFileTimeStamp());// 修改时间
					fileInfo.setFilePath(fileAdapter.getfilePath());// 文件所在路径
				}
				
				fileInfos.add(fileInfo);
			}
		    
		} catch (Exception e) {
			// TODO: handle exception
			logger.error(e);
			throw new SystemErrorException(MessageCode.ERR_FILE, "游览文件信息异常");
		}
		return fileInfos;
	}

	@Override
	public String getFtpFileContent(MetadataNode node) {
		// TODO Auto-generated method stub
		String fileContent = "";
		try {
			ftpAdapter.setIp(node.getAttributeMap().get("Host"));//ip
			ftpAdapter.setUserName(node.getAttributeMap().get("UserName"));
			ftpAdapter.setUserPwd(node.getAttributeMap().get("Password"));
//			System.out.println(node.getAttributeMap().get(
//					"FileName").length()+1);
			ftpAdapter.setPath(node.getAttributeMap().get("Path").substring(0, node.getAttributeMap().get("Path").length() - node.getAttributeMap().get(
					"FileName").length() - 1));
			ftpAdapter.setPort(Integer.parseInt(node.getAttributeMap().get("Port")));
			ftpAdapter.reSet();
			//根据文件类型来判断调用具体的读取方法
			if (".pdf|.doc|.docx|.xls|.xlsx".contains(FileUtils.getFileEx(node.getAttributeMap().get("FileName")))) {
				fileContent = ftpAdapter.downloadFileAndRead(node.getAttributeMap().get("Path"), node.getAttributeMap().get("FileName"), -1);
			} else {
				fileContent = ftpAdapter.readFile(node.getAttributeMap().get("FileName"));
			}
		} catch (Exception e) {
			// TODO: handle exception
			logger.error(e);
			throw new SystemErrorException(MessageCode.ERR_FTPCONTENT,
					"读取FTP文件信息异常");
		}
		return fileContent;
	}

	/**
	 * 通过FTP浏览文件夹下面的所有文件。 返回该节点定义的目录下面的所有文件和目录
	 */
	@Override
	public List<FileInfo> browseftpFileNode(MetadataNode node) {
		// TODO Auto-generated method stub
		List<FileInfo> fileInfos=new ArrayList<FileInfo>();
		try {
			ftpAdapter.setIp(node.getAttributeMap().get("Host"));
			ftpAdapter.setUserName(node.getAttributeMap().get("UserName"));
			ftpAdapter.setUserPwd(node.getAttributeMap().get("Password"));
			ftpAdapter.setPath(node.getAttributeMap().get("Path"));
			ftpAdapter.setPort(Integer.parseInt(node.getAttributeMap().get("Port")));
			ftpAdapter.setFileType(node.getAttributeMap().get("Filter"));
			ftpAdapter.reSet();
			int depth= Integer.parseInt(node.getAttributeMap().get("Depth"));
			String path=node.getAttributeMap().get("Path");
			fileInfos=ftpAdapter.getFileList(path,depth);
		} catch (Exception e) {
			// TODO: handle exception
			logger.error(e);
			throw new SystemErrorException(MessageCode.ERR_FILE, "游览FTP节点信息异常");
		}
	
		return fileInfos;
	}
	

}
