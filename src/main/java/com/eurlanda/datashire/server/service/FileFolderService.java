package com.eurlanda.datashire.server.service;

import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.server.dao.SourceColumnDao;
import com.eurlanda.datashire.server.dao.SourceTableDao;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.utils.fileSource.FileFolderAdapterImpl;
import com.eurlanda.datashire.server.utils.utility.DocExtractSquidUtil;
import com.eurlanda.datashire.utility.MessageCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 文件类型 获取元数据
 * Created by Eurlanda - Java、 on 2017/8/3.
 */
@Service
public class FileFolderService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileFolderService.class);
    FileFolderAdapterImpl fileFolderAdapter=new FileFolderAdapterImpl();
    @Autowired
    private SourceTableDao sourceTableDao;
    @Autowired
    private GetMeatDataService getMeatDataService;
    @Autowired
    private SourceColumnDao sourceColumnDao;

    /**
     * 获取txt,office文件元数据
     * @param fileFolderSquid
     * @param docExtractSquid
     * @param length
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> getDocExtractFileContent(FileFolderSquid fileFolderSquid,
                                                        DocExtractSquid docExtractSquid, int length) throws Exception {
        Map<String,Object> map=new HashMap<String,Object>();
        String fileName=null;
        String fileContent=null;
        try {
            int sourceTableId=docExtractSquid.getSource_table_id();
            //根据抽取的squid中source_table_id 获取DBSourceTable中文件名称。
            LOGGER.debug(String.format("getDbSourceBySourceTableId============================", sourceTableId));
            DBSourceTable sourceTable=sourceTableDao.getDbSourceBySourceTableId(sourceTableId);
            if(sourceTable!=null){
                fileName=sourceTable.getTable_name();
            }
            if(fileName!=null && !fileName.equals("")){
                //调用读取office 文件内容
                if (".doc|.docx|.xls|.xlsx|.pdf".contains(FileUtils.getFileEx(fileName))){
                    LOGGER.debug(String.format("getOfficeFileContent============================", fileFolderSquid,fileName,length));
                    fileContent=fileFolderAdapter.getOfficeFileContent(fileFolderSquid,fileName,length);
                }else{
                    LOGGER.debug(String.format("getTXTFileContent============================", fileFolderSquid,fileName,length));
                    fileContent=fileFolderAdapter.getTXTFileContent(fileFolderSquid,fileName,length);
                }
            }else{
                throw new ErrorMessageException(MessageCode.ERR_FILENAME.value());
            }
            //调用将文件转换成table格式的方法
            if(fileContent!=null && !fileContent.equals("")){
                LOGGER.debug(String.format("getDocTableList============================", fileContent,docExtractSquid));
              List<List<String>>  docList=DocExtractSquidUtil.getDocTableList(fileContent,docExtractSquid);
              if(docList!=null && docList.size()>0){
                  //转换成Source Column
                  LOGGER.debug(String.format("conversionColumn============================", docList,sourceTableId));
                  List<SourceColumn> sourceColumns=getMeatDataService.conversionColumn(docList,sourceTableId);
                  if(sourceColumns!=null && sourceColumns.size()>0){
                     //生成Trans，TransLink，Column与ReferenceColumn等对象
                      LOGGER.debug(String.format("addColumn============================", sourceColumns,fileFolderSquid.getId(),docExtractSquid.getId()));
                      map=getMeatDataService.addColumn(sourceColumns,fileFolderSquid.getId(),docExtractSquid.getId());
                  }
              }
            }else{
                //元数据为空，或者解析失败。
                LOGGER.debug(String.format("元数据为空，或者解析失败!"));
                throw new ErrorMessageException(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
            }
        }catch (ErrorMessageException e){
            e.printStackTrace();
            LOGGER.debug(String.format("获取文件内容失败!"),e);
            throw new ErrorMessageException(e.getErrorCode());
        } catch (Exception e){
            e.printStackTrace();
            LOGGER.debug(String.format("获取文件内容失败!"),e);
            throw e;
        }
        return map;
    }

    /**
     * 获取xml文件元数据
     * @param fileFolderSquid
     * @param xmlExtractSquid
     * @return
     * @throws Exception
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> getXmlExtractFileContent(FileFolderSquid fileFolderSquid,
                                                        XmlExtractSquid  xmlExtractSquid)throws Exception{
        Map<String,Object> map=new HashMap<String,Object>();
        String fileName="";
        try {
            //获取定义文件名称
             fileName=xmlExtractSquid.getXsd_dtd_file();
            if(fileName!=null && !fileName.equals("")){
                //读取xml文件内容，返回的就是一个SourceColumn的集合
                LOGGER.debug(String.format("getXMLFileContent============================", fileFolderSquid,xmlExtractSquid,fileName));
                List<SourceColumn> sourceColumns=fileFolderAdapter.getXMLFileContent(fileFolderSquid,xmlExtractSquid,fileName);
                if(sourceColumns!=null && sourceColumns.size()>0){
                    //生成Trans，TransLink，Column与ReferenceColumn等对象
                    LOGGER.debug(String.format("addColumn============================", sourceColumns,fileFolderSquid.getId(),xmlExtractSquid.getId()));
                    //重新获取要删除以前的
                    sourceColumnDao.deleteSourceColumnByTableId(xmlExtractSquid.getSource_table_id());
                    sourceColumnDao.insertSourceColumn(sourceColumns);
                    map=getMeatDataService.addColumn(sourceColumns,fileFolderSquid.getId(),xmlExtractSquid.getId());
                }else{
                    //元数据为空，或者解析失败。
                    LOGGER.debug(String.format("元数据为空，或者解析失败!"));
                    throw new ErrorMessageException(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
                }
            }else{
                //文件名错误
                LOGGER.debug(String.format("文件名错误"));
                throw new ErrorMessageException(MessageCode.ERR_FILENAME.value());
            }

        }catch (ErrorMessageException e){
            e.printStackTrace();
            LOGGER.debug(String.format("获取文件内容失败!"),e);
            throw new ErrorMessageException(e.getErrorCode());
        }catch (Exception e){
            e.printStackTrace();
            LOGGER.debug(String.format("获取文件内容失败!"),e);
            throw e;
        }
        return map;


    }
}
