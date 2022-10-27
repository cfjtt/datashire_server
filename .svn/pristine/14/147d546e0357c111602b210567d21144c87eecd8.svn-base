package com.eurlanda.datashire.server.service;

import cn.com.jsoft.jframe.utils.FileUtils;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.server.dao.SourceColumnDao;
import com.eurlanda.datashire.server.dao.SourceTableDao;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.utils.fileSource.HDFSAdapterImp;
import com.eurlanda.datashire.server.utils.utility.DocExtractSquidUtil;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/8/3.
 */
@Service
public class HDFSService {
    private static final Logger logger = LoggerFactory.getLogger(HDFSService.class);

    HDFSAdapterImp hdfsAdapterImp = new HDFSAdapterImp();

    @Autowired
    private GetMeatDataService getMeatDataService;
    @Autowired
    private SourceTableDao sourceTableDao;
    @Autowired
    private SourceColumnDao sourceColumnDao;

    /**
     * 获取HDFS文本类型或者Office类型的文件
     *
     * @param
     * @return
     * @throws Exception
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> getTXTOrOfficeFileContent(HdfsSquid hdfsSquid, DocExtractSquid docExtractSquid, int length) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        String result = null;
        try {
            if (hdfsSquid == null || docExtractSquid == null || length == 0) {
                throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());
            } else {
                DBSourceTable dbSourceTable = sourceTableDao.getDbSourceBySourceTableId(docExtractSquid.getSource_table_id());
                String fileName = null;
                if (dbSourceTable != null) {
                    fileName = dbSourceTable.getTable_name();
                }
                if (fileName != null && !fileName.equals("")) {
                    if (StringUtils.isNotEmpty(FileUtils.getFileEx(fileName)) && ".doc|.docx|.pdf|.xlsx|.xls".contains(FileUtils.getFileEx(fileName))) {
                        result = hdfsAdapterImp.getOfficeFileContent(hdfsSquid, fileName, length);
                    } else {
                        //获取数据
                        result = hdfsAdapterImp.getTXTFileContent(hdfsSquid, fileName, length);
                    }
                } else {
                    logger.debug(String.format("文件名称错误!"));
                    throw new ErrorMessageException(MessageCode.ERR_FILENAME.value());
                }
                //获取数据
                if (result!=null && !result.equals("")) {
                    //转换成table结果
                    List<List<String>> lists = DocExtractSquidUtil.getDocTableList(result, docExtractSquid);
                    //转换成SourceColumn形式
                    List<SourceColumn> sourceColumnList = getMeatDataService.conversionColumn(lists, docExtractSquid.getSource_table_id());
                    //添加Column
                    resultMap = getMeatDataService.addColumn(sourceColumnList, hdfsSquid.getId(), docExtractSquid.getId());

                }else {
                    //元数据为空，或者解析失败。
                    logger.debug(String.format("元数据为空，或者解析失败!"));
                    throw new ErrorMessageException(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
                }
            }
        } catch (ErrorMessageException e){
            e.printStackTrace();
            logger.debug(String.format("获取文件内容失败!"),e);
            throw new ErrorMessageException(e.getErrorCode());
        }catch (Exception e) {
            logger.error("--获取元数据失败--", e);
            e.printStackTrace();
            throw e;
        }
        return resultMap;
    }

    /**
     * 获取Office
     *
     * @param
     * @return
     * @throws Exception
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> getXMLFileContent(HdfsSquid hdfsSquid, XmlExtractSquid xmlExtractSquid) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        try {
            if (hdfsSquid == null || xmlExtractSquid == null) {
                throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());
            } else {
                String fileName = xmlExtractSquid.getXsd_dtd_file();
                if (fileName != null && !fileName.equals("")) {
                    //获取数据
                    List<SourceColumn> sourceColumnLsit = hdfsAdapterImp.getXMLFileContent(hdfsSquid, xmlExtractSquid, fileName);
                    //获取数据
                    if (sourceColumnLsit == null || sourceColumnLsit.size() == 0) {
//                        logger.debug("--获取数据失败--");
                        //元数据为空，或者解析失败。
                        logger.debug(String.format("元数据为空，或者解析失败!"));
                        throw new ErrorMessageException(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
                    }
                    //重新获取要删除以前的
                    sourceColumnDao.deleteSourceColumnByTableId(xmlExtractSquid.getSource_table_id());
                    sourceColumnDao.insertSourceColumn(sourceColumnLsit);
                    //添加Column
                    resultMap = getMeatDataService.addColumn(sourceColumnLsit, hdfsSquid.getId(), xmlExtractSquid.getId());
                } else {
                    //文件名错误
                    logger.debug(String.format("文件名错误"));
                    throw new ErrorMessageException(MessageCode.ERR_FILENAME.value());
                }
            }
        } catch (Exception e) {
            logger.error("--获取元数据失败--", e);
            e.printStackTrace();
            throw e;
        }
        return resultMap;
    }
}
