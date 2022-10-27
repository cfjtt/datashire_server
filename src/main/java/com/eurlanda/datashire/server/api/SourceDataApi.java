package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.service.FTPService;
import com.eurlanda.datashire.server.service.FileFolderService;
import com.eurlanda.datashire.server.service.HDFSService;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.SysConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Eurlanda - Java、 on 2017/8/3.
 */
@Service
@SocketApi("2015")
public class SourceDataApi {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceDataApi.class);
    @Autowired
    private  FileFolderService fileFolderService;
    @Autowired
    private FTPService ftpService;
    @Autowired
    private HDFSService hdfsService;

    @SocketApiMethod(commandId = "0001")
    public String getSourceDataByExtractSquid(String info){
        ReturnValue out=new ReturnValue();
        Map<String, Object> resultMap = new HashMap<String, Object>();
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        try {
            Squid squid=JsonUtil.toGsonBean(infoMap.get("ConnectionSquid").toString(),Squid.class);
            ExtractSquid extractSquid=JsonUtil.toGsonBean(infoMap.get("ExtractSquid").toString(),ExtractSquid.class);
            //连接Squid是fileFolderSquid
            if(squid.getSquid_type()== SquidTypeEnum.FILEFOLDER.value()){
                FileFolderSquid fileFolderSquid=JsonUtil.toGsonBean(infoMap.get("ConnectionSquid").toString(),FileFolderSquid.class);
                //抽取squid是DocExtractSquid
                if(extractSquid.getSquid_type()==SquidTypeEnum.DOC_EXTRACT.value()){
                    DocExtractSquid  docExtractSquid = JsonUtil.toGsonBean(infoMap.get("ExtractSquid").toString(),DocExtractSquid.class);
                    resultMap=fileFolderService.getDocExtractFileContent(fileFolderSquid,docExtractSquid,1024*1024*2);
                    //抽取squid是XmlExtractSquid
                }else if(extractSquid.getSquid_type()==SquidTypeEnum.XML_EXTRACT.value()){
                    XmlExtractSquid xmlExtractSquid = JsonUtil.toGsonBean(infoMap.get("ExtractSquid").toString(),XmlExtractSquid.class);
                    resultMap=fileFolderService.getXmlExtractFileContent(fileFolderSquid,xmlExtractSquid);
                }
                //连接Squid是FtpSquid
            }else if(squid.getSquid_type()== SquidTypeEnum.FTP.value()){
                FtpSquid ftpSquid=JsonUtil.toGsonBean(infoMap.get("ConnectionSquid").toString(),FtpSquid.class);
                //抽取squid是DocExtractSquid
                if(extractSquid.getSquid_type()==SquidTypeEnum.DOC_EXTRACT.value()){
                    DocExtractSquid  docExtractSquid = JsonUtil.toGsonBean(infoMap.get("ExtractSquid").toString(),DocExtractSquid.class);
                    resultMap=ftpService.getDocExtractFileContent(ftpSquid,docExtractSquid,1024*1024*2);
                    //抽取squid是XmlExtractSquid
                }else if(extractSquid.getSquid_type()==SquidTypeEnum.XML_EXTRACT.value()){
                    XmlExtractSquid xmlExtractSquid = JsonUtil.toGsonBean(infoMap.get("ExtractSquid").toString(),XmlExtractSquid.class);
                    resultMap=ftpService.getXmlExtractFileContent(ftpSquid,xmlExtractSquid);
                }
                //连接Squid是HdfsSquid
            }else if(squid.getSquid_type()== SquidTypeEnum.HDFS.value()
                    || squid.getSquid_type() == SquidTypeEnum.SOURCECLOUDFILE.value()
                    || squid.getSquid_type() == SquidTypeEnum.TRAINNINGFILESQUID.value()){
                HdfsSquid hdfsSquid = JsonUtil.toGsonBean(infoMap.get("ConnectionSquid").toString(), HdfsSquid.class);
                if (SysConf.getValue("hdfs_host").equals(hdfsSquid.getHost())) {
                    hdfsSquid.setHost(SysConf.getValue("hdfsIpAndPort"));
                }
                if(squid.getSquid_type()==SquidTypeEnum.TRAINNINGFILESQUID.value()
                        && hdfsSquid.getHost().equals(SysConf.getValue("train_file_host"))){
                    hdfsSquid.setHost(SysConf.getValue("train_file_real_host"));

                }
                //抽取squid是DocExtractSquid
                if (extractSquid.getSquid_type() == SquidTypeEnum.DOC_EXTRACT.value()) {
                    DocExtractSquid docExtractSquid = JsonUtil.toGsonBean(infoMap.get("ExtractSquid").toString(), DocExtractSquid.class);
                    resultMap = hdfsService.getTXTOrOfficeFileContent(hdfsSquid, docExtractSquid, 1024 * 1024 * 2);
                    //抽取squid是XmlExtractSquid
                } else if (extractSquid.getSquid_type() == SquidTypeEnum.XML_EXTRACT.value()) {
                    XmlExtractSquid xmlExtractSquid = JsonUtil.toGsonBean(infoMap.get("ExtractSquid").toString(), XmlExtractSquid.class);
                    resultMap = hdfsService.getXMLFileContent(hdfsSquid, xmlExtractSquid);
                }
            }
        }catch (ErrorMessageException e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.parse(e.getErrorCode()));
        }catch (Exception e){
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_FILECONTENT);
            LOGGER.error("删除Transformation异常", e);
        }

        return JsonUtil.toJsonString(resultMap,null,out.getMessageCode());
    }

}
