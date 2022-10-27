package com.eurlanda.datashire.server.utils.fileSource;

import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.XmlExtractSquid;

import java.util.List;

/**
 * 文档获取的接口，新的数据源获取类都需要继承该接口去做实现
 * @param <E> 文件类型Connection Squid
 */
public interface FileSourceAdapter<E> {

    /**
     * 通过文件连接Squid，获取指定文件中的内容
     * @param fileConncetionSquid 源Squid
     * @param fileName 文件名称
     * @param length 字节长度
     * @return
     */
    String getTXTFileContent(E fileConncetionSquid, String fileName, int length) throws Exception;

    /**
     * 获取Office类型的文件内容
     * @param fileConnectionSquid
     * @param fileName
     * @return
     */
    String getOfficeFileContent(E fileConnectionSquid,String fileName, int length) throws Exception;

    /**
     * 获取XML
     * @param fileConnectionSquid
     * @param fileName
     * @return
     */
    List<SourceColumn> getXMLFileContent(E fileConnectionSquid, XmlExtractSquid extractSquid, String fileName) throws Exception;
}