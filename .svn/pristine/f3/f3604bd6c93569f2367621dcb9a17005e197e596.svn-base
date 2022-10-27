package com.eurlanda.datashire.server.utils;

import com.eurlanda.datashire.enc.LicenseDecode;
import com.eurlanda.datashire.enc.utils.EncryptUtil;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.utility.VersionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * Created by zhudebin on 2017/6/8.
 */
public class SysUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(SysUtil.class);

    public static int decodeLicenseKey(String key) throws Exception{
        LOGGER.info("key的值为:"+key);
        byte[] ss = LicenseDecode.decode(key.getBytes(Charset.forName("UTF-8")));
        String val = new String(ss);
        LOGGER.info("节点信息:"+val);
        if (val.contains("@@")){
            String version = VersionUtils.getServerVersion();
            String lv = val.split("@@")[1];

            String[] a1 = version.split("\\.");
            String[] a2 = lv.split("\\.");

            if (a1[0].equals(a2[0]) && a1[1].equals(a2[1])){
                int squidCnt = Integer.parseInt(val.split("@@")[0]);
                return squidCnt;
            }
        }
        return 5;
    }

    public static String encodeLicenseKey(String key) throws Exception {
        byte[] keys = EncryptUtil.encryptByPublicKey(key.getBytes(Charset.forName("UTF-8")));
        String str = new String(keys,"utf-8");
        System.out.println(str);
        return str;
    }
    /* 根据入参对象返回对象类型 */
    //	public static DSObjectType getType(final DSBaseModel bean){ // MethodArgumentCouldBeFinal
    public static DSObjectType getType(final Object bean){ // MethodArgumentCouldBeFinal
        if(bean instanceof Team){
            return DSObjectType.TEAM;
        }else if(bean instanceof User){
            return DSObjectType.USER;
        }
        else if(bean instanceof Role){
            return DSObjectType.ROLE;
        }
        else if(bean instanceof Group){
            return DSObjectType.GROUP;
        }
        // added by bo.dang start
        else if(bean instanceof TableExtractSquid){
            return DSObjectType.EXTRACT;
        }
        else if(bean instanceof DocExtractSquid){
            return DSObjectType.DOC_EXTRACT;
        }
        else if(bean instanceof XmlExtractSquid){
            return DSObjectType.XML_EXTRACT;
        }
        else if(bean instanceof WebLogExtractSquid){
            return DSObjectType.WEBLOGEXTRACT;
        }
        else if(bean instanceof WeiBoExtractSquid){
            return DSObjectType.WEIBOEXTRACT;
        }
        else if(bean instanceof WeiBoExtractSquid){
            return DSObjectType.WEIBOEXTRACT;
        }
        else if(bean instanceof WebExtractSquid){
            return DSObjectType.WEBEXTRACT;
        }
        else if(bean instanceof SquidLink){
            return DSObjectType.SQUIDLINK;
        }
        // added by bo.dang end
        else if(bean instanceof StageSquid){
            return DSObjectType.STAGE;
        }
        else if(bean instanceof Transformation){
            return DSObjectType.TRANSFORMATION;
        }
        // ReportSquid added by bo.dang
        else if(bean instanceof ReportSquid){
            return DSObjectType.REPORT;
        }
        return DSObjectType.UNKNOWN;
    }

    public static void main(String[] args){
        String key = "oZ2KwQ2ieWv9+4FjEt+G3BQE5HePtS+RJNL2jJNbNakkQ9+U7FF5Z2yJSWsBLPjnmLS+7fV/cav+omcGOChvHQ==";
        try {
            encodeLicenseKey(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
