package com.eurlanda.datashire.sprint7.service.squidflow;

import cn.com.jsoft.jframe.utils.FileUtils;
import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.alibaba.fastjson.JSONObject;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.RemoteFileAdapter;
import com.eurlanda.datashire.adapter.db.HbaseAdapter;
import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.dao.*;
import com.eurlanda.datashire.dao.impl.*;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.entity.dest.DestImpalaColumn;
import com.eurlanda.datashire.entity.dest.DestWSSquid;
import com.eurlanda.datashire.entity.dest.EsColumn;
import com.eurlanda.datashire.entity.operation.*;
import com.eurlanda.datashire.enumeration.*;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.EsColumnService;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.HdfsColumnService;
import com.eurlanda.datashire.sprint7.service.squidflow.dest.ImpalaColumnService;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;
import com.eurlanda.datashire.utility.*;
import com.eurlanda.datashire.utility.objectsql.SelectSQL;
import com.eurlanda.datashire.validator.SquidValidationTask;
import com.jcraft.jsch.ChannelSftp;
import com.mongodb.*;
import jcifs.smb.SmbAuthException;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.poifs.filesystem.NotOLE2FileException;
import org.apache.xmlbeans.XmlException;
import util.ZipStrUtil;

import java.io.*;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

/**
 * SquidProcess预处理类
 * Title :
 * Description:
 * Author :赵春花 2013-8-28
 * update :赵春花2013-8-28
 * Department : JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class SquidProcess implements ISquidProcess {
    static Logger logger = Logger.getLogger(SquidProcess.class);// 记录日志
    private SquidService squidService;
    private String token;
    private String key;

    public SquidProcess() {
    }

    public SquidProcess(String token) {
        this.token = token;
        squidService = new SquidService(token);
    }

    public SquidProcess(String token, String key) {
        this.token = token;
        this.key = key;
        squidService = new SquidService(token);
    }

    /**
     * 创建StageSquid
     * 作用描述：
     * 序列化TransformationAndCloumn集合对象并验证TransformationAndCloumn是否为空，为空的情况下返回null；
     * 修改说明：
     *
     * @param info
     * @param out
     * @return
     */
    public List<InfoPacket> createMoreStageSquid(String info, ReturnValue out) {
        logger.debug(String.format("createMoreStageSquid-info=%s", info));
        //获得TransformationAndCloumn对象
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        try {
            List<TransformationAndCloumn> transformationAndCloumns = JsonUtil.toGsonList(info, TransformationAndCloumn.class);
            infoPackets = squidService.createMoreStageSquid(transformationAndCloumns, out);
            logger.debug(String.format("createMoreStageSquid-return=%s", infoPackets));
            return infoPackets;
        } catch (Exception e) {
            out.setMessageCode(MessageCode.ERR_DATA_CONVERSION);
            logger.error("TransformationAndCloumns_GSON_ERR", e);
        }
        return infoPackets;
    }

    /**
     * 创建DBSourceSquid集合对象处理类
     * 作用描述：
     * 序列化DBSourceSquid对象集合
     * 创建DBSourceSquid对象集合
     * 成功根据DBSourceSquid对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
     * 修改说明：
     *
     * @param info
     * @param out
     * @return
     */
    public List<InfoPacket> createDBSourceSquids(String info, ReturnValue out) {
        logger.debug(String.format("createDBSourceSquids-info=%s", info));
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        //序列化JSONArray
        List<DBSourceSquid> dbSourceSquids = JsonUtil.toGsonList(info, DBSourceSquid.class);
        //验证DBSourceSquids集合对象
        if (dbSourceSquids == null || dbSourceSquids.size() == 0) {
            //DBSource对象集合不能为空size不能等于0
            out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            logger.debug(String.format("createDBSourceSquids-return-=%s", false));
            return infoPackets;
        }
        infoPackets = squidService.createDBSourceSquids(dbSourceSquids, out);
        logger.debug(String.format("createDBSourceSquids-return=%s", infoPackets));
        return infoPackets;
    }

    /**
     * 修改Squid对象集合
     * 作用描述：
     * 根据Squid对象集合里Squid的ID进行修改
     * 修改说明：
     *
     * @param info json字符串
     * @param out  异常处理
     * @return
     */
    public void updateDbSourceSquid(String info, ReturnValue out) {
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        // Json序列化
        try {
            HashMap<String, Object> data = JsonUtil.toHashMap(info);
            DbSquid dbSourceSquid = JsonUtil.toGsonBean(data.get("DbSourceSquid").toString(), DbSquid.class);
            //只截取备注前200个字符，其余舍弃
            String o = dbSourceSquid.getDescription();
            if (o != null && o.length() > 200) {
                String k = o.substring(0, 200);
                dbSourceSquid.setDescription(k);
            }
            if (null != dbSourceSquid) {
                adapter.openSession();
                /*if(dbSourceSquid.getSquid_type()== SquidTypeEnum.CLOUDDB.value()
                        && (dbSourceSquid.getHost().equals(SysConf.getValue("cloud_host")+":3306"))){
                    dbSourceSquid.setHost(SysConf.getValue("cloud_db_ip_port"));
                }*/
                ISquidDao squidDao = new SquidDaoImpl(adapter);
                boolean flag = squidDao.update(dbSourceSquid);
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(dbSourceSquid.getId(), dbSourceSquid.getId(), dbSourceSquid.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_INCREMENT.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(dbSourceSquid.getId(), dbSourceSquid.getId(), dbSourceSquid.getName(), MessageBubbleCode.WARN_DATASQUID_NO_SQUIDINDEXES.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(dbSourceSquid.getId(), dbSourceSquid.getId(), dbSourceSquid.getName(), MessageBubbleCode.ERROR_SQL_SYNTAX_FILTER.value())));
                if (!flag) {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            } else {
                out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            }
        } catch (Exception e) {
            // TODO: handle exception
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新DbSourceSquid异常", e);
        } finally {
            adapter.closeSession();
        }
    }

    /**
     * 修改Squid
     * 作用描述：
     * 根据Squid对象集合里Squid的ID进行修改
     * 修改说明：
     *
     * @param info json字符串
     * @param out  异常处理
     * @return
     */
    public boolean updateSquid(String info, ReturnValue out) {
        //Json序列化
        List<Squid> squids = JsonUtil.toGsonList(info, Squid.class);
        if (null == squids || squids.size() == 0) {
            out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            return false;
        }
        return squidService.updateSquids(squids, out);
    }

    /**
     * 修改ExtractSquid对象集合
     * 作用描述：
     * 根据ExtractSquid对象集合里ExtractSquid的ID进行修改
     * 修改说明：
     *
     * @param info
     * @param out
     * @return
     */
    public void updateExtractSquids(String info, ReturnValue out) {
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        try {
            HashMap<String, Object> data = JsonUtil.toHashMap(info);
            TableExtractSquid extractSquid = JsonUtil.toGsonBean(data.get("TableExtractSquid").toString(), TableExtractSquid.class);
            if (null != extractSquid) {
                adapter.openSession();
                ISquidDao squidDao = new SquidDaoImpl(adapter);
                boolean flag = squidDao.update(extractSquid);
                if (!flag) {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            } else {
                out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            }
        } catch (Exception e) {
            // TODO: handle exception
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新extrasquid异常", e);
        } finally {
            adapter.closeSession();
        }
    }

    /**
     * 修改stageSquid对象集合
     * 作用描述：
     * 根据Squid对象集合里stageSquid的ID进行修改
     * 修改说明：
     *
     * @param info json字符串
     * @param out  异常处理
     * @return
     */
    public void updateStageSquids(String info, ReturnValue out) {
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        ISquidDao squidDao = new SquidDaoImpl(adapter);
        List<Integer> list = new ArrayList<>();
        List<Object> paramsList = new ArrayList<>();
        try {
            HashMap<String, Object> data = JsonUtil.toHashMap(info);
            StageSquid stageSquid = JsonUtil.toGsonBean(data.get("StageSquid").toString(), StageSquid.class);
            if (null != stageSquid) {
                adapter.openSession();
                StageSquid stage = squidDao.getSquidForCond(stageSquid.getId(), StageSquid.class);
                boolean flag = squidDao.update(stageSquid);
                String sql = "SELECT dcm.id FROM ds_column dcm,ds_transformation dtf,ds_squid ds WHERE dtf.id IN(SELECT dtl.TO_TRANSFORMATION_ID FROM ds_transformation dt,ds_transformation_link dtl WHERE dt.id = dtl.FROM_TRANSFORMATION_ID AND dt.column_id IN (SELECT ds.id FROM ds_squid dsq,ds_column ds WHERE dsq.id = ds.squid_id AND dsq.id = " + stageSquid.getId() + "))AND dcm.id = dtf.COLUMN_ID and dcm.squid_id = ds.id and dtf.squid_id = ds.id and ds.squid_type_id = 20";
                List<Map<String, Object>> maps = adapter.query2List(true, sql, null);
                if (maps.size() > 0 && maps != null) {
                    if (StringUtils.isHavaChinese(stageSquid.getName())) {
                        SquidTypeEnum types = SquidTypeEnum.valueOf(squidDao.getSquidTypeById(stageSquid.getId()));
                        stageSquid.setName(types.toString() + stageSquid.getId());
                    }
                    if (StringUtils.isHavaChinese(stage.getName())) {
                        SquidTypeEnum types = SquidTypeEnum.valueOf(squidDao.getSquidTypeById(stage.getId()));
                        stage.setName(types.toString() + stage.getId());
                    }
                    for (Map<String, Object> map2 : maps) {
                        list.add(Integer.parseInt(map2.get("ID").toString()));
                    }
                    String params = JsonUtil.toJSONString(list).replace("[", "(").replace("]", ")");
                    sql = "UPDATE ds_column SET name = REPLACE(name,?,?) WHERE id IN " + params;
                    paramsList.add(stage.getName());
                    paramsList.add(stageSquid.getName());
                    adapter.execute(sql, paramsList);
                }
                if (!flag) {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            } else {
                out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            }
        } catch (Exception e) {
            // TODO: handle exception
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新stagesquid异常", e);
        } finally {
            adapter.closeSession();
        }
    }

    /**
     * 修改DestinationSquid对象集合
     * 作用描述：
     * 根据DestinationSquid对象集合里DestinationSquid的ID进行修改
     * 修改说明：
     *
     * @param info json字符串
     * @param out  异常处理
     * @return
     */
    public List<InfoPacket> updateDestinationSquids(String info, ReturnValue out) {
        logger.debug(String.format("updateDestinationSquids-info=%s", info));
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        //Json序列化
        List<DbSquid> dbDestinationSquids = JsonUtil.toGsonList(info, DbSquid.class);
        if (null == dbDestinationSquids || dbDestinationSquids.size() == 0) {
            out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            return infoPackets;
        }
        infoPackets = squidService.updateDestinationSquids(dbDestinationSquids, out);
        logger.debug(String.format("updateDestinationSquids-code=%s", out.getMessageCode()));
        return infoPackets;

    }

    /**
     * 创建ExtractSquids集合对象处理类
     * 作用描述：
     * 创建ExtractSquids对象集合
     * 成功根据ExtractSquids对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
     * 修改说明：
     *
     * @param info json字符串
     * @param out  异常处理
     * @return
     */
    public List<InfoPacket> createExtractSquids(String info, ReturnValue out) {
        logger.debug(String.format("createExtractSquids-info=%s", info));
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        //序列化JSONArray
        List<TableExtractSquid> extractSquids = JsonUtil.toGsonList(info, TableExtractSquid.class);
        //验证ExtractSquid集合对象
        if (extractSquids == null || extractSquids.size() == 0) {
            //ExtractSquid对象集合不能为空size不能等于0
            out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            logger.debug(String.format("createExtractSquids-columns-=%s", false));
            return infoPackets;
        }
        infoPackets = squidService.createExtractSquids(extractSquids, out);
        logger.debug(String.format("createExtractSquids-return=%s", infoPackets));
        return infoPackets;
    }

    /**
     * 创建DestinationSquid集合对象处理类
     * <p>
     * 作用描述：
     * 创建DestinationSquid对象集合
     * 成功根据DestinationSquid对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
     * <p>
     * <p>
     * 修改说明：
     *
     * @param info json字符串
     * @param out  异常处理
     * @return
     */
    public List<InfoPacket> createDestinationSquids(String info, ReturnValue out) {
        logger.debug(String.format("createDestinationSquids-info=%s", info));
        List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
        //序列化JSONArray
        List<DBDestinationSquid> dbDestinationSquids = JsonUtil.toGsonList(info, DBDestinationSquid.class);
        //验证DestinationSquid集合对象
        if (dbDestinationSquids == null || dbDestinationSquids.size() == 0) {
            //DestinationSquid对象集合不能为空size不能等于0
            out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            logger.debug(String.format("createDestinationSquids-result-=%s", false));
            return infoPackets;
        }
        infoPackets = squidService.createDestinationSquids(dbDestinationSquids, out);
        logger.debug(String.format("createDestinationSquids-return=%s", infoPackets));
        return infoPackets;
    }


    /**
     * 创建StageSquid
     * 作用描述：
     * 序列化StageSquidAndSquidLink对象
     * ExtractSquidAndSquidLink对象里包含ExtractSquid对象和SquidLink对象
     * 验证：
     * ExtractSquidAndSquidLink对象集合不为空
     * 修改说明：
     *
     * @param info json字符串
     * @param out  异常处理
     */
    public List<ExtractSquidAndSquidLink> createMoreExtractsquid(String info, ReturnValue out) {
        List<ExtractSquidAndSquidLink> list = JsonUtil.toGsonList(info, ExtractSquidAndSquidLink.class);
        squidService.createMoreExtractSquid(list, TokenUtil.getToken(), out);
        return list;
    }

    /**
     * 创建fileSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> createFileFloderSquid(String info,
                                                     ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int fileSquidId = 0;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            FileFolderSquid fileFolderSquid = JsonUtil
                    .object2HashMap(info, FileFolderSquid.class);
            if (null != fileFolderSquid) {
                fileSquidId = adapter.insert2(fileFolderSquid);
                if (fileSquidId > 0)// 插入成功
                {
                    outputMap.put("newSquidId", fileSquidId);
                } else {
                    out.setMessageCode(MessageCode.INSERT_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(fileSquidId, fileSquidId, fileFolderSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
            }
        } catch (BeyondSquidException e) {
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("createFileFloderSquid is error", e);
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createFileFloderSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 创建FtpSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> createFtpSquid(String info,
                                              ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int ftpSquidId = 0;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            FtpSquid ftpConnection = JsonUtil
                    .object2HashMap(info, FtpSquid.class);
            if (null != ftpConnection) {
                ftpSquidId = adapter.insert2(ftpConnection);
                if (ftpSquidId > 0)// 插入成功
                {
                    outputMap.put("newSquidId", ftpSquidId);
                } else {
                    out.setMessageCode(MessageCode.INSERT_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(ftpSquidId, ftpSquidId, ftpConnection.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
            }
        } catch (BeyondSquidException e) {
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("createFtpSquid is error", e);
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createFtpSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 创建HdfsSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> createHdfsSquid(String info,
                                               ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int hdfsSquidId = 0;
        try {
            // 实例化相关的数据库处理类
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            HdfsSquid hdfsConnection = JsonUtil
                    .object2HashMap(info, HdfsSquid.class);
            if (null != hdfsConnection) {

                hdfsSquidId = adapter.insert2(hdfsConnection);
                if (hdfsSquidId > 0)// 插入成功
                {
                    outputMap.put("newSquidId", hdfsSquidId);
                } else {
                    out.setMessageCode(MessageCode.INSERT_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(hdfsSquidId, hdfsSquidId, hdfsConnection.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
            }
        } catch (BeyondSquidException e) {
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("createHdfsSquid is error", e);
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createHdfsSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 创建WeiboSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> createWeiboSquid(String info,
                                                ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int weiBoSquidId = 0;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            WeiboSquid weiboSquid = JsonUtil
                    .object2HashMap(info, WeiboSquid.class);
            if (null != weiboSquid) {
                weiBoSquidId = adapter.insert2(weiboSquid);
                if (weiBoSquidId > 0)// 插入成功
                {
                    outputMap.put("newSquidId", weiBoSquidId);
                } else {
                    out.setMessageCode(MessageCode.INSERT_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(weiBoSquidId, weiBoSquidId, weiboSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
            }
        } catch (BeyondSquidException e) {
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("createWeiBoSquid is error", e);
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createWeiBoSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 创建Url列表
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> createWebUrls(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        List<Url> urls = null;
        int urlId = 0;
        int squid_id = 0;
        List<Integer> integers = new ArrayList<Integer>();
        try {
            Map<String, Object> map = JsonUtil.toHashMap(info);
            urls = JsonUtil
                    .toGsonList(map.get("WebUrls").toString(), Url.class);
            squid_id = Integer.parseInt(map.get("WebSquidId").toString());
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            for (Url url : urls) {
                url.setSquid_id(squid_id);
                urlId = adapter.insert2(url);
                if (urlId < 0) {
                    out.setMessageCode(MessageCode.ERR_CREATEWEBURLS);
                    break;
                } else {
                    integers.add(urlId);
                }
            }
            CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(squid_id, squid_id, null, MessageBubbleCode.WARN_WEB_CONNECTION_NO_URL_LIST.value())));
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createUrls is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.error("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        outputMap.put("Ids", integers);
        return outputMap;
    }

    /**
     * 创建WebSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> createWebSquid(String info,
                                              ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int WebSquidId = 0;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            WebSquid webSquid = JsonUtil
                    .object2HashMap(info, WebSquid.class);
            if (null != webSquid) {
                WebSquidId = adapter.insert2(webSquid);
                if (WebSquidId > 0)// 插入成功
                {
                    outputMap.put("newSquidId", WebSquidId);
                } else {
                    out.setMessageCode(MessageCode.INSERT_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(WebSquidId, WebSquidId, webSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
            }
        } catch (BeyondSquidException e) {
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("createWebSquid is error", e);
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createWebSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 更新FileSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> updateFileSquid(String info,
                                               ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            FileFolderSquid fileFolderConnection = JsonUtil
                    .object2HashMap(info, FileFolderSquid.class);
            if (null != fileFolderConnection) {
                updateFlag = adapter.update2(fileFolderConnection);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateFileSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 更新FtpSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> updateFtpSquid(String info,
                                              ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            FtpSquid ftpConnection = JsonUtil
                    .object2HashMap(info, FtpSquid.class);
            if (null != ftpConnection) {
                updateFlag = adapter.update2(ftpConnection);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateFtpSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 更新HdfsSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> updateHdfsSquid(String info,
                                               ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            HdfsSquid hdfsConnection = JsonUtil
                    .object2HashMap(info, HdfsSquid.class);
            if (null != hdfsConnection) {

                //判断路径是否正确(不允许出现../)
                /*if(hdfsConnection.getFile_path().indexOf("/../")>0){
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                } else {*/
                    updateFlag = adapter.update2(hdfsConnection);
                    if (!updateFlag)// 更新失败
                    {
                        out.setMessageCode(MessageCode.UPDATE_ERROR);
                    }
                //}
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateHdfsSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 更新WeiboSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> updateWeiboSquid(String info,
                                                ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
        boolean updateFlag = false;
        try {
            WeiboSquid weiboSquid = JsonUtil
                    .object2HashMap(info, WeiboSquid.class);
            if (null != weiboSquid) {
                updateFlag = connectProcess.updateWeibo(weiboSquid, out);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateWeiboSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
        }
        return outputMap;
    }

    /**
     * 更新WebSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> updateWebSquid(String info,
                                              ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
        boolean updateFlag = false;
        try {
            WebSquid weiboSquid = JsonUtil
                    .object2HashMap(info, WebSquid.class);
            if (null != weiboSquid) {
                updateFlag = connectProcess.updateWeb(weiboSquid, out);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateWebSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
        }
        return outputMap;
    }

    /**
     * 更新WebSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public void updateWebUrls(String info, ReturnValue out) {
        boolean updateFlag = false;
        int squid_id = 0;
        List<Url> urls = null;
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            Map<String, Object> map = JsonUtil.toHashMap(info);
            urls = JsonUtil
                    .toGsonList(map.get("WebUrls").toString(), Url.class);
            squid_id = Integer.parseInt(map.get("WebSquidId").toString());
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            for (Url url : urls) {
                url.setSquid_id(squid_id);
                updateFlag = adapter.update2(url);
                if (!updateFlag) {
                    out.setMessageCode(MessageCode.ERR_UPDATEWEBURLS);
                    break;
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateWebUrls is error", e);
            out.setMessageCode(MessageCode.ERR_UPDATEWEBURLS);
        } finally {
            adapter.closeSession();
        }
    }

    /**
     * 根据连接信息获取filefolder的列表信息
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> connectFileFloderSquid(String info,
                                                      ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
        int executeResult = 0;
        boolean updateFlag = false;
        SmbFile smbFile = null;
        List<Integer> cancelSquidIds=new ArrayList<>();
        int updateSquidStatus=0;
        IRelationalDataManager adapter = null;
        List<Object> params = new ArrayList<>(); // 查询参数
        try {
            FileFolderSquid fileFolderConnection = JsonUtil
                    .object2HashMap(info, FileFolderSquid.class);
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            String sql=" select k.TO_SQUID_ID as TOSQUID from ds_squid_link k where k.FROM_SQUID_ID =?";
            params.clear();
            params.add(fileFolderConnection.getId());
            //查询下游所有的抽取squid
            List<Map<String, Object>> squidIds=adapter.query2List(true,sql,params);
            if(squidIds!=null && squidIds.size()>0){
                for(Map<String,Object> map:squidIds){
                    cancelSquidIds.add(Integer.parseInt(map.get("TOSQUID").toString()));
                }
            }
            //修改这squid状态。
            if(cancelSquidIds!=null && cancelSquidIds.size()>0){
                List<Object> updateSquidList=new ArrayList<>();
                String updateSql="update ds_squid  set DESIGN_STATUS=1 where id in ";
                updateSquidList.addAll(cancelSquidIds);
                String ids = JsonUtil.toGsonString(updateSquidList);
                ids=ids.substring(1,ids.length()-1);
                updateSql+="("+ids+")";
                adapter.execute(updateSql);
            }
                outputMap.put("ErrorSquidIdList", cancelSquidIds);

            if (!(fileFolderConnection.getFile_path().startsWith("/"))) {
                fileFolderConnection.setFile_path("/" + fileFolderConnection.getFile_path());
            }
            if (null != fileFolderConnection) {
                fileFolderConnection.setFile_path(fileFolderConnection.getFile_path().replaceAll("\\\\", "/"));
                FileFolderUtils fileFolderUtils = new FileFolderUtils();
                List<FileInfo> fileInfos = null;
                //分共享文件和本地文件(对于服务器的共享文件，都可以当本地文件来读)
                if (StringUtils.isBlank(fileFolderConnection.getHost())) {
                    fileInfos = fileFolderUtils.browseFile(fileFolderConnection);
                } else {
                    if (fileFolderConnection.getHost().equals(OSUtils.getLocalIP()))//按照本地文件
                    {
                        fileInfos = fileFolderUtils.browseFile(fileFolderConnection);
                    } else {//共享文件
                        fileInfos = new RemoteFileAdapter().getFiles(smbFile, fileFolderConnection);
                    }
                }
                //outputMap.put("SourceTables", sourceTables);
                //存储DS_SOURCE_TABLE
                int sourceId = fileFolderConnection.getId();
                List<SourceTable> sourceTables = new FtpUtils().converFile2Source(fileInfos);
                executeResult = connectProcess.connectionDBSourceTable(sourceTables, sourceId);
                if (executeResult < 0) {
                    out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                } else {
                    //只截取备注前200个字符，其余舍弃
                    String o = fileFolderConnection.getDescription();
                    if (o != null && o.length() > 200) {
                        String k = o.substring(0, 200);
                        fileFolderConnection.setDescription(k);
                    }
                    //存储DS_FILEFOLDER_CONNECTION
                    updateFlag = connectProcess.updateFileFolder(fileFolderConnection, out);
                    if (!updateFlag) {
                        out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                    } else {
                        List<DBSourceTable> dbSourceTables = connectProcess.getDBSourceTable(sourceId, out);
                        this.setSourceTable(sourceTables, dbSourceTables);
                        outputMap.put("SourceTables", sourceTables);
                    }

                }

            }
        } catch (SmbAuthException e) {
            SmbException smb = new SmbException(1, false);
            logger.debug(e.getNtStatus() == smb.NT_STATUS_ACCESS_DENIED);
            for (int i = 0; i < smb.NT_STATUS_CODES.length; i++) {
                logger.debug(smb.NT_STATUS_CODES[i]);
            }
            out.setMessageCode(MessageCode.ERR_USERNAME_OR_PASSWORD);
        } catch (Exception e) {
            smbFile = null;
            e.printStackTrace();
            ///logger.error("connectFileFloderSquid is error", e);
            out.setMessageCode(MessageCode.ERR_FILE);
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 根据连接信息获取ftp的列表信息
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> connectFtpSquid(String info,
                                               ReturnValue out) {
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
        Map<String, Object> outputMap = new HashMap<String, Object>();
        int executeResult = 0;
        boolean updateFlag = false;
        List<FileInfo> fileInfos = new ArrayList<>();
        List<Integer> cancelSquidIds=new ArrayList<>();
        int updateSquidStatus=0;
        IRelationalDataManager adapter = null;
        List<Object> params = new ArrayList<>(); // 查询参数
        try {
            FtpSquid ftpConnection = JsonUtil
                    .object2HashMap(info, FtpSquid.class);
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            String sql=" select k.TO_SQUID_ID as TOSQUID from ds_squid_link k where k.FROM_SQUID_ID =?";
            params.clear();
            params.add(ftpConnection.getId());
            //查询下游所有的抽取squid
            List<Map<String, Object>> squidIds=adapter.query2List(true,sql,params);
            if(squidIds!=null && squidIds.size()>0){
                for(Map<String,Object> map:squidIds){
                    cancelSquidIds.add(Integer.parseInt(map.get("TOSQUID").toString()));
                }
            }
            //修改这squid状态。
            if(cancelSquidIds!=null && cancelSquidIds.size()>0){
                List<Object> updateSquidIds=new ArrayList<>();
                String updateSql="update ds_squid  set DESIGN_STATUS=1 where id in ";
                updateSquidIds.addAll(cancelSquidIds);
                String ids = JsonUtil.toGsonString(updateSquidIds);
                ids=ids.substring(1,ids.length()-1);
                updateSql+="("+ids+")";
                adapter.execute(updateSql);
            }
            outputMap.put("ErrorSquidIdList", cancelSquidIds);
            if (null != ftpConnection) {
                //sftp
                if (1 == ftpConnection.getProtocol()) {
                    SftpUtils sftpUtils = new SftpUtils();
                    ChannelSftp sftp = sftpUtils.connect(ftpConnection);
                    fileInfos = sftpUtils.listFiles(ftpConnection, ftpConnection.getMax_travel_depth(), 0, fileInfos, ftpConnection.getFile_path(), sftp, ftpConnection.getFilter());
                    sftpUtils.closeChannel();
                } else if (0 == ftpConnection.getProtocol()) {
                    //ftp
                    FtpUtils ftpUtils = new FtpUtils();
                    fileInfos = ftpUtils.browseftpFile(ftpConnection);
                }
                if (fileInfos == null) {
                    fileInfos = new ArrayList<>();
                }
                //存储DS_SOURCE_TABLE
                int sourceId = ftpConnection.getId();
                List<SourceTable> sourceTables = new FtpUtils().converFile2Source(fileInfos);
                 /*if(sourceTables.size()==0){
                     out.setMessageCode(MessageCode.ERR_URL);
				 }*/
                executeResult = connectProcess.connectionDBSourceTable(sourceTables, sourceId);
                if (executeResult < 0) {
                    out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                } else {
                    //只截取备注前200个字符，其余舍弃
                    String o = ftpConnection.getDescription();
                    if (o != null && o.length() > 200) {
                        String k = o.substring(0, 200);
                        ftpConnection.setDescription(k);
                    }
                    //存储DS_FILEFOLDER_CONNECTION
                    updateFlag = connectProcess.updateFtp(ftpConnection, out);
                    if (!updateFlag) {
                        out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                    } else {
                        List<DBSourceTable> dbSourceTables = connectProcess.getDBSourceTable(sourceId, out);
                        this.setSourceTable(sourceTables, dbSourceTables);
                        outputMap.put("SourceTables", sourceTables);
                    }
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("connectFtpSquid is error", e);
            out.setMessageCode(MessageCode.ERR_FTPFILE);
        }finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 根据连接信息获取hdfs的列表信息
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> connectHdfsSquid(String info,
                                                ReturnValue out) {
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
        Map<String, Object> outputMap = new HashMap<String, Object>();
        int executeResult = 0;
        boolean updateFlag = false;
        int squidType = 0;
        List<Integer> cancelSquidIds=new ArrayList<>();
        int updateSquidStatus=0;
        IRelationalDataManager adapter = null;
        List<Object> params = new ArrayList<>(); // 查询参数
        try {
            HdfsSquid hdfsConnection = JsonUtil
                    .object2HashMap(info, HdfsSquid.class);
            if (SysConf.getValue("hdfs_host").equals(hdfsConnection.getHost())) {
                hdfsConnection.setHost(SysConf.getValue("hdfsIpAndPort"));
            }
            if(SysConf.getValue("train_file_host").equals(hdfsConnection.getHost())){
                hdfsConnection.setHost(SysConf.getValue("train_file_real_host"));
            }
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            String sql=" select k.TO_SQUID_ID as TOSQUID from ds_squid_link k where k.FROM_SQUID_ID =?";
            params.clear();
            params.add(hdfsConnection.getId());
            //查询下游所有的抽取squid
            List<Map<String, Object>> squidIds=adapter.query2List(true,sql,params);
            if(squidIds!=null && squidIds.size()>0){
                for(Map<String,Object> map:squidIds){
                    cancelSquidIds.add(Integer.parseInt(map.get("TOSQUID").toString()));
                }
            }
            //修改这squid状态。
            if(cancelSquidIds!=null && cancelSquidIds.size()>0){
              List<Object> updateSquidIds=new ArrayList<>();
                String updateSql="update ds_squid  set DESIGN_STATUS=1 where id in ";
                updateSquidIds.addAll(cancelSquidIds);
                String ids = JsonUtil.toGsonString(updateSquidIds);
                ids=ids.substring(1,ids.length()-1);
                updateSql+="("+ids+")";
                adapter.execute(updateSql);
            }
            outputMap.put("ErrorSquidIdList", cancelSquidIds);




            if (null != hdfsConnection) {
                squidType = hdfsConnection.getSquid_type();
                List<FileInfo> fileInfos = HdfsUtils.getFileStatus(hdfsConnection);
                //存储DS_SOURCE_TABLE
                int sourceId = hdfsConnection.getId();
                List<SourceTable> sourceTables = new FtpUtils().converFile2Source(fileInfos);
                executeResult = connectProcess.connectionDBSourceTable(sourceTables, sourceId);
                if (executeResult < 0) {
                    out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                } else {
                    //只截取备注前200个字符，其余舍弃
                    String o = hdfsConnection.getDescription();
                    if (o != null && o.length() > 200) {
                        String k = o.substring(0, 200);
                        hdfsConnection.setDescription(k);
                    }
                    //存储DS_FILEFOLDER_CONNECTION
                    updateFlag = connectProcess.updateHdfs(hdfsConnection, out);
                    if (!updateFlag) {
                        out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                    } else {
                        List<DBSourceTable> dbSourceTables = connectProcess.getDBSourceTable(sourceId, out);
                        this.setSourceTable(sourceTables, dbSourceTables);
                        outputMap.put("SourceTables", sourceTables);
                    }
                }

            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("connectHdfsSquid is error", e);
            if (squidType == SquidTypeEnum.SOURCECLOUDFILE.value()) {
                out.setMessageCode(MessageCode.ERR_SOURCE_CLOUD_FILE_HDFS);
            } else {
                out.setMessageCode(MessageCode.ERR_HDFS);
            }
        } finally {
            if(adapter!=null){
                adapter.closeSession();
            }
        }
        return outputMap;
    }

    /**
     * 根据连接信息获取微博的列表信息
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> connectWeiboSquid(String info,
                                                 ReturnValue out) {
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
        Map<String, Object> outputMap = new HashMap<String, Object>();
        List<SourceTable> sourceTables = new ArrayList<SourceTable>();
        int executeResult = 0;
        boolean updateFlag = false;
        try {
            WeiboSquid weiboSquid = JsonUtil
                    .object2HashMap(info, WeiboSquid.class);
            if (null != weiboSquid) {
                logger.info("weibo id: " + weiboSquid.getId());
                //存储DS_SOURCE_TABLE和DS_SOURCE_COLUMN(微博的表和列都是固定的)
                DBSourceTable dbSourceTable = new DBSourceTable();
                dbSourceTable.setTable_name("weibo");
                dbSourceTable.setSource_squid_id(weiboSquid.getId());
                //由于可以多次点连接按钮,如果table_name和source_squid_id有记录,则不进行操作
                boolean flag = connectProcess.connectMore("weibo", weiboSquid.getId());
                if (flag)//有记录
                {
                    executeResult = 1;
                } else {
                    executeResult = connectProcess.createDBSourceTable(dbSourceTable, out, "weibo");
                }
                if (executeResult < 0) {
                    out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                } else {
                    //只截取备注前200个字符，其余舍弃
                    String o = weiboSquid.getDescription();
                    if (o != null && o.length() > 200) {
                        String k = o.substring(0, 200);
                        weiboSquid.setDescription(k);
                    }
                    //存储DS_WEIBO_CONNECTION
                    updateFlag = connectProcess.updateWeibo(weiboSquid, out);
                    if (!updateFlag) {
                        out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                    } else {
                        List<DBSourceTable> dbSourceTables = connectProcess.getDBSourceTable(weiboSquid.getId(), out);
                        //查询所有的
                        List<SourceColumn> sourceColumns = connectProcess.getSourceColumn(dbSourceTables.get(0).getId(), out);
                        connectProcess.setSourceTable(dbSourceTables.get(0), sourceColumns, sourceTables);
                        //this.setSourceTable(sourceTables, dbSourceTables);
                        outputMap.put("SourceTables", sourceTables);
                    }

                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("connectWeiboSquid is error", e);
            out.setMessageCode(MessageCode.ERR_WEIBO);
        }
        return outputMap;
    }

    /**
     * 根据连接信息获取网页的列表信息
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> connectWebSquid(String info,
                                               ReturnValue out) {
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
        Map<String, Object> outputMap = new HashMap<String, Object>();
        List<SourceTable> sourceTables = new ArrayList<SourceTable>();
        int executeResult = 0;
        boolean updateFlag = false;
        try {
            WebSquid webSquid = JsonUtil
                    .object2HashMap(info, WebSquid.class);
            if (null != webSquid) {
                //存储DS_SOURCE_TABLE和DS_SOURCE_COLUMN(微博的表和列都是固定的)
                DBSourceTable dbSourceTable = new DBSourceTable();
                dbSourceTable.setTable_name("web");
                dbSourceTable.setSource_squid_id(webSquid.getId());
                //由于可以多次点连接按钮,如果table_name和source_squid_id有记录,则不进行操作
                boolean flag = connectProcess.connectMore("web", webSquid.getId());
                if (flag)//有记录
                {
                    executeResult = 1;
                } else {
                    executeResult = connectProcess.createDBSourceTable(dbSourceTable, out, "web");
                }
                if (executeResult < 0) {
                    out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                } else {
                    //只截取备注前200个字符，其余舍弃
                    String o = webSquid.getDescription();
                    if (o != null && o.length() > 200) {
                        String k = o.substring(0, 200);
                        webSquid.setDescription(k);
                    }
                    //存储DS_WEIBO_CONNECTION
                    updateFlag = connectProcess.updateWeb(webSquid, out);
                    if (!updateFlag) {
                        out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                    } else {
                        List<DBSourceTable> dbSourceTables = connectProcess.getDBSourceTable(webSquid.getId(), out);
                        List<SourceColumn> sourceColumns = connectProcess.getSourceColumn(dbSourceTables.get(0).getId(), out);
                        connectProcess.setSourceTable(dbSourceTables.get(0), sourceColumns, sourceTables);
                        //this.setSourceTable(sourceTables, dbSourceTables);
                        outputMap.put("SourceTables", sourceTables);
                    }

                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("connectWebSquid is error", e);
            out.setMessageCode(MessageCode.ERR_WEB);
        }
        return outputMap;
    }

    /**
     * 根据id,查询表集合，获取连接信息以及分割属性
     *
     * @param adapter
     * @param sourceId
     * @param docExtractSquidID
     * @param out
     * @return
     */
    public List<String> getDocExtractSourceList(
            IRelationalDataManager adapter,
            int sourceId,
            int docExtractSquidID,
            ReturnValue out) throws Exception {
        //DataAdapterFactory adapterFactory = null;
        FileFolderSquid fileFolderSquid = null;
        FtpSquid ftpSquid = null;
        HdfsSquid hdfsSquid = null;
        List<String> list = new ArrayList<String>();
        Map<String, String> params = new HashMap<String, String>(); // 查询参数
        try {
            //查询DocExtractSquid
            params.clear();
            params.put("ID", String.valueOf(docExtractSquidID));
            DocExtractSquid docExtractSquid = adapter.query2Object2(true, params, DocExtractSquid.class);
            //在linux上将\r\n,替换成\n
            if (!OSUtils.isWindowsOS()) {
                String lineSeparator = System.getProperty("line.separator");
                if (docExtractSquid.getRow_delimiter() != null && docExtractSquid.getRow_delimiter().contains("\r\n")) {
                    String rowDelimiter = docExtractSquid.getRow_delimiter().replaceAll("\r\n", lineSeparator);
                    docExtractSquid.setRow_delimiter(rowDelimiter);
                }
                if (docExtractSquid.getDelimiter() != null && docExtractSquid.getDelimiter().contains("\r\n")) {
                    String delimiter = docExtractSquid.getDelimiter().replaceAll("\r\n", lineSeparator);
                    docExtractSquid.setDelimiter(delimiter);
                }
            }
            // fixed bug 680 start by bo.dang
            params.clear();
            params.put("id", Integer.toString(docExtractSquid.getSource_table_id(), 10));
            DBSourceTable sourceTable = adapter.query2Object2(true, params, DBSourceTable.class);
            String fileName = sourceTable.getTable_name();
            //先根据sourceID查询该squid的类型
            params.clear();
            params.put("ID", String.valueOf(sourceId));
            Squid squid = adapter.query2Object2(true, params, Squid.class);
            int encoding = squid.getEncoding();
            String encode = EncodingUtils.getEncoding(encoding);
            if (null != squid) {
                FileFolderUtils fileFolderUtils = new FileFolderUtils();
                if (squid.getSquid_type() == SquidTypeEnum.FILEFOLDER.value()) {
                    params.clear();
                    params.put("ID", String.valueOf(sourceId));
                    fileFolderSquid = adapter.query2Object2(true, params,
                            FileFolderSquid.class);
                    // 调用FileFolderUtils工具类获取指定文件的内容
                    //分共享文件和本地文件(对于服务器的共享文件，都可以当本地文件来读)
                    if (StringUtils.isBlank(fileFolderSquid.getHost())) {
                        list = getSubcontent(fileFolderSquid, list, docExtractSquid, fileName, encode, fileFolderUtils);
                    } else {
                        if (fileFolderSquid.getHost().equals(OSUtils.getLocalIP()))//按照本地文件
                        {
                            list = getSubcontent(fileFolderSquid, list, docExtractSquid, fileName, encode, fileFolderUtils);
                        } else {//共享文件(分Office和文本文档)
                            RemoteFileAdapter remoteFileAdapter = new RemoteFileAdapter();
                            if (".doc|.docx|.xls|.xlsx|.pdf".contains(FileUtils.getFileEx(fileName)) && FileUtils.getFileEx(fileName) != "") {
                                logger.info("开始解析office文件");
                                list = remoteFileAdapter.readOfficeFile(docExtractSquid, fileFolderSquid, fileName);
                            } else {
                                list = remoteFileAdapter.readFile(docExtractSquid, fileFolderSquid, fileName);

                            }
                        }
                    }
                } else if (squid.getSquid_type() == SquidTypeEnum.FTP.value()) {
                    params.clear();
                    params.put("ID", String.valueOf(sourceId));
                    ftpSquid = adapter.query2Object2(true, params, FtpSquid.class);
                    String ct = null;
                    if (0 == ftpSquid.getProtocol())// ftp
                    {
                        FtpUtils ftpUtils = new FtpUtils();
                        if (docExtractSquid.getDoc_format() == ExtractFileType.XLS.value() || docExtractSquid.getDoc_format() == ExtractFileType.XLSX.value()) {
                            list = ftpUtils.getExcelFileFromFTP(ftpSquid, fileName, docExtractSquid);
                        } else {
                            ct = ftpUtils.getFtpFileContent(ftpSquid, fileName, docExtractSquid.getDoc_format());// 全部的内容
                            // fixed bug 680 end by bo.dang
                            list = fileFolderUtils.getDocContent(ct, docExtractSquid, encode);
                        }
                    } else if (1 == ftpSquid.getProtocol())// sftp
                    {
                        SftpUtils sftpUtils = new SftpUtils();
                        if (docExtractSquid.getDoc_format() == ExtractFileType.XLS.value() || docExtractSquid.getDoc_format() == ExtractFileType.XLSX.value()) {
                            list = sftpUtils.getExcelFileContentFromSFTP(ftpSquid, fileName, docExtractSquid);
                        } else {
                            ct = sftpUtils.getSftpFileContent(ftpSquid, fileName, docExtractSquid.getDoc_format());// 全部的内容
                            list = fileFolderUtils.getDocContent(ct, docExtractSquid, encode);
                        }
                    }

                } else if (squid.getSquid_type() == SquidTypeEnum.HDFS.value()
                        || squid.getSquid_type() == SquidTypeEnum.SOURCECLOUDFILE.value()
                        || squid.getSquid_type() == SquidTypeEnum.TRAINNINGFILESQUID.value()) {
                    params.clear();
                    params.put("ID", String.valueOf(sourceId));
                    hdfsSquid = adapter.query2Object2(true, params, HdfsSquid.class);
                    HdfsUtils hdfsUtils = new HdfsUtils();
                    list = hdfsUtils.readHDFSFile(hdfsSquid, docExtractSquid, fileName, encode);
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            logger.error("getDocExtractSourceList is error", e);
            out.setMessageCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA);
            throw e;
        }
        return list;
    }

    private List<String> getSubcontent(FileFolderSquid fileFolderSquid,
                                       List<String> list, DocExtractSquid docExtractSquid,
                                       String fileName, String encode, FileFolderUtils fileFolderUtils) throws XmlException, OpenXML4JException, IOException, Exception {
        ExtractUtilityBase utility = new ExtractUtilityBase();
        if (docExtractSquid.getDoc_format() == ExtractFileType.XLSX
                .value()) {
            //转换成csv，防止内存溢出

			/*XSSFExcelExtractor xssfExcelExtractor = new XSSFExcelExtractor(
					FileFolderUtils.replacePath(fileFolderSquid
							.getFile_path()) + "/" + fileName);
			xssfExcelExtractor.setFormulasNotResults(true);
			xssfExcelExtractor.setIncludeSheetNames(false);
			list = utility.getXlsxValues(list, docExtractSquid, xssfExcelExtractor);*/
            String path = FileFolderUtils.replacePath(fileFolderSquid
                    .getFile_path()) + "/" + fileName;
            String csvPath = path.substring(0, path.indexOf(".xlsx")) + ".csv";
            OPCPackage p = OPCPackage.open(path, PackageAccess.READ);
            File file = new File(csvPath);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileOutputStream outStream = new FileOutputStream(file);
            PrintStream out = new PrintStream(outStream, false, "utf-8");
            XLSX2CSV xlsx2csv = new XLSX2CSV(p, out, 10);
            xlsx2csv.process();
            p.close();
            out.flush();
            out.close();
            //读取csv文件
            FileInputStream input = new FileInputStream(file);
            BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(input));
            int count = 0;
            int lineCount = 0;
            int headerRowNo = docExtractSquid.getHeader_row_no() - 1;// 列信息所在行
            int firstDataRowNo = docExtractSquid.getFirst_data_row_no() - 1;// 值属性所在行
            int endDataRowNo = firstDataRowNo + 50;
            String line = "";
            while ((line = readerBuffer.readLine()) != null) {
                if (lineCount >= endDataRowNo)
                    break;
                if (lineCount == headerRowNo) {
                    list.add(line);
                } else if (lineCount != headerRowNo && lineCount <= endDataRowNo) {
                    if (lineCount >= firstDataRowNo) {
                        list.add(line);
                    }
                }
                lineCount++;
            }
            //删除csv文件
            CreateFileUtil.deleteFile(csvPath);
        } else if (docExtractSquid.getDoc_format() == ExtractFileType.XLS.value()) {
             /*InputStream in = new FileInputStream(FileFolderUtils.replacePath(fileFolderSquid
						.getFile_path()) + "/" + fileName);
				BOMInputStream bomInputStream = new BOMInputStream(in);
				HSSFWorkbook workbook = new HSSFWorkbook(bomInputStream);
				ExcelExtractor extractor = new ExcelExtractor(workbook);
				extractor.setFormulasNotResults(true);
				extractor.setIncludeSheetNames(false);
				list =utility.getXlsValues(list, docExtractSquid, extractor);*/
            String path = FileFolderUtils.replacePath(fileFolderSquid
                    .getFile_path()) + "/" + fileName;
            String csvPath = path.substring(0, path.indexOf(".xls")) + ".csv";
            XLS2CSV xls2csv = new XLS2CSV(path, csvPath);
            xls2csv.process();
            FileInputStream stream = new FileInputStream(csvPath);
            BufferedReader readerBuffer = new BufferedReader(new InputStreamReader(stream, "utf-8"));
            int count = 0;
            int lineCount = 0;
            int headerRowNo = docExtractSquid.getHeader_row_no() - 1;// 列信息所在行
            int firstDataRowNo = docExtractSquid.getFirst_data_row_no() - 1;// 值属性所在行
            int endDataRowNo = firstDataRowNo + 50;
            String line = "";
            while ((line = readerBuffer.readLine()) != null) {
                if (lineCount >= endDataRowNo)
                    break;
                if (lineCount == headerRowNo) {
                    list.add(line);
                } else if (lineCount != headerRowNo && lineCount <= endDataRowNo) {
                    if (lineCount >= firstDataRowNo) {
                        list.add(line);
                    }
                }
                lineCount++;
            }
            //删除csv文件
        } else {
            String ct = fileFolderUtils.getFileContent(fileFolderSquid.getFile_path(), fileName, docExtractSquid.getDoc_format());
            list = fileFolderUtils.getDocContent(ct, docExtractSquid, encode);
        }
        return list;
    }

    /**
     * 获取XmlExtractSquid源文件的path
     *
     * @param adapter
     * @param squidLinkFromId
     * @param tableName
     * @param out
     * @return
     */
    public Map<String, Object> getXmlExtractSourcePath(
            IRelationalDataManager adapter,
            int squidLinkFromId,
            String tableName,
            ReturnValue out) {
        FileFolderSquid fileFolderSquid = null;
        FtpSquid ftpSquid = null;
        HdfsSquid hdfsSquid = null;
        Map<String, String> params = new HashMap<String, String>(); // 查询参数
        String filePath = null;
        String tempPath = null;
        Map<String, Object> resultMap = new HashMap<String, Object>();
        try {
            // 实例化相关的数据库处理类
            //先根据sourceTableID查询该squid的类型
            params.clear();
            params.put("ID", String.valueOf(squidLinkFromId));
            Squid squid = adapter.query2Object2(true, params, Squid.class);

            //int encoding=squid.getEncoding();
            //String encode=EncodingUtils.getEncoding(encoding);
            if (null != squid) {
                if (squid.getSquid_type() == SquidTypeEnum.FILEFOLDER.value()) {
                    params.clear();
                    params.put("ID", String.valueOf(squidLinkFromId));
                    fileFolderSquid = adapter.query2Object2(true, params, FileFolderSquid.class);
                    tempPath = fileFolderSquid.getFile_path();
                    if (tempPath.contains(":")) {
                        if (tempPath.endsWith("\\")) {
                            filePath = fileFolderSquid.getFile_path() + tableName;
                        } else {
                            // 获取文件名
                            filePath = fileFolderSquid.getFile_path() + "/" + tableName;
                        }
                        resultMap.put("delFlag", false);
                    } else {
                        // 下载文件
                        filePath = RemoteFileAdapter.downloadRmoteFile(fileFolderSquid, tableName);
                        // 是否要删除下载的文件
                        resultMap.put("delFlag", true);
                    }

                } else if (squid.getSquid_type() == SquidTypeEnum.FTP.value()) {
                    params.clear();
                    params.put("ID", String.valueOf(squidLinkFromId));
                    ftpSquid = adapter.query2Object2(true, params, FtpSquid.class);
                    FtpUtils ftpUtils = new FtpUtils();
                    // 获取文件名
                    filePath = ftpUtils.getFtpFilePath(ftpSquid, tableName);
                    resultMap.put("delFlag", true);
                } else if (squid.getSquid_type() == SquidTypeEnum.HDFS.value()
                        || squid.getSquid_type() == SquidTypeEnum.SOURCECLOUDFILE.value()
                        || squid.getSquid_type() == SquidTypeEnum.TRAINNINGFILESQUID.value()) {
                    params.clear();
                    params.put("ID", String.valueOf(squidLinkFromId));
                    hdfsSquid = adapter.query2Object2(true, params, HdfsSquid.class);
                    filePath = HdfsUtils.getHdfsFilePath(hdfsSquid, tableName);
                    resultMap.put("delFlag", true);
                }
            }


        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            logger.error("getDocExtractSourceList is error", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        }
        resultMap.put("filePath", filePath);
        return resultMap;
    }

    /**
     * 对SourceTable 进行赋值
     *
     * @param sourceTables
     * @param dbSourceTables
     */
    public void setSourceTable(List<SourceTable> sourceTables, List<DBSourceTable> dbSourceTables) {
        for (int i = 0; i < sourceTables.size(); i++) {
            for (int j = 0; j < dbSourceTables.size(); j++) {
                if (dbSourceTables.get(j).getTable_name().equals(sourceTables.get(i).getTableName())) {
                    sourceTables.get(i).setId(dbSourceTables.get(j).getId());
                    sourceTables.get(i).setIs_extracted(dbSourceTables.get(j).isIs_extracted());
                    break;
                }
            }
        }
    }

    /**
     * 查看file数据
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> viewFileData(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        String data = "";
        Timer timer = new Timer();
        final Map<String, Object> returnMap = new HashMap<>();
        returnMap.put("1", 1);
        key = TokenUtil.getKey();
        token = TokenUtil.getToken();
        try {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMap, DSObjectType.FILEFOLDER, "1000", "0215", key, token, MessageCode.BATCH_CODE.value());
                }
            }, 25 * 1000, 25 * 1000);
            //反序列化对象
            Map<String, Object> map = JsonUtil.toHashMap(info);
            FileFolderSquid fileFolderSquid = JsonUtil.toGsonBean(map.get("FileFolderSquid").toString(), FileFolderSquid.class);
            fileFolderSquid.setFile_path(fileFolderSquid.getFile_path().replaceAll("\\\\", "/"));
            String fileName = map.get("FileName").toString();
            FileFolderUtils fileFolderUtils = new FileFolderUtils();

            //分共享文件和本地文件(对于服务器的共享文件，都可以当本地文件来读)
            if (StringUtils.isBlank(fileFolderSquid.getHost())) {
                String content = fileFolderUtils.getFileContent(fileFolderSquid.getFile_path(), fileName, -1);
                if (StringUtils.isBlank(content))//读取大型文件
                {
                    data = FileFolderUtils.getSomeContent(50, fileFolderSquid.getFile_path() + "/" + fileName, EncodingUtils.getEncoding(fileFolderSquid.getEncoding()));
                } else {
                    data = fileFolderUtils.getPartContent(content, EncodingUtils.getEncoding(fileFolderSquid.getEncoding()));
                }
            } else {
                if (fileFolderSquid.getHost().equals(OSUtils.getLocalIP()))//按照本地文件
                {
                    String content = fileFolderUtils.getFileContent(fileFolderSquid.getFile_path(), fileName, -1);
                    if (StringUtils.isBlank(content))//读取大型文件
                    {
                        data = FileFolderUtils.getSomeContent(50, fileFolderSquid.getFile_path() + "/" + fileName, EncodingUtils.getEncoding(fileFolderSquid.getEncoding()));
                    } else {
                        data = fileFolderUtils.getPartContent(content, EncodingUtils.getEncoding(fileFolderSquid.getEncoding()));
                    }
                } else {//共享文件(分Office和文本文档)
                    RemoteFileAdapter remoteFileAdapter = new RemoteFileAdapter();
                    if (StringUtils.isNotBlank(FileUtils.getFileEx(fileName)) && ".doc|.docx|.xls|.xlsx|.pdf".contains(FileUtils.getFileEx(fileName))) {
                        data = remoteFileAdapter.getPartOfficeContent(fileFolderSquid, fileName, -1);
                    } else {
                        data = remoteFileAdapter.getPartContent(fileFolderSquid, fileName);
                        //data = remoteFileAdapter.getPartOfficeContent(fileFolderSquid, fileName,1);
                    }
                }
            }
            data = URLEncoder.encode(data, "utf-8");
        } catch (NotOLE2FileException e) {
            logger.error(e);
            logger.error("加密的Office文件");
            out.setMessageCode(MessageCode.ERR_FILEOLE2);
            timer.purge();
            timer.cancel();
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_FILE);
            timer.purge();
            timer.cancel();
        } finally {
            timer.purge();
            timer.cancel();
        }
        outputMap.put("data", data);
        return outputMap;
    }

    /**
     * 查看ftp数据
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> viewFtpData(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        String data = "";
        String content = "";
        Timer timer = new Timer();
        key = TokenUtil.getKey();
        token = TokenUtil.getToken();
        try {
            //增加定时器
            final Map<String, Object> returnMap = new HashMap<>();
            returnMap.put("1", 1);
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMap, DSObjectType.FTP, "1000", "0216", key, token, MessageCode.BATCH_CODE.value());
                }
            }, 25 * 1000, 25 * 1000);
            //反序列化对象
            Map<String, Object> map = JsonUtil.toHashMap(info);//"FileSquid""FileName"
            FtpSquid ftpSquid = JsonUtil.toGsonBean(map.get("FtpSquid").toString(), FtpSquid.class);
            ftpSquid.setFile_path(ftpSquid.getFile_path().replaceAll("\\\\", "/"));
            String fileName = map.get("FileName").toString();

            FileFolderUtils fileFolderUtils = new FileFolderUtils();
            if (0 == ftpSquid.getProtocol())//ftp
            {
                FtpUtils ftpUtils = new FtpUtils();
                content = ftpUtils.getFtpFileContent(ftpSquid, fileName, -1);//全部的内容
            } else if (1 == ftpSquid.getProtocol())//sftp
            {
                SftpUtils sftpUtils = new SftpUtils();
                content = sftpUtils.getSftpFileContent(ftpSquid, fileName, -1);//全部的内容
            }
            data = fileFolderUtils.getPartContent(content, EncodingUtils.getEncoding(ftpSquid.getEncoding()));
            data = URLEncoder.encode(data, "utf-8");
        } catch (Exception e) {
            // TODO: handle exception
            logger.error(e);
            out.setMessageCode(MessageCode.ERR_FTPCONTENT);
            timer.purge();
            timer.purge();
        } finally {
            timer.purge();
            timer.cancel();
        }
        outputMap.put("data", data);
        return outputMap;
    }

    /**
     * 查看hdfs数据
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> viewHdfsData(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        String data = "";
        HdfsSquid hdfsSquid = null;
        //增加定时器
        Timer timer = new Timer();
        final Map<String, Object> returnMap = new HashMap<>();
        returnMap.put("1", 1);
        key = TokenUtil.getKey();
        token = TokenUtil.getToken();
        //反序列化对象
        try {
            Map<String, Object> map = JsonUtil.toHashMap(info);//"FileSquid""FileName"
            hdfsSquid = JsonUtil.toGsonBean(map.get("HdfsSquid").toString(), HdfsSquid.class);
            hdfsSquid.setFile_path(hdfsSquid.getFile_path().replaceAll("\\\\", "/"));
            DSObjectType type = null;
            if (hdfsSquid.getSquid_type() == SquidTypeEnum.SOURCECLOUDFILE.value()) {
                type = DSObjectType.SOURCECLOUDFILE;
            } else if(hdfsSquid.getSquid_type() == SquidTypeEnum.TRAINNINGFILESQUID.value()){
                type = DSObjectType.TRAINNINGFILESQUID;
            } else {
                type = DSObjectType.HDFS;
            }
            final DSObjectType type2 = type;
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMap, type2, "1000", "0217", key, token, MessageCode.BATCH_CODE.value());
                }
            }, 25 * 1000, 25 * 1000);

            String fileName = map.get("FileName").toString();
            if (SysConf.getValue("hdfs_host").equals(hdfsSquid.getHost())) {
                hdfsSquid.setHost(SysConf.getValue("hdfsIpAndPort"));
            }
            if (SysConf.getValue("train_file_host").equals(hdfsSquid.getHost())) {
                hdfsSquid.setHost(SysConf.getValue("train_file_real_host"));
            }
            boolean unCompression = Boolean.valueOf(map.get("UnCompression").toString());
            HdfsUtils hdfsUtils = new HdfsUtils();
            data = hdfsUtils.getPartContent(hdfsSquid, fileName, EncodingUtils.getEncoding(hdfsSquid.getEncoding()), -1, unCompression);
            //编码防止读取文件乱码时，客户端报反序列化异常
            data = URLEncoder.encode(data, "utf-8");
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            if (hdfsSquid.getSquid_type() == SquidTypeEnum.SOURCECLOUDFILE.value()) {
                out.setMessageCode(MessageCode.ERR_SOURCE_CLOUD_FILE_HDFS);
            } else {
                out.setMessageCode(MessageCode.ERR_HDFS);
            }
        } finally {
            timer.purge();
            timer.cancel();
        }
        outputMap.put("data", data);
        return outputMap;
    }

    /**
     * 获取某Repository下的所有的DataMining Squid的集合
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> getAllDataMiningSquidInRepository(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            int squidId = Integer.valueOf(parmsMap.get("SquidId") + "");
            boolean isFromCurrentProject = Boolean.parseBoolean(parmsMap.get("IsFromCurrentProject") + "");
            if (squidId > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                IRepositoryDao repositoryDao = new RepositoryDaoImpl(adapter3);
                String filterIds = this.getFilterSquidIds(adapter3, squidId);

                int id = 0;
                if (isFromCurrentProject) {
                    id = repositoryDao.getProjectIdBySquid(squidId);
                } else {
                    id = repositoryDao.getRepositoryIdBySquid(squidId);
                }
                ISquidDao squidDao = new SquidDaoImpl(adapter3);
                Transformation transformation = squidDao.getSquidForCond(squidId, Transformation.class);
                List<Map<String, Object>> vals = repositoryDao.getAllDataMiningSquidInRepository(isFromCurrentProject, id, filterIds);
                outputMap.put("DataList", vals);
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            logger.error("[获取getAllDataMiningSquidInRepository===========================exception]", e);
            try {
                adapter3.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter3.closeSession();
        }
        return null;
    }

    /**
     * 云端接口，获取Quantify Squid集合
     */
    public Map<String, Object> getAllDataMiningSquidInCloud(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        int squidId = Integer.valueOf(paramsMap.get("SquidId") + "");
        List<Integer> ListProjectId = (List<Integer>) paramsMap.get("ListProjectId");
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        List<Map<String, Object>> dataList = new ArrayList<>();
        try {
            adapter.openSession();
            IRepositoryDao repositoryDao = new RepositoryDaoImpl(adapter);
            if (squidId > 0) {
                String filterIds = this.getFilterSquidIds(adapter, squidId);
                if (ListProjectId != null && ListProjectId.size() > 0) {
                    for (Integer id : ListProjectId) {
                        List<Map<String, Object>> dataLists = repositoryDao.getAllDataMiningSquidInRepository(true, id.intValue(), filterIds);
                        if (dataLists != null && dataLists.size() > 0) {
                            dataList.addAll(dataLists);
                        }
                    }
                } else {
                    out.setMessageCode(MessageCode.NODATA);
                }
                outputMap.put("DataList", dataList);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            logger.error("获取getAllDataMiningSquidInCloud异常");
            e.printStackTrace();
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 获取某Repository下的所有的Quantify Squid的集合
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> getAllQuantifySquidInRepository(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            int squidId = Integer.valueOf(parmsMap.get("SquidId") + "");
            boolean isFromCurrentProject = Boolean.parseBoolean(parmsMap.get("IsFromCurrentProject") + "");
            if (squidId > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                IRepositoryDao repositoryDao = new RepositoryDaoImpl(adapter3);
                String filterIds = this.getFilterSquidIds(adapter3, squidId);
                int id = 0;
                if (isFromCurrentProject) {
                    id = repositoryDao.getProjectIdBySquid(squidId);
                } else {
                    id = repositoryDao.getRepositoryIdBySquid(squidId);
                }

                //int projectId = repositoryDao.getProjectIdBySquid(squidId);
                List<Map<String, Object>> vals = repositoryDao.getAllQuantifySquidInRepository(isFromCurrentProject, id, filterIds);
                outputMap.put("DataList", vals);
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            logger.error("[获取getAllQuantifySquidInRepository===========================exception]", e);
            try {
                adapter3.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter3.closeSession();
        }
        return null;
    }

    /**
     * 获取某Repository下的所有的DataMining Version的集合
     *
     * @param info
     * @param out
     * @return
     */
    public Map<String, Object> getDataMiningVersions(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            int repositoryId = Integer.valueOf(parmsMap.get("RepositoryId") + "");
            int squidId = Integer.valueOf(parmsMap.get("SquidId") + "");
            if (repositoryId > 0 && squidId > 0) {
                DBConnectionInfo dbinfo = HbaseAdapter.getHbaseConnection();
                HbaseAdapter adapter = new HbaseAdapter(dbinfo);
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();

                ISquidDao squidDao = new SquidDaoImpl(adapter3);
                Squid squid = squidDao.getObjectById(squidId, Squid.class);
                if (squid != null) {
                    //训练表名字
                    //String tableName = HbaseUtil.genTrainModelTableName(repositoryId, squid.getSquidflow_id(), squidId);
                    String tableName = "t_" + repositoryId + "_" + squid.getSquidflow_id() + "_" + squidId;
                    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
                    try {
                        list = adapter.queryForList(new SelectSQL(tableName.toUpperCase(), "VERSION"), null);
                    } catch (Exception e) {
                        logger.info("tableName is null : " + tableName);
                    }
                    List<Integer> versions = new ArrayList<Integer>();
                    if (list != null && list.size() > 0) {
                        for (Map<String, Object> map : list) {
                            int val = Integer.parseInt(map.get("VERSION") + "");
                            versions.add(val);
                        }
                    }
                    HashSet h = new HashSet(versions);
                    versions.clear();
                    versions.addAll(h);
                    outputMap.put("Versions", versions);
                    return outputMap;
                } else {
                    out.setMessageCode(MessageCode.ERR_SQUID_NULL);
                }
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            try {
                adapter3.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            logger.error("[获取getAllDataMiningSquidInRepository===========================exception]", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter3.closeSession();
        }
        return null;
    }

    /**
     * 排除squidId 下游的 DataMiningSquid
     *
     * @param adapter3
     * @param squidId
     * @return
     */
    public String getFilterSquidIds(IRelationalDataManager adapter3, int squidId) {
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
        List<SquidLink> link = squidLinkDao.getSquidLinkListByFromSquid(squidId);
        String ids = "";
        if (link != null && link.size() > 0) {
            for (SquidLink squidLink : link) {
                int fromSquidId = squidLink.getTo_squid_id();
                String val = getFilterSquidIds(adapter3, fromSquidId);
                if (!ValidateUtils.isEmpty(val)) {
                    ids += "," + val;
                }
            }
            ids += "," + squidId;
        } else {
            ids += "," + squidId;
        }
        if (ids != "") {
            ids = ids.substring(1);
        }
        return ids;
    }

    /**
     * 根据squid_id查询所有的url
     *
     * @param adapter3
     * @param squid_id
     * @return
     */
    public List<Url> getAllUrls(IRelationalDataManager adapter3, int squid_id) {
        Map<String, Object> params = new HashMap<String, Object>(); // 查询参数
        List<Url> urls = new ArrayList<Url>();
        params.put("squid_id", squid_id);
        urls = adapter3.query2List2(true, params, Url.class);
        return urls;
    }

    /**
     * 更新cdc属性
     *
     * @param info
     * @return
     */
    public Map<String, Object> manageDataSquidByCDC(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            DataSquid data = JsonUtil.toGsonBean(parmsMap.get("Squid") + "", DataSquid.class);
            if (data != null && data.getId() > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                ISquidDao squidDao = new SquidDaoImpl(adapter3);
                DataSquid tempSquid = squidDao.getSquidForCond(data.getId(), DataSquid.class);
                int status = 0; //1、删除   2、添加
                if (tempSquid != null) {
                    //判断当前设置给予相关的处理状态,
                    if (data.isIs_persisted()) {
                        if (data.getCdc() == 0 && tempSquid.getCdc() == 1) {
                            status = 1;
                        } else if (data.getCdc() == 1 && tempSquid.getCdc() == 0) {
                            status = 2;
                        } else {
                            //只需要更新Squid
                            int cnt = squidDao.updateDataSquid(data);
                            return outputMap;
                        }
                    } else {
                        if (tempSquid.isIs_persisted() && tempSquid.getCdc() == 1) {
                            status = 1;
                            //如果取消落地，自动取消cdc设置
                            data.setCdc(0);
                        }
                    }
                    //删除设置了cdc附加的属性
                    if (status == 1) {
                        outputMap.putAll(this.delectCdcAndColumn(adapter3, data, out));
                        outputMap.put("newSquidPropertys", new ArrayList<Column>());
                    } else if (status == 2) {//新增附加cdc设置
                        outputMap.putAll(this.addCdcAndColumn(adapter3, data));
                        outputMap.put("delSquidPropertys", new ArrayList<Integer>());
                    }
                } else {
                    out.setMessageCode(MessageCode.NODATA);
                }
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            try {
                if (adapter3 != null) {
                    adapter3.rollback();
                    logger.error("事物回滚");
                }
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            logger.error("[获取getAllDataMiningSquidInRepository===========================exception]", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter3 != null) adapter3.closeSession();
        }
        return outputMap;
    }

    /**
     * 删除cdc创建的column及下游squid中的RefColumn
     *
     * @param adapter3
     * @param squid
     * @param out
     * @return
     * @throws Exception
     */
    public Map<String, Object> delectCdcAndColumn(IRelationalDataManager adapter3, DataSquid squid, ReturnValue out) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        IColumnDao columnDao = new ColumnDaoImpl(adapter3);
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataShirServiceplug plug = new DataShirServiceplug(TokenUtil.getToken());
        int cnt = squidDao.updateDataSquid(squid);
        if (cnt > 0) {
            List<Column> listColumns = columnDao.getColumnListForCdc(squid.getId());
            List<DataSquidCollectionPropertyId> dataList = new ArrayList<DataSquidCollectionPropertyId>();
            if (listColumns != null && listColumns.size() > 0) {
                for (Column column : listColumns) {
                    List<DataSquidCollectionPropertyId> tempList =
                            new ArrayList<DataSquidCollectionPropertyId>();
                    boolean flag = plug.delColumn(adapter3, column.getId(), 0, out, tempList);
                    if (!flag) {
                        if (out.getMessageCode() == MessageCode.SUCCESS) {
                            out.setMessageCode(MessageCode.DELETE_NOTEXISTS);
                        }
                        return null;
                    } else {
                        this.fillDataForList(dataList, tempList);
                    }
                }
            } else {
                DataSquidCollectionPropertyId temp = new DataSquidCollectionPropertyId();
                temp.setSquidId(squid.getId());
                dataList.add(temp);
            }
            outputMap.put("delSquidPropertys", dataList);
        }
        return outputMap;
    }

    /**
     * 合并Map中的值
     *
     * @param dataList
     * @param tempList
     */
    public void fillDataForList(List<DataSquidCollectionPropertyId> dataList,
                                List<DataSquidCollectionPropertyId> tempList) {
        if (dataList.size() > 0) {
            for (DataSquidCollectionPropertyId p : dataList) {
                for (DataSquidCollectionPropertyId c : tempList) {
                    if (p.getSquidId() == c.getSquidId()) {
                        p.getColumnIds().addAll(c.getColumnIds());
                        p.getReferenceColumnIds().addAll(c.getReferenceColumnIds());
                        p.getTransformationIds().addAll(c.getTransformationIds());
                        p.getTransformLinkIds().addAll(c.getTransformLinkIds());
                        p.getUpdateInputs().addAll(c.getUpdateInputs());
                        p.getDeleteInputs().addAll(c.getDeleteInputs());
                    }
                }
            }
        } else {
            dataList.addAll(tempList);
        }
    }

    /**
     * 添加cdc需要添加的column信息及同步下游squid的refColumn
     *
     * @param adapter3
     * @param squid
     * @return
     * @throws Exception
     */
    @SuppressWarnings("static-access")
    public Map<String, Object> addCdcAndColumn(IRelationalDataManager adapter3, DataSquid squid) throws Exception {
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        IColumnDao columnDao = new ColumnDaoImpl(adapter3);
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
        //当前操作只修改cdc及落地值
        Map<String, Object> outputMap = new HashMap<String, Object>();
        List<Column> newColumns = new ArrayList<Column>();
        List<DataSquidCollectionProperty> dataList = new ArrayList<DataSquidCollectionProperty>();
        List<Transformation> newTrans = new ArrayList<Transformation>();
        List<Column> columns = new ArrayList<Column>();
        TransformationService service = new TransformationService(TokenUtil.getToken());

        List<DestImpalaColumn> newCol = new ArrayList<DestImpalaColumn>();
        List<DestHDFSColumn> newCol1 = new ArrayList<DestHDFSColumn>();
        List<EsColumn> newCol2 = new ArrayList<EsColumn>();

        int cnt = squidDao.updateDataSquid(squid);
        if (cnt > 0) {
            columns.add(new Column("start_date", SystemDatatype.DATETIME.value(), 0));
            columns.add(new Column("end_date", SystemDatatype.DATETIME.value(), 0));
            columns.add(new Column("active_flag", SystemDatatype.NCHAR.value(), 1));
            columns.add(new Column("version", SystemDatatype.SMALLINT.value(), 0));

            //查询当前squid中存在的column个数
            int order = columnDao.getColumnCountBySquidId(squid.getId());
            //查询下游squid数量
            List<SquidLink> squidLinkList = squidLinkDao.getSquidLinkListByCdc(squid.getId());
            //添加新的column，并保存新的newcolumn集合
            for (Column column : columns) {
                Column newColumn = service.initColumn(adapter3, column.getName(),
                        column.getData_type(), column.getLength(),
                        order + 1, squid.getId());
                newColumns.add(newColumn);

                Transformation trans = service.initTransformation(adapter3, squid.getId(),
                        newColumn.getId(), TransformationTypeEnum.VIRTUAL.value(),
                        newColumn.getData_type(), 1);
                newTrans.add(trans);
                order++;
            }
            DataSquidCollectionProperty newData = new DataSquidCollectionProperty(squid.getId(), newColumns,
                    new ArrayList<ReferenceColumn>(),
                    newTrans, new ArrayList<TransformationLink>());
            dataList.add(newData);
            //根据column信息同步下游squid中的ReferenceColumn
            synchronizedCdc(adapter3, squidLinkList, newColumns, null, newCol, newCol1, newCol2, dataList, squid, true);
        }
        outputMap.put("newImpalaColumn", newCol);
        outputMap.put("newHDFSColumn", newCol1);
        outputMap.put("newEsColumn", newCol2);
        outputMap.put("newSquidPropertys", dataList);
        return outputMap;
    }


    public void synchronizedCdc(IRelationalDataManager adapter3,
                                List<SquidLink> squidLinkList, List<Column> newColumns, List<Column> exceptionColumns,
                                List<DestImpalaColumn> newCol, List<DestHDFSColumn> newCol1,
                                List<EsColumn> newCol2, List<DataSquidCollectionProperty> dataList,
                                Squid squid, boolean isNeedSyn) throws Exception {
        if (squidLinkList != null && squidLinkList.size() > 0) {
            IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
            ISquidDao squidDao = new SquidDaoImpl(adapter3);
            TransformationService service = new TransformationService(TokenUtil.getToken());
            ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
            IColumnDao columnDao = new ColumnDaoImpl(adapter3);
            for (SquidLink squidLink : squidLinkList) {
                int count = 0;
                int groupId = 0;
                int columnGroupOrder = 0;
                List<ReferenceColumn> refColumnList = new ArrayList<ReferenceColumn>();
                List<Transformation> transList = new ArrayList<Transformation>();
                Squid tSquid = squidDao.getObjectById(squidLink.getTo_squid_id(), Squid.class);
                Squid uSquid = squidDao.getObjectById(squidLink.getFrom_squid_id(), Squid.class);
                //获取当前squid中的refColumn信息
                List<ReferenceColumn> rc = refColumnDao.getRefColumnListBySquid(squidLink.getFrom_squid_id(),
                        squidLink.getTo_squid_id());
                if (tSquid.getSquid_type() != SquidTypeEnum.EXCEPTION.value()) {
                    if (rc != null && rc.size() > 0) {
                        groupId = rc.get(0).getGroup_id();
                        columnGroupOrder = rc.get(0).getRelative_order();
                    } else {
                        count = refColumnDao.getRefGroupCountForSquidId(squidLink.getTo_squid_id());
                        ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
                        columnGroup.setKey(StringUtils.generateGUID());
                        columnGroup.setReference_squid_id(squidLink.getTo_squid_id());
                        columnGroup.setRelative_order(count + 1);
                        columnGroup.setId(adapter3.insert2(columnGroup));
                        groupId = columnGroup.getId();
                        columnGroupOrder = columnGroup.getRelative_order();
                    }
                }
                if (tSquid != null) {
//					if (tSquid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()) {  //点击取消CDC时，过滤掉对exceptionSquid的更新
//						break;
//					}
                    //点击CDC  同步更新下游的落地squid   修改bug3735
                    if (tSquid.getSquid_type() == SquidTypeEnum.DEST_IMPALA.value()) {
                        for (Column column : newColumns) {
                            if (uSquid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                                if (column.getName().equals(ConstantsUtil.CN_GROUP_TAG)) {
                                    continue;
                                }
                            }
                            DestImpalaColumn impalaColumn = ImpalaColumnService.getImpalaColumnByColumn(column, tSquid.getId());
                            adapter3.insert2(impalaColumn);
                            newCol.add(impalaColumn);
                        }
                    } else if (tSquid.getSquid_type() == SquidTypeEnum.DEST_HDFS.value() || tSquid.getSquid_type() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                        for (Column column : newColumns) {
                            if (uSquid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                                if (column.getName().equals(ConstantsUtil.CN_GROUP_TAG)) {
                                    continue;
                                }
                            }
                            DestHDFSColumn hdfsColumn = HdfsColumnService.getHDFSColumnByColumn(column, tSquid.getId());
                            adapter3.insert2(hdfsColumn);
                            newCol1.add(hdfsColumn);
                        }
                    } else if (tSquid.getSquid_type() == SquidTypeEnum.DESTES.value()) {
                        for (Column column : newColumns) {
                            if (uSquid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                                if (column.getName().equals(ConstantsUtil.CN_GROUP_TAG)) {
                                    continue;
                                }
                            }
                            EsColumn esColumn = EsColumnService.genEsColumnByColumn(column, tSquid.getId());
                            adapter3.insert2(esColumn);
                            newCol2.add(esColumn);
                        }
                    } else if (tSquid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()) {
                        //查询当前squid中存在的column个数
                        int order = columnDao.getColumnCountBySquidId(tSquid.getId());
                        List<TransformationLink> links = new ArrayList<>();
                        List<Column> leftColumns = new ArrayList<>();
                        String columnName = null;
                        if (exceptionColumns != null && exceptionColumns.size() > 0) {
                            Squid formSquid = squidDao.getObjectById(exceptionColumns.get(0).getSquid_id(), Squid.class);
                            columnName = formSquid.getName() + "_";
                            count = refColumnDao.getRefGroupCountForSquidId(formSquid.getId());
                            ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
                            columnGroup.setKey(StringUtils.generateGUID());
                            columnGroup.setReference_squid_id(formSquid.getId());
                            columnGroup.setRelative_order(count + 1);
                            columnGroup.setId(adapter3.insert2(columnGroup));
                            groupId = columnGroup.getId();
                            columnGroupOrder = columnGroup.getRelative_order();
                            for (Column column : exceptionColumns) {
                                Column newColumn = service.initColumn(adapter3, columnName + column.getName(),
                                        column.getData_type(), column.getLength(),
                                        order + 1, tSquid.getId());
                                leftColumns.add(newColumn);
                                Transformation leftTrans = service.initTransformation(adapter3, tSquid.getId(),
                                        newColumn.getId(), TransformationTypeEnum.VIRTUAL.value(),
                                        newColumn.getData_type(), 1);
                                order++;
                                transList.add(leftTrans);
                                ReferenceColumn refColumn = service.initReference(adapter3, column, column.getId(),
                                        column.getRelative_order(), formSquid,
                                        squidLink.getTo_squid_id(), groupId, columnGroupOrder);
                                refColumnList.add(refColumn);

                                Transformation trans = service.initTransformation(adapter3, squidLink.getTo_squid_id(),
                                        refColumn.getColumn_id(), TransformationTypeEnum.VIRTUAL.value(),
                                        refColumn.getData_type(), 1);
                                transList.add(trans);

                                TransformationLink tranLink = service.initTransformationLink(adapter3, trans.getId(), leftTrans.getId(), column.getRelative_order());
                                links.add(tranLink);
                            }
                            //把获取到的ReferenceColumn和Trans添加到集合里面去
                            DataSquidCollectionProperty tempData = new DataSquidCollectionProperty(tSquid.getId(),
                                    leftColumns, refColumnList,
                                    transList, links);
                            dataList.add(tempData);
                        }
                    } else {
                        List<TransformationLink> links = new ArrayList<>();
                        List<Column> leftColumns = new ArrayList<>();
                        Map<String, Object> nameColumn = new HashMap<>();
                        List<Column> columnList = new ArrayList<>();
                        for (ReferenceColumn referenceColumn : rc) {
                            for (Column column : newColumns) {
                                if (referenceColumn.getColumn_id() == column.getId()) {
                                    columnList.add(column);
                                }
                            }
                        }
                        newColumns.removeAll(columnList);
//						boolean flag=false;
//						if(rc!=null&&rc.size()>0){
//							flag= refColumnDao.deleReferenceColumnBySquidId(squidLink.getFrom_squid_id(),
//									squidLink.getTo_squid_id());
//						}
//						List<ReferenceColumn> r = refColumnDao.getRefColumnListBySquid(squidLink.getFrom_squid_id(),
//								squidLink.getTo_squid_id());
                        for (Column column : newColumns) {
//							if(uSquid.getSquid_type()==SquidTypeEnum.GROUPTAGGING.value()){
//								if(column.getName().equals(ConstantsUtil.CN_GROUP_TAG)){
//									continue;
//								}
//							}
                            //根据column信息同步下游squid中的ReferenceColumn(变换面板左边)
                            ReferenceColumn refColumn = service.initReference(adapter3, column, column.getId(),
                                    column.getRelative_order(), squid,
                                    squidLink.getTo_squid_id(), groupId, columnGroupOrder);
                            refColumnList.add(refColumn);

                            Transformation trans = service.initTransformation(adapter3, squidLink.getTo_squid_id(),
                                    refColumn.getColumn_id(), TransformationTypeEnum.VIRTUAL.value(),
                                    refColumn.getData_type(), 1);
                            transList.add(trans);
                            if (tSquid.getSquid_type() == SquidTypeEnum.GROUPTAGGING.value()) {
                                //变换面板右边
                                Column leftColumn = service.initColumn(adapter3, column, column.getRelative_order(), tSquid.getId(), nameColumn);
                                leftColumns.add(leftColumn);
                                Transformation leftTrans = service.initTransformation(adapter3, squidLink.getTo_squid_id(), leftColumn.getId(), TransformationTypeEnum.VIRTUAL.value(), column.getData_type(), 1);
                                transList.add(leftTrans);
                                //创建TransformationLink
                                TransformationLink tranLink = service.initTransformationLink(adapter3, trans.getId(), leftTrans.getId(), column.getRelative_order());
                                links.add(tranLink);
                                isNeedSyn = true;
                            }
                        }
                        //把获取到的ReferenceColumn和Trans添加到集合里面去
                        DataSquidCollectionProperty tempData = new DataSquidCollectionProperty(squidLink.getTo_squid_id(),
                                leftColumns, refColumnList,
                                transList, links);
                        dataList.add(tempData);
                    }
                }
                //递归
                if (isNeedSyn) {
                    //查询出SquidLink
                    List<SquidLink> squidLinks = squidLinkDao.getSquidLinkListByCdc(squidLink.getTo_squid_id());
                    for (SquidLink squidLink1 : squidLinks) {
                        Squid toSquid = squidDao.getObjectById(squidLink1.getTo_squid_id(), Squid.class);
                        if (toSquid.getSquid_type() == SquidTypeEnum.EXCEPTION.value()) {
                            exceptionColumns = newColumns;
                        }
                    }
                    List<Column> newToColumns = columnDao.getColumnListBySquidId(squidLink.getTo_squid_id());
                    synchronizedCdc(adapter3, squidLinks, newToColumns, exceptionColumns, newCol, newCol1, newCol2, dataList, tSquid, isNeedSyn);
                }
            }
        }
    }

    /**
     * 批量设置dataSquid落地属性为true
     *
     * @param info
     * @return
     */
    public Map<String, Object> setSquidsPersistence(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            List<Integer> squidIds = JsonUtil.toGsonList(parmsMap.get("SquidIds") + "",
                    Integer.class);
            if (squidIds != null && squidIds.size() > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                ISquidDao squidDao = new SquidDaoImpl(adapter3);
                String ids = JsonUtil.toJSONString(squidIds);
                if (!ValidateUtils.isEmpty(ids)) {
                    ids = ids.substring(1);
                    ids = ids.substring(0, ids.length() - 1);
                }
                squidDao.setPersistenceByIds(ids);
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            try {
                if (adapter3 != null) {
                    adapter3.rollback();
                    logger.error("事物回滚");
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            logger.error("[获取setSquidsPersistence===========================exception]", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter3 != null) adapter3.closeSession();
        }
        return outputMap;
    }

    /**
     * 批量设置落地目标
     */
    public Map<String, Object> setSquidsDest(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            List<Integer> squidIds = JsonUtil.toGsonList(parmsMap.get("SquidIds") + "",
                    Integer.class);
            int destSquidId = Integer.parseInt(parmsMap.get("DestSquidId") + "");
            if (squidIds != null && squidIds.size() > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                ISquidDao squidDao = new SquidDaoImpl(adapter3);
                String ids = JsonUtil.toJSONString(squidIds);
                if (!ValidateUtils.isEmpty(ids)) {
                    ids = ids.substring(1);
                    ids = ids.substring(0, ids.length() - 1);
                }
                if (destSquidId > 0) {
                    squidDao.setDestSquidByIds(destSquidId, ids);
                }
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            try {
                if (adapter3 != null) {
                    adapter3.rollback();
                    logger.error("事物回滚");
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            logger.error("[设置setDestSquid异常===========================exception]", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter3 != null) adapter3.closeSession();
        }
        return outputMap;
    }

    /**
     * 批量取消dataSquid落地属性（需要考虑cdc的处理）
     *
     * @param info
     * @return
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> cancelSquidsPersistence(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter3 = null;
        try {
            Map<String, Object> parmsMap = JsonUtil.toHashMap(info);
            List<Integer> squidIds = JsonUtil.toGsonList(parmsMap.get("SquidIds") + "",
                    Integer.class);
            List<DataSquidCollectionPropertyId> lists =
                    new ArrayList<DataSquidCollectionPropertyId>();
            if (squidIds != null && squidIds.size() > 0) {
                adapter3 = DataAdapterFactory.getDefaultDataManager();
                adapter3.openSession();
                ISquidDao squidDao = new SquidDaoImpl(adapter3);
                for (Integer id : squidIds) {
                    DataSquid HbasrSquid = squidDao.getSquidForCond(id, HBaseExtractSquid.class);
                    DataSquid tempSquid = squidDao.getSquidForCond(id, DataSquid.class);
                    DataSquid KafkaSquid = squidDao.getSquidForCond(id, KafkaExtractSquid.class);
                    if (KafkaSquid != null) {
                        KafkaSquid.setProcess_mode(0);
                        KafkaSquid.setIs_persisted(false);
                        KafkaSquid.setTruncate_existing_data_flag(0);
                        KafkaSquid.setCdc(0);
                        KafkaSquid.setIs_indexed(false);
                        squidDao.update(KafkaSquid);
                        DataSquidCollectionPropertyId data = new DataSquidCollectionPropertyId();
                        data.setSquidId(id);
                        lists.add(data);
                    }
                    if (HbasrSquid != null) {
                        HbasrSquid.setProcess_mode(0);
                        HbasrSquid.setIs_persisted(false);
                        HbasrSquid.setTruncate_existing_data_flag(0);
                        HbasrSquid.setCdc(0);
                        HbasrSquid.setIs_indexed(false);
                        squidDao.update(HbasrSquid);
                        DataSquidCollectionPropertyId data = new DataSquidCollectionPropertyId();
                        data.setSquidId(id);
                        lists.add(data);
                    }
                    if (tempSquid != null) {
                        tempSquid.setProcess_mode(0);
                        tempSquid.setIs_persisted(false);
                        tempSquid.setTruncate_existing_data_flag(0);
                        tempSquid.setCdc(0);
                        tempSquid.setIs_indexed(false);
                        Map<String, Object> map = this.delectCdcAndColumn(adapter3, tempSquid, out);
                        List<DataSquidCollectionPropertyId> ll = (ArrayList<DataSquidCollectionPropertyId>) map.get("delSquidPropertys");
                        lists.addAll(ll);
                    } else {
                        DocExtractSquid docSquid = squidDao.getSquidForCond(id, DocExtractSquid.class);
                        if (docSquid != null) {
                            docSquid.setIs_persisted(false);
                            docSquid.setTruncate_existing_data_flag(0);
                            docSquid.setCdc(0);
                            docSquid.setIs_indexed(false);
                            squidDao.update(docSquid);
                            DataSquidCollectionPropertyId data = new DataSquidCollectionPropertyId();
                            data.setSquidId(id);
                            lists.add(data);
                        }
                    }
                }
                outputMap.put("CancelSquidInfo", lists);
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            try {
                if (adapter3 != null) {
                    adapter3.rollback();
                    logger.error("事物回滚");
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            logger.error("[获取cancelSquidsPersistence===========================exception]", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            if (adapter3 != null) adapter3.closeSession();
        }
        return outputMap;
    }

    /**
     * 创建SourceTable
     * 2014-12-1
     *
     * @param info
     * @param out
     * @return
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public Map<String, Object> createSourceTable(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
        SourceTable st = JsonUtil.object2HashMap(info, SourceTable.class);
        int executeResult;
        boolean updateFlag = false;
        int sourceId = st.getSource_squid_id();
        List<SourceTable> listSourceTable = new ArrayList<SourceTable>();
        listSourceTable.add(st);
        try {
            executeResult = connectProcess.createDBSourceTable(listSourceTable, sourceId);
            if (executeResult < 0) {
                out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
            } else {
                List<DBSourceTable> dbSourceTables = connectProcess.getDBSourceTable(sourceId, out);
                this.setSourceTable(listSourceTable, dbSourceTables);
                outputMap.put("SourceTables", listSourceTable);
            }
            outputMap.put("newSourceTableId", st.getId());
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("connectWebserviceSquid is error", e);
            out.setMessageCode(MessageCode.ERR_FILE);
        }
        return outputMap;
    }

    /**
     * 更新SourceTable
     * 2014-12-1
     *
     * @param info
     * @param out
     * @return
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public Map<String, Object> updateSourceTable(String info, ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
        SourceTable st = JsonUtil.object2HashMap(info, SourceTable.class);
        int executeResult;
        int sourceId = st.getSource_squid_id();
        List<SourceTable> listSourceTable = new ArrayList<SourceTable>();
        listSourceTable.add(st);
        try {
            executeResult = connectProcess.modifyDBSourceTable(listSourceTable, sourceId);
            if (executeResult < 0) {
                out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
            } else {
                List<DBSourceTable> dbSourceTables = connectProcess.getDBSourceTable(sourceId, out);
                this.setSourceTable(listSourceTable, dbSourceTables);
                outputMap.put("SourceTables", listSourceTable);
            }

        } catch (Exception e) {
            // TODO: handle exception
            out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
            logger.error("connectWebserviceSquid is error", e);
            out.setMessageCode(MessageCode.ERR_FILE);
        }
        return outputMap;
    }

    /**
     * 获取WebService方法列表
     * 2014-11-28
     *
     * @param info
     * @param out
     * @return
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public Map<String, Object> connectWebserviceSquid(String info,
                                                      ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        ConnectProcess connectProcess = new ConnectProcess(TokenUtil.getToken());
        WebserviceSquid ws = JsonUtil.object2HashMap(info, WebserviceSquid.class);
        int executeResult;
        boolean updateFlag = false;
        try {
            //获取WebServiceFunction
            String wsAddress = ws.getAddress();
            List<WebServiceFunction> listWebServiceFunction = null;
            if (ws.getIs_restful()) {
                listWebServiceFunction = WebServiceUtil.getWebServiceFunctionByUrl(wsAddress, WebServiceUtil.XML_TYPE_WADL);
            } else {
                listWebServiceFunction = WebServiceUtil.getWebServiceFunctionByUrl(wsAddress, WebServiceUtil.XML_TYPE_WSDL);
            }
            int sourceId = ws.getId();
            List<SourceTable> sourceTables = WebServiceUtil.converWebServiceFunction2Source(listWebServiceFunction);

            executeResult = connectProcess.connectionDBSourceTable(sourceTables, sourceId);
            if (executeResult < 0) {
                out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
            } else {
                //存储DS_FILEFOLDER_CONNECTION
                updateFlag = connectProcess.updateWebService(ws, out);
                if (!updateFlag) {
                    out.setMessageCode(MessageCode.ERR_DS_SOURCE_TABLE);
                } else {

                    List<DBSourceTable> dbSourceTables = connectProcess.getDBSourceTable(sourceId, out);//findDbSourceTables
                    this.setSourceTable(sourceTables, dbSourceTables);
                    if (!ws.getIs_restful()) {//只有wsdl需要创建sourceColumn
                        createSourceColumn(sourceTables, out, sourceId);//createColumn
                    }
                    outputMap.put("SourceTables", sourceTables);
                }
            }

        } catch (Exception e) {

            // TODO: handle exception
            logger.error("connectWebserviceSquid is error", e);
            out.setMessageCode(MessageCode.ERR_FILE);
        }
        return outputMap;
    }

    /**
     * 创建字段(column)
     * 2014-12-5
     *
     * @return
     * @throws SQLException
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    private boolean createSourceColumn(List<SourceTable> sourceTableList, ReturnValue out, Integer source_squid_id) {
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        adapterFactory = DataAdapterFactory.newInstance();
//		SourceTableDaoImpl stdi = new SourceTableDaoImpl(adapter);
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            TransformationService ts = new TransformationService(TokenUtil.getToken());
            adapter.openSession();
            for (SourceTable sourceTable : sourceTableList) {
                if (sourceTable.getReturnParam() != null) {
                    //先进行删除
                    Map<String, String> params = new HashMap<String, String>();
                    params.put("source_table_id", sourceTable.getId() + "");
                    adapter.delete(params, SourceColumn.class);
                    for (Entry<String, String> entry : sourceTable.getReturnParam().entrySet()) {
                        SystemDatatype type = SystemDatatype.NVARCHAR;
                        int length = -1;
                        int precision = 0;
                        if (entry.getValue() != null && entry.getValue().equals("string")) {

                        } else if (entry.getValue() != null && entry.getValue().equals("int")) {
                            length = 0;
                            type = SystemDatatype.INT;
                        } else if (entry.getValue() != null && entry.getValue().equals("long")) {
                            length = 0;
                            type = SystemDatatype.BIGINT;
                        } else if (entry.getValue() != null && entry.getValue().equals("float")) {
                            length = 0;
                            precision = 4;
                            type = SystemDatatype.FLOAT;
                        } else if (entry.getValue() != null && entry.getValue().equals("double")) {
                            length = 0;
                            precision = 8;
                            type = SystemDatatype.FLOAT;
                        }
                        SourceColumn sc = ts.initSourceColumn(type, length, precision, entry.getKey(), true, false, false);
                        sc.setSource_table_id(sourceTable.getId());
                        adapter.insert2(sc);
                    }
//					stdi.getSourceTableBySquidId(source_squid_id);
                }
            }
            return true;
        } catch (SQLException e) {
            // TODO: handle exception
            logger.error("createHttpSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return false;
    }

    /**
     * 创建httpSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> createHttpSquid(String info,
                                               ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int httpSquidId = 0;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            HttpSquid httpSquid = JsonUtil
                    .object2HashMap(info, HttpSquid.class);
            if (null != httpSquid) {
                httpSquidId = adapter.insert2(httpSquid);
                if (httpSquidId > 0)// 插入成功
                {
                    outputMap.put("newSquidId", httpSquidId);
                } else {
                    out.setMessageCode(MessageCode.INSERT_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(httpSquidId, httpSquidId, httpSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createHttpSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 更新httpSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> updateHttpSquid(String info,
                                               ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            HttpSquid httpSquid = JsonUtil
                    .object2HashMap(info, HttpSquid.class);
            if (null != httpSquid) {
                updateFlag = adapter.update2(httpSquid);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateHttpSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 2014-12-4
     *
     * @param info
     * @return
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public Map<String, Object> updateThirdPartyParams(String info) {

        return null;
    }

    /**
     * 创建WebserviceSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> createWebserviceSquid(String info,
                                                     ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        int webserviceSquidId = 0;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            WebserviceSquid webserviceSquid = JsonUtil
                    .object2HashMap(info, WebserviceSquid.class);
            if (null != webserviceSquid) {
                webserviceSquidId = adapter.insert2(webserviceSquid);
                if (webserviceSquidId > 0)// 插入成功
                {
                    outputMap.put("newSquidId", webserviceSquidId);
                } else {
                    out.setMessageCode(MessageCode.INSERT_ERROR);
                }
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(webserviceSquidId, webserviceSquidId, webserviceSquid.getName(), MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createWebserviceSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }


    /**
     * 更新httpSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> updateWebserviceSquid(String info,
                                                     ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        boolean updateFlag = false;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            WebserviceSquid webserviceSquid = JsonUtil
                    .object2HashMap(info, WebserviceSquid.class);
            if (null != webserviceSquid) {
                updateFlag = adapter.update2(webserviceSquid);
                if (!updateFlag)// 更新失败
                {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateWebserviceSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 更新httpSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author lei.bin
     */
    public Map<String, Object> updateThirdPartyParams(String info,
                                                      ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            // 实例化相关的数据库处理类
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            List<ThirdPartyParams> listThirdPartyParams = JsonUtil.toGsonList(JsonUtil.toHashMap(info).get("DataList").toString(), ThirdPartyParams.class);
            if (null != listThirdPartyParams && listThirdPartyParams.size() > 0) {
                boolean b = false;
                for (ThirdPartyParams thirdPartyParams : listThirdPartyParams) {
                    boolean d = adapter.update2(thirdPartyParams);
                    if (d) {
                        b = true;
                    }
                }
                if (!b) {//更新是失败
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateWebserviceSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }


    /**
     * 创建DestWsSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author yi.zhou
     */
    public Map<String, Object> createDestWsSquid(String info,
                                                 ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DestWSSquid squid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("DestWSSquid")), DestWSSquid.class);
            if (null != squid) {
                int newId = adapter.insert2(squid);
                outputMap.put("newSquidId", newId);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createDestWsSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 修改DestWsSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author yi.zhou
     */
    public Map<String, Object> updateDestWsSquid(String info,
                                                 ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            DestWSSquid squid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("DestWSSquid")), DestWSSquid.class);
            if (null != squid) {
                adapter.update2(squid);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateDestWsSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 创建AnnotationSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author yi.zhou
     */
    public Map<String, Object> createAnnotationSquid(String info,
                                                     ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            AnnotationSquid squid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("AnnotationSquid")), AnnotationSquid.class);
            if (null != squid) {
                int newId = adapter.insert2(squid);
                outputMap.put("newSquidId", newId);
            } else {
                out.setMessageCode(MessageCode.INSERT_ERROR);
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createAnnotationSquid is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 修改DestWsSquid
     *
     * @param info
     * @param out
     * @return outputMap
     * @author yi.zhou
     */
    public Map<String, Object> updateAnnotationSquid(String info,
                                                     ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        DataAdapterFactory adapterFactory = null;
        IRelationalDataManager adapter = null;
        try {
            adapterFactory = DataAdapterFactory.newInstance();
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            AnnotationSquid squid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("AnnotationSquid")), AnnotationSquid.class);
            if (null != squid) {
                adapter.update2(squid);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateAnnotationSquid is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 创建createNOSQLConnetion
     *
     * @param info
     * @param out
     * @return outputMap
     * @author yi.zhou
     */
    public Map<String, Object> createNOSQLConnection(String info,
                                                     ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            NOSQLConnectionSquid squid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("MangoDBConnectionSquid")), NOSQLConnectionSquid.class);
            if (null != squid) {
                int newId = adapter.insert2(squid);
                outputMap.put("newSquidId", newId);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createNOSQLConnection is error", e);
            out.setMessageCode(MessageCode.INSERT_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 修改updateNOSQLConnection
     *
     * @param info
     * @param out
     * @return outputMap
     * @author yi.zhou
     */
    public Map<String, Object> updateNOSQLConnection(String info,
                                                     ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            NOSQLConnectionSquid squid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("UpdateMangoDBConnectionSquid")), NOSQLConnectionSquid.class);
            if (null != squid) {
                adapter.update2(squid);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateNOSQLConnection is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * connectNOSQLConnetion
     *
     * @param info
     * @param out
     * @return outputMap
     * @author yi.zhou
     */
    public Map<String, Object> connectNOSQLConnection(String info,
                                                      ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        List<SourceTable> lists = new ArrayList<SourceTable>();
        IRelationalDataManager adapter = null;
        List<Integer> cancelSquidIds=new ArrayList<>();
        int updateSquidStatus=0;
        List<Object> paramsObject = new ArrayList<>(); // 查询参数
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            NOSQLConnectionSquid nosql = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("MangoDBConnectionSquid")), NOSQLConnectionSquid.class);
            String sql=" select k.TO_SQUID_ID as TOSQUID from ds_squid_link k where k.FROM_SQUID_ID =?";
            paramsObject.clear();
            paramsObject.add(nosql.getId());
            //查询下游所有的抽取squid
            List<Map<String, Object>> squidIds=adapter.query2List(true,sql,paramsObject);
            if(squidIds!=null && squidIds.size()>0){
                for(Map<String,Object> map:squidIds){
                    cancelSquidIds.add(Integer.parseInt(map.get("TOSQUID").toString()));
                }
            }
            //修改这squid状态。
            if(cancelSquidIds!=null && cancelSquidIds.size()>0){
                List<Object> updateSquidIds=new ArrayList<>();
                String updateSql="update ds_squid  set DESIGN_STATUS=1 where id in ";
                updateSquidIds.addAll(cancelSquidIds);
                String ids = JsonUtil.toGsonString(updateSquidIds);
                ids=ids.substring(1,ids.length()-1);
                updateSql+="("+ids+")";
                adapter.execute(updateSql);
            }
            outputMap.put("ErrorSquidIdList", cancelSquidIds);


            SquidTypeEnum stEnum = SquidTypeEnum.parse(nosql.getSquid_type());
            switch (stEnum) {
                case MONGODB:
                    DB mongoDB = NoSqlConnectionUtil.createMongoDBConnection(nosql);

                    Set<String> origColls = mongoDB.getCollectionNames();
                    if (origColls.size() == 0) {
                        out.setMessageCode(MessageCode.ERR_CONNECTION_FAIL);
                    }
                    Map<String, String> params = new HashMap<String, String>();
                    params.put("source_squid_id", nosql.getId() + "");
                    params.put("is_extracted", "0");
                    adapter.delete(params, DBSourceTable.class);

                    /**
                     * 遍历集合，实现筛选面板的多条件过滤和模糊查找
                     */

					/* 过滤没有找到官方的函数，暂时先做首字母匹配*/
                    List<String> collectionNames = new ArrayList<>();
                    if (StringUtils.isNotEmpty(nosql.getFilter())) {
                        //多条件过滤
                        if (nosql.getFilter().split(",").length > 1) {
                            for (String collectionName : origColls) {
                                for (String filter : nosql.getFilter().split(",")) {
                                    filter = filter.replaceAll("\\*", ".*").replaceAll("\\?", ".").replaceAll("%", ".*");
                                    if (collectionName.matches(filter))
                                        collectionNames.add(collectionName);
                                }

                            }
                        } else {
                            for (String collectionName : origColls) {
                                String filter = nosql.getFilter();
                                filter = filter.replaceAll("\\*", ".*").replaceAll("\\?", ".").replaceAll("%", ".*");
                                if (collectionName.matches(filter))
                                    collectionNames.add(collectionName);
                            }
                        }

                    } else {
                        for (String collectionName : origColls)
                            collectionNames.add(collectionName);
                    }


                    params.clear();
                    params.put("source_squid_id", nosql.getId() + "");
                    List<DBSourceTable> formerColls = adapter.query2List(true, params, DBSourceTable.class);
                    //设置返回的sourceList集合
                    List<DBSourceTable> sourceTableList = new ArrayList<>();
                    if (collectionNames.size() > 0) {
                        for (String s : collectionNames) {
                            if (s.equals("system.indexes")) {
                                continue;
                            }
                            boolean b = true;
                            if (formerColls != null && formerColls.size() > 0) {
                                for (DBSourceTable dbSourceTable : formerColls) {
                                    if (s.equals(dbSourceTable.getTable_name())) {
                                        formerColls.remove(dbSourceTable);
                                        sourceTableList.add(dbSourceTable);
                                        b = false;
                                        break;
                                    }
                                }
                            }

                            if (b) {
                                DBSourceTable dbSt = new DBSourceTable();
                                dbSt.setTable_name(s);
                                dbSt.setSource_squid_id(nosql.getId());
                                dbSt.setIs_extracted(false);
                                dbSt.setId(adapter.insert2(dbSt));
                                sourceTableList.add(dbSt);
                            }

                        }
                    }
                    //结束后重新生成返回集合
                    List<DBSourceTable> oldformerColls = sourceTableList;
                    //当获取到的表名不为空时
                    if (oldformerColls != null && oldformerColls.size() > 0 && origColls.size() > 0) {
                        for (DBSourceTable dbSourceTable : oldformerColls) {
                            SourceTable st = new SourceTable();
                            st.setTableName(dbSourceTable.getTable_name());
                            st.setSource_squid_id(dbSourceTable.getSource_squid_id());
                            st.setIs_extracted(dbSourceTable.isIs_extracted());
                            st.setId(dbSourceTable.getId());
                            lists.add(st);
                        }
                    }
                    break;
                default:
                    break;
            }
            outputMap.put("SourceTable", lists);
            return outputMap;
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("connectNOSQLConnection is error", e);
            out.setMessageCode(MessageCode.ERR_DBSOURCE_CONNECTION);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * getNoSQLPreviewData
     *
     * @param info
     * @param out
     * @return outputMap
     * @author yi.zhou
     */
    public Map<String, Object> getNoSQLPreviewData(String info,
                                                   ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            int sourceTableId = Integer.parseInt(paramsMap.get("SourceTableId").toString());
            //根据sourceTableId查询对象
            ISourceTableDao sourceTableDao = new SourceTableDaoImpl(adapter);
            DBSourceTable dbSourceTable = sourceTableDao.getObjectById(sourceTableId, DBSourceTable.class);
            if (dbSourceTable != null) {
                //根据Source_squid_id()查询ds_sql_connection对象
                ISquidDao squidDao = new SquidDaoImpl(adapter);
                NOSQLConnectionSquid dbSourceSquid = squidDao.getSquidForCond(dbSourceTable.getSource_squid_id(), NOSQLConnectionSquid.class);
                Mongo m;
                if (dbSourceSquid.getHost().contains(":")) {
                    String host = dbSourceSquid.getHost().split(":")[0];
                    int port = Integer.parseInt(dbSourceSquid.getHost().split(":")[1]);
                    m = new Mongo(host, port);
                } else {
                    m = new Mongo(dbSourceSquid.getHost(), dbSourceSquid.getPort());
                }
                DB d = m.getDB(dbSourceSquid.getDb_name());
                DBCollection coll = d.getCollection(dbSourceTable.getTable_name());
                DBObject query = new BasicDBObject();
                DBCursor cur = coll.find(query);
                outputMap.put("PreviewData", ZipStrUtil.compress(cur.toArray().toString().getBytes()));
                return outputMap;
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("getNoSQLPreviewData is error", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }


    /**
     * getNoSQLPreviewData
     *
     * @param info
     * @param out
     * @return outputMap
     * @author yi.zhou
     */
    public void createMongodbExtracts(String info,
                                      ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter = null;
        try {
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> map = JsonUtil.toHashMap(info);
            List<Integer> sourceTableIds = JsonUtil.toGsonList(map.get("SourceTableIds") + "", Integer.class);
            Integer squidFlowId = Integer.parseInt(map.get("SquidFlowId").toString());
            Integer connectionSquidId = Integer.parseInt(map.get("ConnectionSquidId").toString());
            Integer x = Integer.parseInt(map.get("X").toString());
            Integer y = Integer.parseInt(map.get("Y").toString());
            //根据sourceTableIds查询对象
            List<ExtractSquidAndSquidLink> packet = createExtractSquidAndSquidLink(adapter, sourceTableIds, connectionSquidId, squidFlowId, x, y);
            int i = 0;
            boolean b = false;
            List<Squid> listSquid = new ArrayList<Squid>();
            List<SquidLink> listSquidLink = new ArrayList<SquidLink>();
            for (ExtractSquidAndSquidLink esaql : packet) {
                if (listSquid.size() > 0 && listSquid.size() % 20 == 0) {
                    outputMap.put("Squids", listSquid);
                    outputMap.put("SquidLinks", listSquidLink);
                    outputMap.put("SuccessCount", i);
                    PushMessagePacket.pushMap(outputMap, DSObjectType.MONGODBEXTRACT, "1000", "1304", key, token, MessageCode.SUCCESS.value());
                    listSquidLink = new ArrayList<SquidLink>();
                    listSquid = new ArrayList<Squid>();
                }
                /*ExtractServicesub subService = new ExtractServicesub(token, adapter);
				subService.drag2ExtractSquid(esaql, out);
				if(out.getMessageCode()!=null&&out.getMessageCode().equals(MessageCode.SQL_ERROR)){
					b=true;
					continue;
				}*/
                listSquid.add(esaql.getExtractSquid());
                listSquidLink.add(esaql.getSquidLink());
                i++;
            }
            if (listSquid.size() > 0) {
                outputMap.put("Squids", listSquid);
                outputMap.put("SquidLinks", listSquidLink);
                outputMap.put("SuccessCount", i);
                PushMessagePacket.pushMap(outputMap, DSObjectType.MONGODBEXTRACT, "1000", "1304", key, token, MessageCode.SUCCESS.value());
            }
            if (b) {
                Map<String, Object> param = new HashMap<String, Object>();
                param.put("SuccessCount", i);
                param.put("Squids", new ArrayList<Squid>());
                param.put("SquidLinks", new ArrayList<SquidLink>());
                PushMessagePacket.pushMap(param, DSObjectType.MONGODBEXTRACT, "1000", "1304", key, token, MessageCode.COMPLETE.value());
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("createMongodbExtracts is error", e);
            out.setMessageCode(MessageCode.SQL_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
    }

    /**
     * 修改updateMongodbExtract
     *
     * @param info
     * @param out
     * @return outputMap
     * @author yi.zhou
     */
    public Map<String, Object> updateMongodbExtract(String info,
                                                    ReturnValue out) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        IRelationalDataManager adapter = null;
        try {
            System.out.println(info);
            adapter = DataAdapterFactory.getDefaultDataManager();
            adapter.openSession();
            Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
            MongodbExtractSquid squid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("UpdateMangoDBExtractSquid")), MongodbExtractSquid.class);
            if (null != squid) {
                adapter.update2(squid);
                outputMap.put("UpdateMangoDBExtractSquid", squid);
            } else {
                out.setMessageCode(MessageCode.NODATA);
            }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("updateMongodbExtract is error", e);
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return outputMap;
    }

    /**
     * 生成extractSquid和link集合
     *
     * @param adapter
     * @param sourceTableIds
     * @param connectionSquidId
     * @param squidFlowId
     * @param x
     * @param y
     * @return
     */
    private List<ExtractSquidAndSquidLink> createExtractSquidAndSquidLink(IRelationalDataManager adapter, List<Integer> sourceTableIds, Integer connectionSquidId, Integer squidFlowId, Integer x, Integer y) {
        Map<String, String> params = new HashMap<String, String>();
        List<ExtractSquidAndSquidLink> listExtractSquidAndSquidLink = new ArrayList<ExtractSquidAndSquidLink>();
        if (x == null) {
            x = 100;
        }
        if (y == null) {
            y = 100;
        }
        boolean fob = false;
        for (Integer sourceTableId : sourceTableIds) {
            ExtractSquidAndSquidLink esasl = new ExtractSquidAndSquidLink();
            TableExtractSquid tes = new TableExtractSquid();
            esasl.setExtractSquid(tes);
            params.clear();
            params.put("id", sourceTableId + "");
            DBSourceTable st = adapter.query2Object2(true, params, DBSourceTable.class);
            tes.setSquidflow_id(squidFlowId);
            tes.setSource_table_id(sourceTableId);//ID
            tes.setTable_name(st.getTable_name());
            tes.setName(squidService.getSquidName(adapter, st.getTable_name(), squidFlowId));//NAME
            tes.setSquid_type(SquidTypeEnum.MONGODBEXTRACT.value());
            tes.setKey(StringUtils.generateGUID());
            if (fob) {
                y += 70;
            } else {
                fob = true;
            }
            tes.setLocation_x(x);
            tes.setLocation_y(y);
            try {
                tes.setId(adapter.insert2(tes));
                SquidLink sl = new SquidLink();
                esasl.setSquidLink(sl);
                sl.setFrom_squid_id(connectionSquidId);
                sl.setSquid_flow_id(squidFlowId);
                sl.setTo_squid_id(tes.getId());
                sl.setKey(StringUtils.generateGUID());
                sl.setId(adapter.insert2(sl));
                listExtractSquidAndSquidLink.add(esasl);

                if (!st.isIs_extracted()) {
                    st.setIs_extracted(true);
                    adapter.update2(st);
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                break;
            }
        }
        return listExtractSquidAndSquidLink;
    }

    public static void main(String[] args) {
    	/*try {
    		SquidProcess process = new SquidProcess();
        	DBConnectionInfo dbinfo = process.getHbaseConnection();
    		HbaseAdapter adapter = new HbaseAdapter(dbinfo);
    		String tableName = "csv1";
    		List<Map<String, Object>> list = adapter.queryForList(new SelectSQL(tableName));
    		System.out.println(list);
		} catch (Exception e) {
			e.printStackTrace();
		}*/
        try {
            String info = "{SquidIds:[111,112],DestSquidId:20}";
            SquidProcess process = new SquidProcess(null);
            process.setSquidsDest(info, new ReturnValue());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void ss() throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection conn = DriverManager.getConnection("jdbc:phoenix:e231:2181");
        ResultSet rs = conn.createStatement().executeQuery("select * from DS_SYS.SF_LOG");
        int cols = rs.getMetaData().getColumnCount();
        for (int i = 0; i < cols; i++) {
            System.out.print(rs.getMetaData().getColumnName(i + 1) + "\t");
        }
        System.out.println();
        while (rs.next()) {
            for (int i = 0; i < cols; i++) {
                System.out.print(rs.getString(i + 1) + "\t");
            }
            System.out.println();
        }
    }

    public static void tt() {
        String info = "{\"UpdateMangoDBExtractSquid\":{\"Columns\":[],\"SourceColumns\":[],\"Is_indexed\":true,\"Is_persisted\":true,\"destination_squid_id\":94,\"top_n\":0,\"truncate_existing_data_flag\":1,\"process_mode\":0,\"cdc\":0,\"encoding\":0,\"Transformations\":[],\"TransformationLinks\":[],\"Table_name\":\"test2_cc\",\"Source_table_id\":567,\"Description\":\"12\",\"Location_x\":340,\"Location_y\":64,\"Squid_height\":70.0,\"Squid_width\":70.0,\"Squid_type\":39,\"Squidflow_id\":3,\"Filter\":\"{\\\"event_ts\\\":{$ne:null},\\\"causedby\\\":\\\"user\\\",\\\"mode\\\":\\\"wakeup\\\",\\\"status.0\\\":\\\"on\\\"}\",\"Design_status\":0,\"Variables\":[],\"Id\":92,\"Key\":\"A2AC8FF4-324F-BEB3-ED9A-CC0C718886EB\",\"Name\":\"e_test2\"}}";
        JSONObject firstObj = JSONObject.parseObject(info);
//    	JSONObject childObj = firstObj.getJSONObject("UpdateMangoDBExtractSquid");
//		MongodbExtractSquid squid  = childObj.parseObject(childObj.toString(), MongodbExtractSquid.class);
        Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
        MongodbExtractSquid squid = JsonUtil.toGsonBean(String.valueOf(paramsMap.get("UpdateMangoDBExtractSquid")), MongodbExtractSquid.class);
        System.out.println(squid);
    }

    /**
     * 对json字符串中特殊的字符进行转义
     *
     * @param data
     * @return
     */
    public static String transJson(String data) {
        if (StringUtils.isEmpty(data)) {
            return data;
        }
        data.replaceAll("\\\\", "\\");
        return data;
    }
}
