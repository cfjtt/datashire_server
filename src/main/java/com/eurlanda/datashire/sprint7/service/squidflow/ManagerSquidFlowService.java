package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ManagerSquidFlowService
 * 主要处理SquidFlow调度的数据进行相应业务处理调用相对应的process预处理类
 * Description:
 * Author :周益 2014-5-19
 * update :周益 2014-5-19
 * Department :  JAVA后端研发部
 * Copyright : ©2013-2014 悦岚（上海）数据服务有限公司
 */
@Service
public class ManagerSquidFlowService {
    private static Logger logger = Logger.getLogger(ManagerSquidFlowService.class);// 记录日志
    //异常处理机制
    ReturnValue out = new ReturnValue();
    private String token;//令牌根据令牌得到相应的连接信息
    private String key;//key值

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

    /**
     * 启动流式计算SquidFlow
     *
     * @param info 客户端参数
     * @return 返回客户端的数据
     */
    public String RunFlowCalculationSquidFlow(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        InfoNewMessagePacket map = process.RunFlowCalculationSquidFlow(info);
        return map.toJsonString();
    }

    public String StopFlowCalculationSquidFlow(String info) {
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        InfoNewMessagePacket map = process.StopFlowCalculationSquidFlow(info);
        return map.toJsonString();
    }

    public String GetApplicationStatus(String info) {
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        InfoNewMessagePacket map = process.GetApplicationStatus(info);
        return map.toJsonString();
    }

    public String deleteApplicationStatus(String info) {
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        InfoNewMessagePacket map = process.deleteApplicationStatus(info);
        return map.toJsonString();
    }

    /**
     * 忽略目的地squid标记，应用断点，运行squidFlow的所有squid
     *
     * @param info
     * @return
     */
    public String runALLWithDebugging(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.runALLWithDebugging(info, out);
        return infoNewMessagePacket(map, DSObjectType.RUNSQUIDFLOW, out);
    }


    /**
     * 元数据检查
     *
     * @param info
     * @return
     */
    public String checkMetaDataOfSquidFlow(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.metaDataCheck(info, out);
        /*if (map==null&&out.getMessageCode().equals(MessageCode.SUCCESS)){
			out.setMessageCode(MessageCode.ERR);
		}*/
        System.out.println(map.toString());
        InfoNewMessagePacket infoPacket = new InfoNewMessagePacket(map, out.getMessageCode().value());
        System.out.println(infoPacket.toJsonString());
        return infoPacket.toJsonString();
        //	return  infoNewMessagePacket(map, DSObjectType.RUNSQUIDFLOW, out);
    }


    /**
     * 跳过断点，进行执行squidFlow
     *
     * @param info
     * @return
     */
    public String resumeEngine(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.resumeEngine(info, out);
        return infoNewMessagePacket(map, DSObjectType.RUNSQUIDFLOW, out);
    }

    /**
     * 跳过 debugging, 继续流转
     *
     * @param info
     * @return
     */
    public String stopDebug(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.stopDebug(info, out);
        return infoNewMessagePacket(map, DSObjectType.RUNSQUIDFLOW, out);
    }

    /**
     * 复制单个
     *
     * @param info
     * @return
     */
    public String copySquidFlowForParams(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.copySquidFlowForParams(info, out, 0);
        return infoNewMessagePacket(map, DSObjectType.SQUID, out);
    }

    /**
     * 复制整个squidFlow
     *
     * @param info
     * @return
     */
    public String copySquidFlowForSquidFlow(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.copySquidFlowForSquidFlow(info, out);
        return infoNewMessagePacket(map, DSObjectType.STAGE, out);
    }

    /**
     * 创建落地数据表后的index
     *
     * @param info
     * @return
     */
    public String createIndexBySquidId(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.createIndexBySquidId(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUIDINDEXS, out);
    }

    /**
     * 删除落地数据表后的index
     *
     * @param info
     * @return
     */
    public String dropIndexBySquidId(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.dropIndexBySquidId(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUIDINDEXS, out);
    }

    /**
     * 创建落地数据表（支持多个Squid）
     *
     * @param info
     * @return
     */
    public String createPersistTableByIds(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.createPersistTableByIds(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUID, out);
    }

    /**
     * 删除落地数据表
     *
     * @param info
     * @return
     */
    public String dropPersistTableByIds(String info) {
        out = new ReturnValue();
        long current = System.currentTimeMillis();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.dropPersistTableByIds(info, out);
        logger.info("===========================\n\t===========================\n"
                + "\t===========================\n"
                + "\t===========================\n");
        logger.info("===========执行删除表操作耗时：" + (System.currentTimeMillis() - current) / 1000.0 + "(秒)");
        return infoNewMessagePacket(map, DSObjectType.SQUID, out);
    }

    /**
     * 根据源数据类型column获取转换后的系统类型column
     *
     * @param info
     * @return
     */
    public String getConvertedColumns(String info) {
        out = new ReturnValue();
        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.getConvertedColumns(info, out);
        return infoNewMessagePacket(map, DSObjectType.COLUMN, out);
    }

    /**
     * 单对象转换成Json格式
     * 作用描述：
     * 修改说明：
     *
     * @param <T>
     * @return *@deprecated 请参考 JsonUtil.toString(...)
     */
    private <T> String infoNewMessagePacket(T object, DSObjectType type, ReturnValue out) {
        return JsonUtil.toJsonString(object, type, out.getMessageCode());
    }

    /**
     * 同步检查
     *
     * @param info
     * @return
     * @author bo.dang
     */
    public String getSquidsSyncStatus(String info) {
//        out = new ReturnValue();
//        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
//        List<Map<Integer, Integer>> resultList = process.checkSquidsSyncStatus(info, out);
//        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
//        Map<String, Object> resultMap = new HashMap<String, Object>();
//        resultMap.put("SyncStatus", resultList);
//        infoMessage.setCode(out.getMessageCode().value());
//        infoMessage.setInfo(resultMap);
//        infoMessage.setDesc("同步检查");
//        return JsonUtil.object2Json(infoMessage);
        return "";
    }


    /**
     * 同步
     *
     * @param info
     * @return
     * @author bo.dang
     */
    public String synchronizeSquids(String info) {
//        out = new ReturnValue();
//        ManagerSquidFlowProcess process = new ManagerSquidFlowProcess(TokenUtil.getToken(), TokenUtil.getKey());
//        List<Map<Integer, Boolean>> resultList = process.synchronizeSquids(info, out);
//        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
//        Map<String, Object> resultMap = new HashMap<String, Object>();
//        resultMap.put("SyncStatus", resultList);
//        infoMessage.setCode(out.getMessageCode().value());
//        infoMessage.setInfo(resultMap);
//        infoMessage.setDesc("同步");
//        return JsonUtil.object2Json(infoMessage);
        return "";
    }
}
