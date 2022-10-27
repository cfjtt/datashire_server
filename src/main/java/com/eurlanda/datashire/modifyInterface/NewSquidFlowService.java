package com.eurlanda.datashire.modifyInterface;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidFlowDao;
import com.eurlanda.datashire.dao.ISquidFlowStatusDao;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.dao.IVariableDao;
import com.eurlanda.datashire.dao.impl.SquidFlowDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidFlowStatusDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidLinkDaoImpl;
import com.eurlanda.datashire.dao.impl.VariableDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.LockSquidFlowProcess;
import com.eurlanda.datashire.sprint7.service.squidflow.ProjectProcess;
import com.eurlanda.datashire.sprint7.service.squidflow.SquidFlowProcess;
import com.eurlanda.datashire.utility.*;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 新的squidflow操作接口
 * Created by Administrator on 2016/12/12.
 */
@Service
public class NewSquidFlowService {
    private static Logger logger = Logger.getLogger(NewSquidFlowService.class);// 记录日志
    ReturnValue out = new ReturnValue();
    private String key;
    private String token;

    /**
     * 新的squidflow打开接口(多波推送)
     *
     * @param info
     */
    public void openSquidFlow(String info) {
        out = new ReturnValue();
        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
        System.out.println("--------------------------" + info + "-----------------------");
        Map<String, Object> map = JsonUtil.toHashMap(info);
        int squidFlowId = Integer.parseInt(map.get("SquidFlowId").toString());
        int project_id = Integer.parseInt(map.get("ProjectId").toString());
        int repository_id = Integer.parseInt(map.get("RepositoryId").toString());
        int status = Integer.parseInt(map.get("Status") + "");
        //增加定时器，防止超时
        Timer timer = new Timer();
        try {
            if (squidFlowId > 0) {
                final Map<String, Object> returnMap = new HashMap<>();
                key = TokenUtil.getKey();
                token = TokenUtil.getToken();
                returnMap.put("1", 1);
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        PushMessagePacket.pushMap(returnMap, DSObjectType.SQUID_FLOW, "1039", "0001", key, token, MessageCode.BATCH_CODE.value());
                    }
                }, 25 * 1000, 25 * 1000);
                adapter.openSession();
                ISquidFlowDao squidFlowDao = new SquidFlowDaoImpl(adapter);
                ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter);
                IVariableDao variableDao = new VariableDaoImpl(adapter);
                ISquidFlowStatusDao squidFlowStatusDao = new SquidFlowStatusDaoImpl(adapter);
                //查询出该squidflow下面，squid的数量
                int count = squidFlowDao.getSquidCountBySquidFlow(squidFlowId);
                Map<String, Object> pushMap = new HashMap<>();
                pushMap.put("Count", count);
                //查询出squidflow的squidlink信息
                List<SquidLink> linkList = squidLinkDao.getSquidLinkListBySquidFlow(squidFlowId);
                if (linkList == null) {
                    linkList = new ArrayList<SquidLink>();
                }

                pushMap.put("SquidLinks", linkList);

                //查询出squidflow下面的变量信息
                List<DSVariable> variables = variableDao.getDSVariableByScope(1, squidFlowId);
                if (variables == null) {
                    variables = new ArrayList<DSVariable>();
                }
                pushMap.put("Variables", variables);

                //查询squidFlow的状态
                List<SquidFlowStatus> list = squidFlowStatusDao.getSquidFlowStatusBySquidFlow(repository_id, squidFlowId);
                if (StringUtils.isNotNull(list) && !list.isEmpty()) {
                    Map<String, Object> calcMap = CalcSquidFlowStatus.calcStatus(list);
                    if (Integer.parseInt(calcMap.get("checkout").toString()) > 0
                            || Integer.parseInt(calcMap.get("schedule").toString()) > 0) {
                        //表示该squidflow已经被加锁
                        out.setMessageCode(MessageCode.WARN_GETSQUIDFLOW);
                        PushMessagePacket.pushMap(pushMap, DSObjectType.SQUID_FLOW, "1039", "0001", TokenUtil.getKey(), TokenUtil.getToken());
                    }
                }
                boolean flag = false;
                if (status == 0) {
                    flag = squidFlowDao.getLockOnSquidFlow(squidFlowId, project_id, repository_id, 1, TokenUtil.getToken());
                } /*else {
                    flag = squidFlowDao.getLockOnSquidFlow(squidFlowId,project_id,repository_id,2,TokenUtil.getToken());
                }*/
                if (flag) {
                    LockSquidFlowProcess lockSquidFlowProcess = new LockSquidFlowProcess(TokenUtil.getToken());
                    //List<SquidFlowStatus> statuses = lockSquidFlowProcess.getSquidFlowStatus2(repository_id,squidFlowId,out,adapter);
                    //if(statuses!=null && statuses.size()>0){
                    if (status == 0) {
                        lockSquidFlowProcess.sendAllClient(repository_id, squidFlowId, 1);
                    }
                    //}
                }
                //推送squidflow信息
                PushMessagePacket.pushMap(pushMap, DSObjectType.SQUID_FLOW, "1039", "0001", TokenUtil.getKey(), TokenUtil.getToken());
                pushMap.clear();
                pushMap = null;

                //多波推送squidflow下面的squid信息
                NewSquidFlowServiceSub sub = new NewSquidFlowServiceSub();
                sub.newPushAllSquid(squidFlowId, repository_id, "1039", "0001", TokenUtil.getKey(), TokenUtil.getToken(), adapter);

            } else {
                out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
                logger.error("getAllSquidKey反序列化失败！" + info);
            }
        } catch (Exception e) {
            timer.purge();
            timer.cancel();
            e.printStackTrace();
            out.setMessageCode(MessageCode.SQL_ERROR);
            logger.error("getAllSquidKey异常！", e);
        } finally {
            timer.purge();
            timer.cancel();
            adapter.closeSession();
        }
    }

    /**
     * 新的打开repository方法，一次加载到squidflow
     *
     * @param info
     * @return
     */
    public String openRepository(String info) {
        out = new ReturnValue();
        Map<String, Object> map = JsonUtil.toHashMap(info);
        Map<String, Object> returnMap = new HashMap<>();
        ProjectProcess projectProcess = new ProjectProcess(TokenUtil.getToken());
        SquidFlowProcess squidFlowProcess = new SquidFlowProcess(TokenUtil.getToken());
        Map<String, Object> projectMap = projectProcess.queryAllProjects(info, out);

        //查询出所有的project
        List<Project> projectList = (List<Project>) projectMap.get("Projects");
        //查询出所有的squidflow对象
        for (Project project : projectList) {
            String squidflowInfo = "{ProjectId:" + project.getId() + ",RepositoryId:" + map.get("RepositoryId") + "}";
            Map<String, Object> squidFlowMap = squidFlowProcess.queryAllSquidFlows(squidflowInfo, out);
            List<SquidFlow> squidFlowList = (List<SquidFlow>) squidFlowMap.get("SquidFlows");
            List<DSVariable> variables = (List<DSVariable>) squidFlowMap.get("Variables");
            if (squidFlowList != null) {
                project.setSquidFlowList(squidFlowList);
            }
            if (variables != null) {
                project.setVariables(variables);
            }
        }
        returnMap.put("Projects", projectList);
        return JsonUtil.toJsonString(returnMap, DSObjectType.REPOSITORY, out.getMessageCode());
    }

}
