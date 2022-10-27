package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.GroupTaggingSquid;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by Eurlanda on 2017/3/8.
 */
@Service
public class GroupTaggingSquidService {
    private String key;
    private String token;
    private ReturnValue out;
    private Logger logger = Logger.getLogger(GroupTaggingSquidService.class);
    /**
     * 创建GroupTaggingSquid
     * @param info
     * @return
     */
    public String createGroupTaggingSquid(String info){
        out=new ReturnValue();
        Map<String, Object> map=new HashMap<String, Object>();
        IRelationalDataManager adapter= null;
        try {
            HashMap<String, Object> data=JsonUtil.toHashMap(info);
            List<Column>  columns = new ArrayList<>();
            GroupTaggingSquid tagSquid =  JsonUtil.toGsonBean(data.get("GroupTaggingSquid").toString(), GroupTaggingSquid.class);
            if(null!=tagSquid ){
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                ISquidDao squidDao=new SquidDaoImpl(adapter);
                int squidId=squidDao.insert2(tagSquid);
                map.put("GroupTaggingSquidId", squidId);
                //创建一个column
                Column column = new Column();
                column.setCollation(0);
                column.setData_type(SystemDatatype.INT.value());
                column.setLength(0);
                column.setPrecision(0);
                column.setKey(StringUtils.generateGUID());
                column.setName(ConstantsUtil.CN_GROUP_TAG);
                column.setNullable(true);
                column.setRelative_order(1);
                column.setSquid_id(squidId);
                column.setId(adapter.insert2(column));
                columns.add(column);
                map.put("Columns",columns);
                //创建transformation
                TransformationService service = new TransformationService(TokenUtil.getToken());
                service.initTransformation(adapter,squidId,column.getId(), TransformationTypeEnum.VIRTUAL.value(),column.getData_type(),1);
            } else {
                out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
            }
        } catch (BeyondSquidException e){
            try {
                if (adapter!=null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("创建GroupTagSquid异常",e );
        } catch (Exception e) {
            // TODO: handle exception
            try {
                if (adapter!=null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.INSERT_ERROR);
            logger.error("创建GroupTagSquid异常",e );
        }finally {
            if (adapter!=null) adapter.closeSession();
        }
        return JsonUtil.toJsonString(map, DSObjectType.GROUPTAGGING, out.getMessageCode());
    }

    /**
     * 修改GroupTaggingSquid
     * @param info
     * @return
     */
    public String updateGroupTaggingSquid(String info){
        out = new ReturnValue();
        IRelationalDataManager adapter= null;
        HashMap<String, Object> paramsMap = JsonUtil.toHashMap(info);
        try {
            GroupTaggingSquid taggingSquid = JsonUtil.toGsonBean(paramsMap.get("GroupTaggingSquid") + "", GroupTaggingSquid.class);
            if (null != taggingSquid) {
                adapter = DataAdapterFactory.getDefaultDataManager();
                adapter.openSession();
                ISquidDao squidDao = new SquidDaoImpl(adapter);
                boolean flag = adapter.update2(taggingSquid);
                if (!flag) {
                    out.setMessageCode(MessageCode.UPDATE_ERROR);
                }
            } else {
                out.setMessageCode(MessageCode.ERR_SQUID_NULL);
            }
        } catch (Exception e){
            logger.error(e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.UPDATE_ERROR);
            logger.error("更新GroupTagging异常", e);
        } finally {
            adapter.closeSession();
        }
        return JsonUtil.toJsonString(new HashMap<String, Object>(),DSObjectType.GROUPTAGGING,out.getMessageCode());
    }

    /**
     * 删除TaggingLink（删除所有的column，transformation，referenceColumn）
     * @param info
     * @return
     */
    public String delTaggingLink(String info){
        LinkProcess linkProcess = new LinkProcess(TokenUtil.getToken());
        out = new ReturnValue();
        Map<String, Object> output = linkProcess.deleteSquidLink(info, out);
        return JsonUtil.toJsonString(output,DSObjectType.SQUIDLINK,out.getMessageCode());
    }

    /**
     * 创建link
     * @param info
     * @return
     */
    public String createTaggingLink(String info){
        Timer timer = new Timer();
        try {
            key=TokenUtil.getKey();
            token = TokenUtil.getToken();
            final Map<String, Object> returnMaps = new HashMap<>();
            returnMaps.put("1",1);
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMaps,DSObjectType.SQUID,"1041","0003",key,
                            token,MessageCode.BATCH_CODE.value());
                }
            },25*1000,25*1000);
            SquidLink squidLink = JsonUtil.object2HashMap(info, SquidLink.class);
            LinkProcess link = new LinkProcess(TokenUtil.getToken());
            out = new ReturnValue();
            Map<String, Object> squidLinkMap = link.createTaggingSquidLink(squidLink, out);
            return JsonUtil.toJsonString(squidLinkMap, DSObjectType.SQUIDLINK, out.getMessageCode());
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            timer.purge();
            timer.cancel();
        }
        return null;
    }
}
