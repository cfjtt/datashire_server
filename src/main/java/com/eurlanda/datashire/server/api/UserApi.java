package com.eurlanda.datashire.server.api;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.annotation.SocketApi;
import com.eurlanda.datashire.server.annotation.SocketApiMethod;
import com.eurlanda.datashire.server.model.CloudOperateRecord;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.server.service.UserService;
import com.eurlanda.datashire.server.utils.MessagePacketUtils;
import com.eurlanda.datashire.server.utils.SocketUtil;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoMessagePacket;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.packet.ListMessagePacket;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 16/1/13.
 */
@Service
@SocketApi("2001")
public class UserApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserApi.class);

    @Autowired
    private UserService userService;

    @SocketApiMethod(commandId = "0001")
    public String login(String info) {
        ReturnValue out = new ReturnValue();
        User user = JsonUtil.object2HashMap(info, User.class);
        Map<String, Object> map = null;

        if(user != null && StringUtils.isNotEmpty(user.getUser_name())
                && user.getPassword() != null) {
            user = userService.findByUsernameAndPassword(user.getUser_name(), user.getPassword());
            if (null == user) {
                out.setMessageCode(MessageCode.ERR_USER_NOT_EXIST);//用户不存在
                LOGGER.error(">>>>>>>>>>>>>>>>>>登陆的用户信息不存在<<<<<<<<<<<<<<");
            } else {
                map = userService.validateLoginInfo(out);
                map.put("User", user);
                // 将token与用户信息绑定
                SocketUtil.SESSION.put(TokenUtil.getToken(), user);
            }
        } else {
            out.setMessageCode(MessageCode.ERR_USER);//用户不存在
            LOGGER.error(">>>>>>>>>>>>>>>>>>用户{" + info + "}信息错误<<<<<<<<<<<<<<");
        }

        return MessagePacketUtils.infoNewMessagePacket(map, DSObjectType.USER, out);
    }

    @SocketApiMethod(commandId = "0002")
    public String logout() {
        String ret = "";
        InfoMessagePacket<Boolean> packet = new InfoMessagePacket<Boolean>();
        // 修改用户状态[已退出]？
        packet.setCode(MessageCode.SUCCESS.value());
        ret = JsonUtil.object2Json(packet);
        // 移除登录
        User user = SocketUtil.SESSION.get(TokenUtil.getToken());
        if(user != null) {
            SocketUtil.SESSION.remove(TokenUtil.getToken());
            TokenUtil.removeTokenAndKey();
            LOGGER.info("退出登录,用户:{},时间:{}", user.getUser_name(),
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            LOGGER.debug("当前登陆用户数据:{}", SocketUtil.SESSION.size());
            //只有云端的用户才写入数据库
            if(StringUtils.isNotEmpty(user.getSpaceId())){
                CloudOperateRecord record = new CloudOperateRecord();
                record.setUser_id(user.getId());
                record.setSpace_id(Integer.parseInt(user.getSpaceId().substring(1)));
                record.setOperate_time(new Date());
                record.setOperate_type(1);
                record.setContent("用户:"+user.getUser_name()+",操作类型:退出,数猎场:"+user.getSpaceId());
                IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
                try{
                    adapter.openSession();
                    adapter.insert2(record);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    adapter.closeSession();
                }
            }
        } else {
            LOGGER.error(">>>>>>>>>>>没有找到该用户的SESSION,token:{}<<<<<<<<<<<<", TokenUtil.getToken());
        }
        return ret;
    }

    @SocketApiMethod(commandId = "0003")
    public String addUser(String info) {
        ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();
        List<User> userList = JsonUtil.toGsonList(info, User.class);
        if(userList!=null){


            for(int i=0; i< userList.size(); i++){
                // 新增用户时，如果密码为空则保存为默认密码
                if(StringUtils.isEmpty(userList.get(i).getPassword())){
                    userList.get(i).setPassword(CommonConsts.DEFAULT_USER_PASSWORD);
                }
                // 新增/更新时，可不分配或取消角色权限
                if(userList.get(i).getRole_id()<=0){
                    userList.get(i).setRole_id(0);
                }
            }
            LOGGER.debug("list: {}", userList);
            int cnt = userService.saveList(userList);

            LOGGER.debug("新增用户成功数:{}", cnt);
            List<InfoPacket> infoPackets = genInfoPackets(userList, cnt > 0);

            packet.setDataList(infoPackets);// 具体操作结果
            packet.setCode(MessageCode.SUCCESS.value()); // 仅表示程序执行正常，不代表数据添加成功
        }else{
            // 反序列化失败
            packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
        }
        packet.setType(DSObjectType.USER);
        return JsonUtil.object2Json(packet);
    }

    @SocketApiMethod(commandId = "0004")
    public String updateUser(String info) {
        ListMessagePacket<InfoPacket> packet = new ListMessagePacket<InfoPacket>();
        List<User> userList = JsonUtil.toGsonList(info, User.class);
        if(userList != null){
            int cnt = userService.updateList(userList);

            LOGGER.debug("更新用户成功数:{}", cnt);
            List<InfoPacket> infoPackets = genInfoPackets(userList, cnt > 0);

            packet.setDataList(infoPackets);// 具体操作结果
            packet.setCode(MessageCode.SUCCESS.value()); // 仅表示程序执行正常，不代表数据添加成功
        }else{
            // 反序列化失败
            packet.setCode(MessageCode.DESERIALIZATION_FAILED.value());
        }
        packet.setType(DSObjectType.USER);
        return JsonUtil.object2Json(packet);
    }

    private List<InfoPacket> genInfoPackets(List<User> userList, boolean isSuccess) {
        List<InfoPacket> infoPackets = new ArrayList<>(userList.size());
        // 封装返回消息
        for(User u : userList) {
            InfoPacket infoPacket = new InfoPacket();
            infoPacket.setKey(TokenUtil.getKey());
            infoPacket.setType(DSObjectType.USER);
            infoPacket.setId(u.getId());
            if(isSuccess) {  // 保存成功
                infoPacket.setCode(MessageCode.SUCCESS.value());
            } else {  // 保存失败
                infoPacket.setCode(MessageCode.SQL_ERROR.value());
            }
            infoPackets.add(infoPacket);
        }
        return infoPackets;
    }

    @SocketApiMethod(commandId = "0005")
    public String getAllUsers() {
        ListMessagePacket<User> packet = new ListMessagePacket<User>();
        packet.setType(DSObjectType.USER);
        List<User> userList =  userService.getAllUser();
        packet.setCode((userList==null||userList.size()==0)?MessageCode.NODATA.value():MessageCode.SUCCESS.value());
        packet.setDataList(userList);
        return JsonUtil.object2Json(packet);
    }
}
