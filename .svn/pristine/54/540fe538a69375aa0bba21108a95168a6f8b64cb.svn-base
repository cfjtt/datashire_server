package com.eurlanda.datashire.server.utils;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.ReturnValue;

/**
 * Created by zhudebin on 2017/6/8.
 */
public class MessagePacketUtils {

    /**
     * 单对象转换成Json格式
     * 作用描述：
     * 修改说明：
     * @param object
     * @return
     */
    public static String infoNewMessagePacket(Object object, DSObjectType type, ReturnValue out) {
        return JsonUtil.toJsonString(object, type, out.getMessageCode());
    }
}
