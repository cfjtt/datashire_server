package com.eurlanda.datashire.sprint7.socket;

import com.alibaba.fastjson.JSONObject;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.socket.Agreement;
import com.eurlanda.datashire.socket.AgreementProtocol;
import com.eurlanda.datashire.socket.MessagePacket;
import com.eurlanda.datashire.socket.protocol.ResponseStatus;
import com.eurlanda.datashire.socket.protocol.ServiceConf;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.text.MessageFormat;

/**
 socketMessage->service业务映射处理器。
 */
public class DefaultMessageProcessor extends AbsMessageProcessor  implements ISocketMessageProcessor{

    private static Logger logger = Logger.getLogger(DefaultMessageProcessor.class);


    @Override
    public MessagePacket doProcess(SocketRequestContext sc) {
        MessagePacket packet = sc.getMessagePacket();
        //命令呼叫成功
        ResponseStatus status = ResponseStatus.SUCCESS_CALL;
        //得到Listner关联的上行数据包
		/*
		 *  获取处理类
		 *  zjweii:此处未考虑多线程，存在重大BUG，会导致客户端消息发送错位a->b。
		 */
        Object service = AgreementProtocol.AGREEMENT_HANDLER.get(packet.getCommandId());
        // 获取对应参数定义
        ServiceConf serviceConf = Agreement.ServiceConfMap.get(packet.getCommandId().concat(packet.getChildCommandId()));
		/*
		 *  修复方法。
		 */
        try {
            String beanName=service.getClass().getSimpleName();
            String firstName = beanName.substring(0,1).toLowerCase();
            beanName=beanName.replaceFirst("^\\w", firstName);
            //service = service.getClass().newInstance();
            //serviceConf = serviceConf.getClass().newInstance();
            CommonConsts.application_Context.getBean(beanName);
        } catch (Exception e1) {
            throw new RuntimeException("未找到spring bean");
        }

        Class<?> cls=null;
        Method meth = null;
        Object retobj = null;
        String desc = "";
        long start = System.currentTimeMillis();
        try {
            // 将token 和 key 保存至 ThreadLocal中
            TokenUtil.setTokenAndKey(StringUtils.bytes2Str(packet.getToken()),
                    StringUtils.bytes2Str(packet.getGuid()));
            cls=service.getClass();

            // 传说中的依赖注入。。。(可以考虑统一配置，权限部分不需要token和key)
            /**  通过ThreadLocal来保存
            Field field = cls.getDeclaredField("token");
            field.setAccessible(true);
            field.set(service,StringUtils.bytes2Str(packet.getToken()));
            field = cls.getDeclaredField("key");
            field.setAccessible(true);
            field.set(service,StringUtils.bytes2Str(packet.getGuid()));
             */

            //入参类型为空的情况
            if(Void.TYPE.equals(serviceConf.getArgType()))
            {
                meth = cls.getMethod(serviceConf.getMethodName());
            }else
            {
                meth = cls.getMethod(serviceConf.getMethodName(), serviceConf.getArgType());
            }

            // 消息包内容
            //String info = JSONObject.fromObject(StringUtils.bytes2Str(packet.getData())).getString("info");
            String info = StringUtils.bytes2Str(packet.getData());

            //校验连接数
            CommonConsts.initDBSourceState();
            if (Void.TYPE.equals(serviceConf.getArgType())) {
                retobj = meth.invoke(service);
            } else {
                retobj = meth.invoke(service, info);
            }
            int state = CommonConsts.checkDBSourceState();
            if(state > 0) {
                // 存在开启的数据库连接没有关闭
                logger.error("\t\t\t=========== 存在开启的数据库连接没有关闭 ======== ");
                logger.error("\t\t=========== 存在开启的数据库连接没有关闭 ======== ");
                logger.error("\t\t=========== 存在开启的数据库连接没有关闭 ======== ");
                logger.error("\t\t\t=========== 存在开启的数据库连接没有关闭 ======== ");
            }

            logger.info("流水号："+new String(packet.getGuid(),0,36)+",客户端："+packet.getChannel().getRemoteAddress().toString()
                    + ",消息号:"+packet.getCommandId()+packet.getChildCommandId()+"，调用业务："+service.getClass().getSimpleName()+"."+meth.getName());

            // 不向客户端发送响应内容
            if (!serviceConf.isSendResponse()){
                logger.info("消息提示：函数设置为取消发送响应内容!");
                // 将token 和 key 从 ThreadLocal中移除
                TokenUtil.removeTokenAndKey();
                return null;
            }
            if(serviceConf.isReplaceKey()){ // 替换包头
                throw new RuntimeException("替换包头----------------找朱德彬");
//                packet.setGuid(StringUtils.str2Bytes(field.get(service).toString()));
            }
        } catch (Exception e) {
            status = ResponseStatus.ERROR_SYSTEM;
            StringWriter pw = new StringWriter();
            PrintWriter p = new PrintWriter(pw);
            e.printStackTrace(p);
            retobj = "{\"code\":-9999,\"desc\":\""+pw.toString()+"\"}";
            logger.error(MessageFormat.format("{0}.{1} service error!", cls==null?null:cls.getSimpleName(), meth==null?null:meth.getName()), e);
            packet.setData(StringUtils.str2Bytes(
                    JSONObject.parseObject(
                            retobj == null ? "" : retobj.toString()
                    ).toString())
            );
            logger.info("调用业务："+service.getClass().getSimpleName()+"."+meth.getName()+ " 执行时间 :"+(System.currentTimeMillis()-start)+" 包大小："+(packet.getData()==null?-1:packet.getData().length+84)+" CODE："+-9999);
            start = System.currentTimeMillis();
            return packet;
        }
        JSONObject jsonObj = JSONObject.parseObject(
                retobj==null?"":retobj.toString()
        );
        String code = "";
        int type = 0;
        if (jsonObj!=null&&jsonObj.containsKey("code")){
            code = String.valueOf(jsonObj.get("code"));
        }
        if (jsonObj!=null&&jsonObj.containsKey("type")){
            type = DSObjectType.parse(jsonObj.get("type")+"").value();
        }
        packet.setDsObjectType(type);
        packet.setData(StringUtils.str2Bytes(jsonObj.toString()));
        logger.info("调用业务："+service.getClass().getSimpleName()+"."+meth.getName()+ " 执行时间 :"+(System.currentTimeMillis()-start)+" 包大小："+(packet.getData()==null?-1:packet.getData().length+84)+" CODE："+code);
        start = System.currentTimeMillis();

        // 将token 和 key 从 ThreadLocal中移除
        TokenUtil.removeTokenAndKey();
        return packet;
    }

}