package com.eurlanda.datashire.userDefinedService;

import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

/**
 * Created by Eurlanda on 2017/4/24.
 */
@Service
public class UserDefinedService {

    private String key;
    private String token;
    private Logger logger = Logger.getLogger(UserDefinedService.class);

    /**
     * 创建UserDefinedSquid
     *
     * @param info
     * @return
     */
    public String createUserDefinedSquid(String info) {
        UserDefinedServiceImpl impl = new UserDefinedServiceImpl();
        return impl.createUserDefinedSquid(info);
    }

    /**
     * 更新squid
     *
     * @param info
     * @return
     */
    public String updateUserDefinedSquid(String info) {
        UserDefinedServiceImpl impl = new UserDefinedServiceImpl();
        return impl.updateUserDefinedSquid(info);
    }

    /**
     * 更新dataMappingColumn
     *
     * @param info
     * @return
     */
    public String updateUserDefinedMappingColumns(String info) {
        UserDefinedServiceImpl impl = new UserDefinedServiceImpl();
        return impl.updateUserDefinedMappingColumns(info);
    }

    /**
     * 更新parameterColumn(直接更新)
     *
     * @param info
     * @return
     */
    public String updateUserDefinedParameterColumns(String info) {
        UserDefinedServiceImpl impl = new UserDefinedServiceImpl();
        return impl.updateUserDefinedParameterColumns(info);
    }

    /**
     * 创建squidLink连接
     * 一个squid只能存在一个squidLink
     * 并且不需要同步column
     *
     * @param info
     * @return
     */
    public String createUserDefinedSquidLink(String info) {
        ReturnValue out = new ReturnValue();
        UserDefinedServiceImpl serviceImpl = new UserDefinedServiceImpl();
        return serviceImpl.createSquidLink(info, out);
    }

    /**
     * 删除squidLink（删除dataMap里面的columnId，删除column）
     * 需要同步下游
     *
     * @param info
     * @return
     */
    public String deleteUserDefinedSquidLink(String info) throws Exception {
        ReturnValue out = new ReturnValue();
        UserDefinedServiceImpl serviceImpl = new UserDefinedServiceImpl();
        return serviceImpl.deleteSquidLink(info);
    }

    /**
     * 获取className
     *
     * @param info
     * @return
     */
    public String changeUserDefinedClassName(String info) {
        UserDefinedServiceImpl serviceImpl = new UserDefinedServiceImpl();
        return serviceImpl.changeUserDefinedClassName(info);
    }
}
