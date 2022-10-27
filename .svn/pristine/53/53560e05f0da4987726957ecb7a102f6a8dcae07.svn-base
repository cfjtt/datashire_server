package com.eurlanda.datashire.cloudService;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.HyperSQLManager;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.SysConf;

import java.util.HashMap;
import java.util.Map;

/**
 * 新的客户登录接口(访问数列云api)
 * Created by Administrator on 2016/12/23.
 */
public class CloudLoginService {
    private IRelationalDataManager  serverDb = DataAdapterFactory.getDefaultDataManager();

    /**
     * 获取云的数据库
     * @return
     */
    public IRelationalDataManager getCloudDb(){
        DbSquid dataSource = new DbSquid();
        dataSource.setDb_type(DataBaseType.MYSQL.value());
        dataSource.setDb_name(SysConf.getValue("cloud.dbname"));
        dataSource.setHost(SysConf.getValue("cloud.host"));
        dataSource.setUser_name(SysConf.getValue("cloud.username"));
        dataSource.setPassword(SysConf.getValue("cloud.password"));
        return new HyperSQLManager(dataSource);
    }

    /**
     * 客户端登录接口(访问云api)
     * @return
     */
    public String  callCloudLoginApi(String info){
        User user = JsonUtil.object2HashMap(info, User.class);
        String apiUrl=SysConf.getValue("cloud.url");
        Map<String,Object> params = new HashMap<>();
        params.put("username",user.getEmail_address());
        params.put("password",user.getPassword());
        //以post的方式发起http请求
        String jsonStr = HttpClientUtils.doPost(SysConf.getValue("cloud.url"),params);
        return jsonStr;
    }
}
