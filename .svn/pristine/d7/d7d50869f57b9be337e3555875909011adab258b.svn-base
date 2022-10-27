package com.eurlanda.datashire.server.utils.utility;

import com.eurlanda.datashire.entity.NOSQLConnectionSquid;
import com.eurlanda.datashire.server.utils.entity.DBConnectionInfo;
import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * Created by e56 on 2015/5/19.
 */
public class NoSqlConnectionUtil {
    public static DB createMongoDBConnection(NOSQLConnectionSquid nosql){
        Mongo m = null;
        if (nosql.getHost().contains(":")){
            String host = nosql.getHost().split(":")[0];
            int port = Integer.parseInt(nosql.getHost().split(":")[1]);
            m = new Mongo(host, port);
        }else{
            m = new Mongo(nosql.getHost(), nosql.getPort());
        }
        DB d = m.getDB(nosql.getDb_name());
        return d;
    }

    public static DB createMongoDBConnection(DBConnectionInfo nosql){
        Mongo m = null;
        if (nosql.getHost().contains(":")){
            String host = nosql.getHost().split(":")[0];
            int port = Integer.parseInt(nosql.getHost().split(":")[1]);
            m=new Mongo(host,port);
        }else{
            m = new Mongo(nosql.getHost(), nosql.getPort());
        }
        DB d = m.getDB(nosql.getDbName());
        return d;
    }

}
