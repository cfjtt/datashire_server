package com.eurlanda.datashire.utility.converter;

import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by Administrator on 2016/9/7.
 */
public class Db2DataTypeConverter {
    public static Map<String, Object> getValue(Map<String, Object> ret,
                                               String key,ResultSet rs,int columnType) throws SQLException {
        switch (DbBaseDatatype.parse(columnType)) {
            case DBCLOB:
            case GRAPHIC:
            case VARGRAPHIC:
                ret.put(key.toUpperCase(), "无法显示的字段类型");
                break;
            default:
                ret.put(key.toUpperCase(), rs.getObject(key)==null?"null":rs.getObject(key)+"");
                break;
        }
        return ret;
    }
}
