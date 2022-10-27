package com.eurlanda.datashire.utility.converter;

import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class OracleDataTypeConverter {
    public static Map<String, Object> getValue(Map<String, Object> ret,
                                               String key, ResultSet rs, int columnType) throws SQLException, IOException {
        switch (DbBaseDatatype.parse(columnType)) {
            case TIMESTAMP:
                ret.put(key.toUpperCase(), rs.getTimestamp(key));
                break;
            case LONG:
            case BLOB:
            case NCLOB:
            case CLOB:
            case BFILE:
                ret.put(key.toUpperCase(), "无法显示的字段类型");
                break;
            case BINARY:
            case VARBINARY:
                InputStream stream = rs.getBinaryStream(key);
                StringBuffer strBuffer = new StringBuffer("");
                if (stream != null) {
                    byte[] buffer = new byte[stream.available()];
                    while ((stream.read(buffer)) > 0) {
                        strBuffer.append(new String(buffer, "utf-8"));
                    }
                }
                ret.put(key.toUpperCase(), strBuffer.toString());
                break;
            default:
                ret.put(key.toUpperCase(), rs.getObject(key) == null ? "null" : rs.getObject(key) + "");
                break;
        }
        return ret;
    }
}
