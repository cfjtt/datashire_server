package com.eurlanda.datashire.utility.converter;

import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class MysqlDataTypeConverter {
	public static Map<String, Object> getValue(Map<String, Object> ret,
			String key, ResultSet rs, int columnType) throws SQLException, IOException {
		switch (DbBaseDatatype.parse(columnType)) {
		case TINYINT:
			ret.put(key.toUpperCase(), rs.getObject(key)==null?null:rs.getInt(key));
			break;
		case TINYTEXT:
		case TEXT:
		case MEDIUMTEXT:
		case LONGTEXT:
		case TINYBLOB:
		case BLOB:
		case MEDIUMBLOB:
		case LONGBLOB:
			ret.put(key.toUpperCase(), "无法显示的字段类型");
			break;
		case BINARY:
		case VARBINARY:
			InputStream stream = rs.getBinaryStream(key);
			StringBuffer strBuffer = new StringBuffer("");
			if(stream!=null) {
				byte[] buffer = new byte[stream.available()];
				while ((stream.read(buffer)) > 0) {
					strBuffer.append(new String(buffer,"utf-8"));
				}
				ret.put(key.toUpperCase(),strBuffer.toString());
			} else {
				ret.put(key.toUpperCase(),"null");
			}
			break;
		default:
			ret.put(key.toUpperCase(), rs.getObject(key)==null?"null":rs.getObject(key)+"");
			break;
		}
		return ret;
	}
}
