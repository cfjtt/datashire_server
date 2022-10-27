package com.eurlanda.datashire.utility.converter;

import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class baseDataTypeConverter {
	public static Map<String, Object> getValue(Map<String, Object> ret,
			String key, ResultSet rs, int columnType) throws SQLException {
		switch (DbBaseDatatype.parse(columnType)) {
			case BYTE:
            case VARBYTE:
                ret.put(key.toUpperCase(), rs.getObject(key) == null ? "null" : new String(rs.getBytes(key)));
                break;
			default:
				ret.put(key.toUpperCase(), rs.getObject(key) == null ? "null" : rs.getObject(key) + "");
				break;
		}
		return ret;
	}
}
