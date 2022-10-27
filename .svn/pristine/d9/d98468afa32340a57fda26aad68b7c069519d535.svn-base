package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import org.apache.commons.lang.StringUtils;

/**
 * 列属性拼接处理类
 * @author 
 *
 */
public class ColumnAttributeUtils {

	public String getColumnAttribute(Column column) {
		StringBuffer stringBuffer = new StringBuffer(200);

		if (StringUtils.isNotBlank(column.getName())) {
			stringBuffer.append(column.getName());// 列名
			stringBuffer.append(" : ");
			stringBuffer.append(SystemDatatype.parse(column.getData_type()));// 类型
			if (9 != column.getData_type())// int 类型特殊处理
			{
				if (0 != column.getLength() && 0 == column.getPrecision()) {
					stringBuffer.append("(" + column.getLength() + ")");
				} else if (0 != column.getLength() && 0 != column.getPrecision()) {
					stringBuffer.append("(" + column.getLength() + ","
							+ column.getPrecision() + ")");// 长度
				}

			}
			if (column.isFK()) {
				stringBuffer.append(" (FK) ");
			}
		}
	

		return stringBuffer.toString();
	}
}
