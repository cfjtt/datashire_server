package com.eurlanda.datashire.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * 业务对象帮助类
 * 	1.提供BO vo PO 对象类型转换
 * 
 * @author dang.lu 2013.11.20
 *
 */
public class BOHelper {
	
	/**
	 * 将SourceColumn转换成真实列
	 * 		SourceColumn: source squid 中对应表的列
	 * 		Column： data squid 中的真实列
	 * 		ReferenceColumn:  data squid 中的引用列
	 * 
	 * @param sourceColumns
	 * @return
	 */
	public static List<Column> convert(List<SourceColumn> sourceColumns){
		if(sourceColumns!=null && !sourceColumns.isEmpty()){
			List<Column> list = new ArrayList<Column>(sourceColumns.size());
			for(int i=0; i<sourceColumns.size(); i++){
				SourceColumn src = sourceColumns.get(i);
				if(src!=null){
					Column c = new Column();
					c.setId(src.getId());
					c.setName(src.getName());
					c.setData_type(src.getData_type());
					c.setNullable(src.isNullable());
					c.setLength(src.getLength());
					c.setPrecision(src.getPrecision());
					list.add(c);
				}
			}
			return list;
		}
		return null;
	}

	public static List<SourceColumn> convert2(List<Column> sourceColumns){
		if(sourceColumns!=null && !sourceColumns.isEmpty()){
			List<SourceColumn> list = new ArrayList<SourceColumn>(sourceColumns.size());
			for(int i=0; i<sourceColumns.size(); i++){
				Column src = sourceColumns.get(i);
				if(src!=null){
					SourceColumn c = new SourceColumn();
					c.setId(src.getId());
					c.setName(src.getName());
					c.setData_type(src.getData_type());
					c.setNullable(src.isNullable());
					c.setLength(src.getLength());
					c.setPrecision(src.getPrecision());
					list.add(c);
				}
			}
			return list;
		}
		return null;
	}
	
	public static List<Column> convert3(List<ReferenceColumn> sourceColumns){
		if(sourceColumns!=null && !sourceColumns.isEmpty()){
			List<Column> list = new ArrayList<Column>(sourceColumns.size());
			for(int i=0; i<sourceColumns.size(); i++){
				ReferenceColumn src = sourceColumns.get(i);
				if(src!=null){
					Column c = new Column();
					c.setId(src.getId()<=0?src.getColumn_id():src.getId());
					c.setKey(src.getKey());
					c.setName(src.getName());
					c.setData_type(src.getData_type());
					c.setNullable(src.isNullable());
					c.setLength(src.getLength());
					c.setPrecision(src.getPrecision());
					list.add(c);
				}
			}
			return list;
		}
		return null;
	}
	
	public static List<SourceColumn> convert4(List<ReferenceColumnGroup> sourceColumns){
		if(sourceColumns!=null && !sourceColumns.isEmpty()){
			List<SourceColumn> list = new ArrayList<SourceColumn>();
			for(int i=0; i<sourceColumns.size(); i++){
				if(sourceColumns.get(i)==null || sourceColumns.get(i).getReferenceColumnList()==null){
					continue;
				}
				for(int j=0; j<sourceColumns.get(i).getReferenceColumnList().size(); j++){
					ReferenceColumn src = sourceColumns.get(i).getReferenceColumnList().get(j);
					if(src!=null){
						SourceColumn c = new SourceColumn();
						c.setId(src.getId());
						c.setName(src.getName());
						c.setData_type(src.getData_type());
						c.setNullable(src.isNullable());
						c.setLength(src.getLength());
						c.setPrecision(src.getPrecision());
						list.add(c);
					}
				}
			}
			return list;
		}
		return null;
	}
	
}
