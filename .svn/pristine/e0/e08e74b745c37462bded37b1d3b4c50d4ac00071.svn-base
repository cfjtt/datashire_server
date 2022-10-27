package com.eurlanda.datashire.server.utils.dbsource.datatype;
/**
 * 数据类型映射用到的类型。
 * @author Gene
 *
 */
public class DataType {
	private String name; //类型标识
	private String nameExt; //类型标识扩展,比如 TIMESTAMP (n) WITH TIME ZONE中的WITH TIME ZONE
	private Integer length; //长度,  -1表示该类型不需要设置length属性
	private Integer precision; //精度, -1表示该类型不需要设置precision属性
	private Integer scale; //小数长度 -1表示该类型不需要设置scale属性
	private String fullName; // 把name, length, precision 和scale组装后的标识串,比如DECIMAL(12,4)
	private String pattern; //根据name, length, precision 和scale提取出的匹配模式,用于和mapping dict 匹配
	private String systemDBType;		// 系统数据类型
	private String outDBType;			// 输出时的数据类型
	private Class javaType;				// java运行时的类型。
	
	//只有类型标识
	public DataType(String newName){
		name = newName.toUpperCase();
		length = -1;
		precision = -1;
		scale = -1;
		fullName = name;
    	pattern = name;
	}
	//有类型标识和长度
	public DataType(String newName, Integer newLength){
		name = newName.toUpperCase();
		length = newLength;
		precision = -1;
		scale = -1;
		fullName = name + "(" + length.toString() + ")";
    	pattern = name + "(n)";
	}
	//有类型标识,精度和小数位
	public DataType(String newName, Integer newPrecision, Integer newScale){
		name = newName.toUpperCase();
		length = -1;
		precision = newPrecision;
		scale = newScale;
		fullName = name + "(" + precision.toString() + "," + scale.toString() + ")";
    	pattern = name + "(p,s)";
	}
	//有类型标识,标识扩展和小数位
	public DataType(String newName, String newNameExt, Integer newScale){
		name = newName.toUpperCase();
		nameExt = newNameExt.toUpperCase();
		scale = newScale;
		fullName = name + "(" + scale.toString() + ")" + newNameExt;
    	pattern = name + " (s) " + newNameExt;
    }
    
	//
	public DataType(String newName, Integer newScale, boolean isScale){
		name = newName.toUpperCase();
		scale = newScale;
		if(true == isScale){
			fullName = name + "(*, " + scale.toString() + ")";
			pattern = name + " (*, s) ";
		}
    }
   
	//
	public static void main(String[] args) throws Exception {		
		DataType dt = new DataType("decimal",   2,3);
		System.out.println(dt.fullName);
		System.out.println(dt.pattern);
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getNameExt() {
		return nameExt;
	}
	public void setNameExt(String nameExt) {
		this.nameExt = nameExt;
	}
	
	public Integer getLength() {
		return length;
	}
	public void setLength(Integer length) {
		this.length = length;
	}
	
	public Integer getPrecision() {
		return precision;
	}
	public void setPrecision(Integer precision) {
		this.precision = precision;
	}
	
	public Integer getScale() {
		return scale;
	}
	public void setScale(Integer scale) {
		this.scale = scale;
	}
	
	public String getFullName() {
		return fullName;
	}
	public void setFullName(String fullName) {
		this.fullName = fullName;
	}
	
	public String getPattern() {
		return pattern;
	}
	public void setPattern(String pattern) {
		this.pattern = pattern;
	}
	public String getSystemDBType() {
		return systemDBType;
	}
	public void setSystemDBType(String systemDBType) {
		this.systemDBType = systemDBType;
	}
	public String getOutDBType() {
		return outDBType;
	}
	public void setOutDBType(String outDBType) {
		this.outDBType = outDBType;
	}
	public Class getJavaType() {
		return javaType;
	}
	public void setJavaType(Class javaType) {
		this.javaType = javaType;
	}
	
	
}
