package com.eurlanda.datashire.sprint7.packet;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.utility.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author Fumin
 *
 */
public class InfoPacket {

	private int id;//主键
	
	private String key;
	
	private DSObjectType type;
	
	//操作(add/update/delete)结果(succeed/failed)
	//add失败id==-1
	private int code;
	private int repositoryId;
	private String name;

	public InfoPacket(int id, String key, DSObjectType type, int code) {
		super();
		this.id = id;
		this.key = key;
		this.type = type;
		this.code = code;
	}

	public InfoPacket(String key){
		this.key = key;
	}
	
	public InfoPacket(Object key){
		this.key = StringUtils.isNull(key)?"":String.valueOf(key);
	}
	
	public InfoPacket(Object key, DSObjectType type){
		this.key = StringUtils.isNull(key)?"":String.valueOf(key);
		this.type = type;
	}
	
	/**
	 * 构造器
	 */
	public InfoPacket(){
		
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public DSObjectType getType() {
		return type;
	}

	public void setType(DSObjectType type) {
		this.type = type;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}


	public int getRepositoryId() {
		return repositoryId;
	}

	public void setRepositoryId(int repositoryId) {
		this.repositoryId = repositoryId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "InfoPacket [id=" + id + ", key=" + key + ", type=" + type
				+ ", code=" + code + "]";
	}
	
	public String toJson(){
		return null;
	}
	
	/** InfoPacket序列化为JSON串(only key and type) （快速构建JSON串，快过google、fast json, 更快更小字节）*/
	public static final String list2json(List<InfoPacket> list){
		int s = list==null?0:list.size();
		if(s>=1){
			StringBuffer b = new StringBuffer(1024);
			b.append("[");
			for(int i=0; i<s; i++){
				b.append("{\"key\":\"").append(list.get(i).getKey()).append("\",\"type\":\"").append(list.get(i).getType()).append("\"}");
				if(i!=s-1){
					b.append(",");
				}
			}
			return b.append("]").toString();
		}else{
			return "[]";
		}
	}
	
	
	public static void main(String[] args) {
		for(int i=0; i<3; i++) tst1();
	}
	
	static void tst1(){
		long s = System.currentTimeMillis();
		int t = 0;
		List<InfoPacket> keyList = new ArrayList<InfoPacket>();
		//Map map = new HashMap();
		List list = new ArrayList();
		for(int i=0; i<t; i++){
			keyList.add(new InfoPacket(StringUtils.generateGUID(), DSObjectType.SQUID));
			//map.put(i, i);
			//list.add(i);
		}
		//JsonUtil.object2Json(keyList);
		list2json(keyList);
		
		//System.out.println(JsonUtil.toString(keyList, DSObjectType.SQUID, new ReturnValue()));
		
		//System.out.println(JsonUtil.object2Json(keyList).length());
		//System.err.println(list2json(keyList).length());
		System.out.println((System.currentTimeMillis()-s)+" ms.");
	}
}
