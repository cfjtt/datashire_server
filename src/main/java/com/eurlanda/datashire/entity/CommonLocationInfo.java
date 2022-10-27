package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.enumeration.DSObjectType;

public class CommonLocationInfo {
	//[{"X":303,"Y":199,"Left_X":0,"Left_Y":0,"Right_X":0,"Right_Y":0,"W":85,"H":51,"Id":0,"Key":"d27a1ff2-c26b-4ed0-b6c1-41e3cebc29e8","Name":null,"Status":0,"Type":12}]
			
	/* 位置信息更新统一映射规则：
	  			SquidModelBase				SquidLink	Transformation	Group(权限)
		x		location_X			start_X		location_X		location_X
		y		location_Y			start_Y		location_Y		location_Y
		w		squid_With			end_X		
		h		squid_Height		end_Y		
		left_X	column_group_x		startmiddle_x		
		left_Y	column_group_y		startmiddle_y		
		right_X	reference_group_x	endmiddle_x		
		right_Y	reference_group_y	endmiddle_y		 */
	public String toSql(){ // 检查key是否重复，是否系统默认库
		if(id<=0){
			return null;
		}
		if(type == DSObjectType.GROUP.value()){
			return "UPDATE DS_SYS_GROUP SET location_x="+x+", location_y="+y+" WHERE ID="+id+"";
		}
		else if(type == DSObjectType.TRANSFORMATION.value()){
			return "UPDATE DS_TRANSFORMATION SET location_x="+x+", location_y="+y+" WHERE ID="+id+"";
		}
		else if(type == DSObjectType.SQUIDLINK.value()){
			return "UPDATE DS_SQUID_LINK SET start_X="+x+", start_Y="+y+
					", end_X="+w+", end_Y="+h+
					", startmiddle_x="+left_X+", startmiddle_y="+left_Y+
					", endmiddle_x="+right_X+", endmiddle_y="+right_Y+
					" WHERE ID="+id+"";
		}
		else if(type == DSObjectType.EXTRACT.value()
				|| type == DSObjectType.DBSOURCE.value()
				|| type == DSObjectType.STAGE.value()
				|| type == DSObjectType.SQUID.value()){
			return "UPDATE DS_SQUID SET location_X="+x+", location_Y="+y+
					", squid_Width="+w+", squid_Height="+h+
					", column_group_x="+left_X+", column_group_y="+left_Y+
					", reference_group_x="+right_X+", reference_group_y="+right_Y+
					" WHERE ID="+id+"";
		}
		return null;
	}
	
	 private int x;
     private int y;
     private int w;
     private int h;
     private int left_X;
     private int left_Y;
     private int right_X;
     private int right_Y;
     
 	private int id;
 	private int type;
     
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public int getX() {
		return x;
	}
	public void setX(int x) {
		this.x = x;
	}
	public int getY() {
		return y;
	}
	public void setY(int y) {
		this.y = y;
	}
	public int getW() {
		return w;
	}
	public void setW(int w) {
		this.w = w;
	}
	public int getH() {
		return h;
	}
	public void setH(int h) {
		this.h = h;
	}
	public int getLeft_X() {
		return left_X;
	}
	public void setLeft_X(int left_X) {
		this.left_X = left_X;
	}
	public int getLeft_Y() {
		return left_Y;
	}
	public void setLeft_Y(int left_Y) {
		this.left_Y = left_Y;
	}
	public int getRight_X() {
		return right_X;
	}
	public void setRight_X(int right_X) {
		this.right_X = right_X;
	}
	public int getRight_Y() {
		return right_Y;
	}
	public void setRight_Y(int right_Y) {
		this.right_Y = right_Y;
	}
	
	@Override
	public String toString() {
		return "Location [x=" + x + ", y=" + y + ", w=" + w + ", h="
				+ h + ", left_X=" + left_X + ", left_Y=" + left_Y
				+ ", right_X=" + right_X + ", right_Y=" + right_Y + ", id="
				+ id + ", type=" + type + "]";
	}
	
}
