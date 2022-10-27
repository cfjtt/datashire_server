package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;

@MultitableMapping(pk="", name = "DS_SYS_PRIVILEGE", desc = "操作权限配置表")
public class Privilege{
	{
		this.setType(DSObjectType.PRIVILEGE);
	}
	
	@ColumnMpping(name="party_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int party_id;// int; //    -- party 可能是user， role或者group
	@ColumnMpping(name="party_type", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private String party_type; // char(1),    -- U: USER， R: ROLE， G: GROUP
	@ColumnMpping(name="entity_type_id", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg=">=1")
	private int entity_type_id; //  int,    -- 该权限设置所涉及到的被操作对象类型 
	
	@ColumnMpping(name="can_view", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean can_view; //  char(1),    -- Y: CAN READ; N: CANNOT READ 
	@ColumnMpping(name="can_create", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean can_create; //  char(1),
	@ColumnMpping(name="can_update", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean can_update; //  char(1),
	@ColumnMpping(name="can_delete", desc="", nullable=true, precision=1, type=Types.VARCHAR, valueReg="")
	private boolean can_delete; //  char(1)
	

	private String entity_type_name; // 只给前台显示使用
	
	// 直接上级权限，没有则全false
	private boolean parent_can_view; //  char(1),    -- Y: CAN READ; N: CANNOT READ 
	private boolean parent_can_create; //  char(1),
	private boolean parent_can_update; //  char(1),
	private boolean parent_can_delete; //  char(1)
	
	private DSObjectType type;
	
	public int getParty_id() {
		return party_id;
	}
	public void setParty_id(int party_id) {
		this.party_id = party_id;
	}
	public String getParty_type() {
		return party_type;
	}
	public void setParty_type(String party_type) {
		this.party_type = party_type;
	}
	public int getEntity_type_id() {
		return entity_type_id;
	}
	public void setEntity_type_id(int entity_type_id) {
		this.entity_type_id = entity_type_id;
	}
	public boolean isCan_view() {
		return can_view;
	}
	public void setCan_view(boolean can_view) {
		this.can_view = can_view;
	}
	public boolean isCan_create() {
		return can_create;
	}
	public void setCan_create(boolean can_create) {
		this.can_create = can_create;
	}
	public boolean isCan_update() {
		return can_update;
	}
	public void setCan_update(boolean can_update) {
		this.can_update = can_update;
	}
	public boolean isCan_delete() {
		return can_delete;
	}
	public void setCan_delete(boolean can_delete) {
		this.can_delete = can_delete;
	}
	public boolean isParent_can_view() {
		return parent_can_view;
	}
	public void setParent_can_view(boolean parent_can_view) {
		this.parent_can_view = parent_can_view;
	}
	public boolean isParent_can_create() {
		return parent_can_create;
	}
	public void setParent_can_create(boolean parent_can_create) {
		this.parent_can_create = parent_can_create;
	}
	public boolean isParent_can_update() {
		return parent_can_update;
	}
	public void setParent_can_update(boolean parent_can_update) {
		this.parent_can_update = parent_can_update;
	}
	public boolean isParent_can_delete() {
		return parent_can_delete;
	}
	public void setParent_can_delete(boolean parent_can_delete) {
		this.parent_can_delete = parent_can_delete;
	}
	public String getEntity_type_name() {
//		for(int i=0; i<DSObjectType.Entity_Type_ID.length; i++){
//			if(this.getEntity_type_id() == DSObjectType.Entity_Type_ID[i]){
//				return DSObjectType.Entity_Type_Name[i];
//			}
//		}
		return entity_type_name;
	}
	public void setEntity_type_name(String entity_type_name) {
		for(int i=0; i<DSObjectType.Entity_Type_ID.length; i++){
			if(this.getEntity_type_id() == DSObjectType.Entity_Type_ID[i]){
				this.entity_type_name = DSObjectType.Entity_Type_Name[i];
				break;
			}
		}
		//this.entity_type_name = entity_type_name;
	}
	
	public DSObjectType getType() {
		return type;
	}
	public void setType(DSObjectType type) {
		this.type = type;
	}
	@Override
	public String toString() {
		return "Privilege [party_id=" + party_id + ", party_type=" + party_type
				+ ", entity_type_id=" + entity_type_id + ", VCUD="
				+ can_view + " " + can_create + " " + can_update + " " + can_delete
				+ ", parent VCUD=" + parent_can_view + " " + parent_can_create + " " + parent_can_update + " " + parent_can_delete + "]";
	}

}
