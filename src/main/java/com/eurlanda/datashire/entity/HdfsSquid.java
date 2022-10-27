package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;

import java.sql.Types;
import java.util.List;
/**
 * hdfs连接实体类
 * @author lei.bin
 *
 */
@MultitableMapping(pk="ID", name ={"DS_SQUID"}, desc = "")
public class HdfsSquid extends Squid {
	@ColumnMpping(name="host", desc="主机IP或者名字", nullable=true, precision=20, type=Types.VARCHAR, valueReg="")
	private String host;
	@ColumnMpping(name="user_name", desc="登录用户名", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String user_name;
	@ColumnMpping(name="password", desc="登录密码", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String password;
	@ColumnMpping(name="file_path", desc="目标数据库名字", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String file_path;
	@ColumnMpping(name="unionall_flag", desc="是否全选(文件格式一致)", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int unionall_flag;
	@ColumnMpping(name="including_subfolders_flag", desc="是否处理FilePath下的所有子文件夹和文件", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int including_subfolders_flag;
	@ColumnMpping(name="allowanonymous_flag", desc="是否允许匿名连接", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int allowanonymous_flag;
	@ColumnMpping(name="course_id", desc="课程id", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
	private Integer  course_id;

	public Integer getCourse_id() {
		return course_id;
	}

	public void setCourse_id(Integer course_id) {
		this.course_id = course_id;
	}
	private List<SourceTable> sourceTableList;

	public List<SourceTable> getSourceTableList() {
		return sourceTableList;
	}

	public void setSourceTableList(List<SourceTable> sourceTableList) {
		this.sourceTableList = sourceTableList;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getUser_name() {
		return user_name;
	}
	public void setUser_name(String user_name) {
		this.user_name = user_name;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getFile_path() {
		return file_path;
	}
	public void setFile_path(String file_path) {
		this.file_path = file_path;
	}
	public int getUnionall_flag() {
		return unionall_flag;
	}
	public void setUnionall_flag(int unionall_flag) {
		this.unionall_flag = unionall_flag;
	}


	public int getIncluding_subfolders_flag() {
		return including_subfolders_flag;
	}

	public void setIncluding_subfolders_flag(int including_subfolders_flag) {
		this.including_subfolders_flag = including_subfolders_flag;
	}

	public int getAllowanonymous_flag() {
		return allowanonymous_flag;
	}

	public void setAllowanonymous_flag(int allowanonymous_flag) {
		this.allowanonymous_flag = allowanonymous_flag;
	}
}
