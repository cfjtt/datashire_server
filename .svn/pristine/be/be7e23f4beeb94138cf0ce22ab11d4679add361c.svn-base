package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;

/**
 * 
 * @author bo.dang
 *
 */
@MultitableMapping(name = {"DS_SQUID", "DS_REPORT_SQUID"},pk="ID", desc = "")
public class ReportSquid extends Squid {
	
	{
		this.setType(DSObjectType.REPORT.value());
	}
	// 是否是实时报表
	@ColumnMpping(name="is_real_time", desc="", nullable=true, precision=100, type=Types.BOOLEAN, valueReg="")
	private boolean is_real_time;
	// 发布后在reportProtal上显示的名字
	@ColumnMpping(name="report_name", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String report_name;
	// 报表模板定义
	@ColumnMpping(name="report_template", desc="", nullable=true, precision=100, type=Types.CLOB, valueReg="")
	private String report_template;
	// 文件夹ID
	@ColumnMpping(name="folder_id", desc="", nullable=false, precision=0, type=Types.INTEGER, valueReg="")
	private int folder_id;
	@ColumnMpping(name="is_support_history", desc="", nullable=true, precision=0, type=Types.BOOLEAN, valueReg="")
	private boolean is_support_history;
	// 保留最大版本数
	@ColumnMpping(name="max_history_count", desc="", nullable=true, precision=0, type=Types.INTEGER, valueReg="")
	private int max_history_count;
	// 是否发送邮件
	@ColumnMpping(name="is_send_email", desc="", nullable=true, precision=100, type=Types.BOOLEAN, valueReg="")
	private boolean is_send_email;
	// 收件人地址
	@ColumnMpping(name="email_receivers", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String email_receivers;
	// 邮件Title
	@ColumnMpping(name="email_title", desc="", nullable=true, precision=100, type=Types.VARCHAR, valueReg="")
	private String email_title;
	// 附件格式
	@ColumnMpping(name="email_report_format", desc="", nullable=true, precision=100, type=Types.INTEGER, valueReg="")
	private int email_report_format;
	// 是否打包
	@ColumnMpping(name="is_packed", desc="", nullable=true, precision=0, type=Types.BOOLEAN, valueReg="")
	private boolean is_packed;
	// 是否压缩
	@ColumnMpping(name="is_compressed", desc="", nullable=true, precision=0, type=Types.BOOLEAN, valueReg="")
	private boolean is_compressed;
	// 加密
	@ColumnMpping(name="is_encrypted", desc="", nullable=true, precision=0, type=Types.BOOLEAN, valueReg="")
	private boolean is_encrypted;
	// 打开密码
	@ColumnMpping(name="password", desc="", nullable=true, precision=20, type=Types.VARCHAR, valueReg="")
	private String password;
	private int repository_id;
	private String publishing_path;
	
	
	public ReportSquid(){
	}
	
	public ReportSquid(Squid s){
		if(s!=null && s.getId()>0){
			this.setId(s.getId());
			this.setName(s.getName());
			this.setTable_name(s.getTable_name());
			this.setFilter(s.getFilter());
			this.setKey(s.getKey());
			this.setSquidflow_id(s.getSquidflow_id());
			this.setLocation_x(s.getLocation_x());
			this.setLocation_y(s.getLocation_y());
			this.setSquid_width(s.getSquid_width());
			this.setSquid_height(s.getSquid_height());
			this.setTransformation_group_x(s.getTransformation_group_x());
			this.setTransformation_group_y(s.getTransformation_group_y());
			this.setSource_transformation_group_x(s.getSource_transformation_group_x());
			this.setSource_transformation_group_y(s.getSource_transformation_group_y());
			this.setIs_show_all(s.isIs_show_all());
			this.setSource_is_show_all(s.isSource_is_show_all());
		}
	}
	
	/**
	 * @return the folder_id
	 */
	public int getFolder_id() {
		return folder_id;
	}
	/**
	 * @param folder_id the folder_id to set
	 */
	public void setFolder_id(int folder_id) {
		this.folder_id = folder_id;
	}
	/**
	 * @return the email_receivers
	 */
	public String getEmail_receivers() {
		return email_receivers;
	}
	/**
	 * @param email_receivers the email_receivers to set
	 */
	public void setEmail_receivers(String email_receivers) {
		this.email_receivers = email_receivers;
	}

	/**
	 * @return the report_name
	 */
	public String getReport_name() {
		return report_name;
	}

	/**
	 * @param report_name the report_name to set
	 */
	public void setReport_name(String report_name) {
		this.report_name = report_name;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}
	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the report_template
	 */
	public String getReport_template() {
		return report_template;
	}

	/**
	 * @param report_template the report_template to set
	 */
	public void setReport_template(String report_template) {
		this.report_template = report_template;
	}

	/**
	 * @return the max_history_count
	 */
	public int getMax_history_count() {
		return max_history_count;
	}

	/**
	 * @param max_history_count the max_history_count to set
	 */
	public void setMax_history_count(int max_history_count) {
		this.max_history_count = max_history_count;
	}

	/**
	 * @return the email_title
	 */
	public String getEmail_title() {
		return email_title;
	}

	/**
	 * @param email_title the email_title to set
	 */
	public void setEmail_title(String email_title) {
		this.email_title = email_title;
	}

	/**
	 * @return the is_real_time
	 */
	public boolean isIs_real_time() {
		return is_real_time;
	}

	/**
	 * @param is_real_time the is_real_time to set
	 */
	public void setIs_real_time(boolean is_real_time) {
		this.is_real_time = is_real_time;
	}

	/**
	 * @return the email_report_format
	 */
	public int getEmail_report_format() {
		return email_report_format;
	}

	/**
	 * @param email_report_format the email_report_format to set
	 */
	public void setEmail_report_format(int email_report_format) {
		this.email_report_format = email_report_format;
	}

	/**
	 * @return the is_support_history
	 */
	public boolean isIs_support_history() {
		return is_support_history;
	}

	/**
	 * @param is_support_history the is_support_history to set
	 */
	public void setIs_support_history(boolean is_support_history) {
		this.is_support_history = is_support_history;
	}

	/**
	 * @return the is_send_email
	 */
	public boolean isIs_send_email() {
		return is_send_email;
	}

	/**
	 * @param is_send_email the is_send_email to set
	 */
	public void setIs_send_email(boolean is_send_email) {
		this.is_send_email = is_send_email;
	}

	/**
	 * @return the is_packed
	 */
	public boolean isIs_packed() {
		return is_packed;
	}

	/**
	 * @param is_packed the is_packed to set
	 */
	public void setIs_packed(boolean is_packed) {
		this.is_packed = is_packed;
	}

	/**
	 * @return the is_compressed
	 */
	public boolean isIs_compressed() {
		return is_compressed;
	}

	/**
	 * @param is_compressed the is_compressed to set
	 */
	public void setIs_compressed(boolean is_compressed) {
		this.is_compressed = is_compressed;
	}

	/**
	 * @return the is_encrypted
	 */
	public boolean isIs_encrypted() {
		return is_encrypted;
	}

	/**
	 * @param is_encrypted the is_encrypted to set
	 */
	public void setIs_encrypted(boolean is_encrypted) {
		this.is_encrypted = is_encrypted;
	}

    public int getRepository_id() {
        return repository_id;
    }

    public void setRepository_id(int repository_id) {
        this.repository_id = repository_id;
    }

	public String getPublishing_path() {
		return publishing_path;
	}

	public void setPublishing_path(String publishing_path) {
		this.publishing_path = publishing_path;
	}
}