package com.eurlanda.datashire.utility;

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FilenameFilter;

public class FilterUtil implements FilenameFilter {
    
	private String fileType;// 过滤类型

	public FilterUtil() {

	}

	public FilterUtil(String fileType) {
		//规范化正则
		fileType=fileType.replaceAll("\\\\","\\\\\\\\")
				.replaceAll("\\$","\\\\\\$")
				.replaceAll("\\^","\\\\\\^")
				.replaceAll("\\+","\\\\\\+")
				.replaceAll("\\.","\\\\\\.")
				.replaceAll("\\{","\\\\\\{")
				.replaceAll("\\(","\\\\\\(")
				.replaceAll("\\)","\\\\\\)")
				.replaceAll("\\[","\\\\\\]")
				.replaceAll("\\|","\\\\\\|");
		this.fileType = fileType;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	@Override
	public boolean accept(File dir, String fileName) {

		return this.check(fileName);
	}
	
	//正则表达式截取指定的文件类型
	public boolean fileTypeMatch(String fileName) {
		boolean flag = false;
		if (StringUtils.isBlank(fileType)) {
			flag = true;
		} else { // 有多种文件类型
			String[] filters = fileType.split(",");
			for(String filter : filters){
				filter = filter.replaceAll("%",".*").replaceAll("\\*",".*").replaceAll("\\?",".");
				if(fileName.matches(filter)){
					flag = true;
					break;
				}
			}
		}
		return flag;
	}

	/**
	 * 校验该文件是否为指定格式文件
	 * 
	 * @param fileName
	 * @return
	 */
	public boolean check(String fileName) {
		return this.fileTypeMatch(fileName);
	}

}
