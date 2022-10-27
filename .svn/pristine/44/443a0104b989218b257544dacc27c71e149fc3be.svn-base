package com.eurlanda.datashire.adapter;

import com.eurlanda.datashire.entity.MetadataNode;

import java.io.File;

/**
 * 文件处理adapter接口
 * @author lei.bin
 *
 */
public interface IFileAdapter {
	/***
	 * 返回当前目录路径
	 */
	public String getDirectory() throws Exception;

	/**
	 * 刷新列表
	 */
	public void refreshList() throws Exception;

	/**
	 * 移动当前文件集合的指针指到下一个条目
	 * 
	 * @return 成功返回true,否则false
	 */
	public boolean nextFile() throws Exception;

	/**
	 * 返回当前指向的文件对象的文件名称
	 */
	public String getFileName() throws Exception;

	/**
	 * 返回当前指向的文件对象的文件尺寸
	 */
	public String getFileSize() throws Exception;

	/**
	 * 返回当前指向的文件对象的最后修改日期
	 */
	public String getFileTimeStamp() throws Exception;

	/**
	 * 返回当前指向的文件对象是否是一个文件目录
	 */
	public boolean isDirectory() throws Exception;

	/**
	 * 返回当前的文件类型
	 */
	public String getFileTypeDesc() throws Exception;

	/**
	 * 读取指定路径的文件内容
	 * 
	 * @return
	 * @throws Exception
	 */
	public String getFileContent(MetadataNode metadataNode) throws Exception;
	/**
	 * 读取指定路径的文件内容
	 * @param fileName
	 * @param filePath
	 * @return
	 * @throws Exception
	 */
	public String getFileContent(String fileName,String filePath,int doc_format) throws Exception;
	/**
	 * 过滤指定文件夹下的指定文件
	 * 
	 * @param file
	 */
	public void listPath(File file) throws Exception;
	/**
	 * 获取文件所在路径
	 * @return
	 * @throws Exception
	 */
	public String getfilePath()throws Exception;
}
