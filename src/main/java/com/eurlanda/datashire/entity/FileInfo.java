package com.eurlanda.datashire.entity;
/**
 * 文件属性
 * @author lei.bin
 *
 */
public class FileInfo {
	private String fileName;// 文件名称
	private String filePath;// 文件路径
	private String fileFilter;// 过滤类型
	private String modifyDate;// 修改时间
	private String fileSize;// 文件大小
	private String fileContent;// 文件内容
	private String fileType;//文件类型
	private boolean is_directory;//是否文件夹

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getFileFilter() {
		return fileFilter;
	}

	public void setFileFilter(String fileFilter) {
		this.fileFilter = fileFilter;
	}

	public String getModifyDate() {
		return modifyDate;
	}

	public void setModifyDate(String modifyDate) {
		this.modifyDate = modifyDate;
	}

	public String getFileSize() {
		return fileSize;
	}

	public void setFileSize(String fileSize) {
		this.fileSize = fileSize;
	}

	public String getFileContent() {
		return fileContent;
	}

	public void setFileContent(String fileContent) {
		this.fileContent = fileContent;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	public boolean isIs_directory() {
		return is_directory;
	}

	public void setIs_directory(boolean is_directory) {
		this.is_directory = is_directory;
	}

    @Override public String toString() {
        return "FileInfo{" +
                "fileName='" + fileName + '\'' +
                ", filePath='" + filePath + '\'' +
                ", fileFilter='" + fileFilter + '\'' +
                ", modifyDate='" + modifyDate + '\'' +
                ", fileSize='" + fileSize + '\'' +
                ", fileContent='" + fileContent + '\'' +
                ", fileType='" + fileType + '\'' +
                ", is_directory=" + is_directory +
                '}';
    }
}
