package com.eurlanda.datashire.utility;

import cn.com.jsoft.jframe.utils.FileUtils;
import cn.com.jsoft.jframe.utils.StringUtils;

import java.io.File;
import java.io.FileFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * 
 * 从现有的类中创建接口
 * 
 */
public class CreateInterface {
	public static void main(String[] args) {
		String packageName="com.eurlanda.datashire.sprint7.service.user";
		//String fileName="PermissionService.java";
		File binDir = new File(CreateInterface.class.getResource("/").getFile());
		File srcDir = new File(binDir.getParent(),"src");
		File workDir = new File(srcDir,packageName.replace(".", "/"));
		File[] files = workDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.isFile() && !pathname.getName().startsWith("I");
			}
		});
		for(File srcFile :files){
			File interfaceFile = createInterface(srcFile,packageName);
			modifySrcFile(srcFile);
			System.out.println("created interface:"+interfaceFile);
		}
		
		
	}
	/**
	 * 创建接口
	 */
	public static File createInterface(File srcFile,String packageName){
		
		String fileName = srcFile.getName();
		
		String content =StringUtils.toString(srcFile,"utf-8");
		content = content.replaceAll("\n", "~");
		
		Matcher matcher = Pattern.compile("(/\\*\\*[^/]*?\\*/).*?public class").matcher(content);
		String strDesc ="";
		if(matcher.find()){
			strDesc= matcher.group(1).replaceAll("~", "\n");
		}
		String iterface ="package "+packageName+";\n";
		iterface+=strDesc+"\n";
		iterface += "public interface I"+fileName.replace(".java","") +"{\n";
		
		
		Pattern pattern = Pattern.compile("(/\\*\\*[^/]*?\\*/)[~\t ]+public[^\\{]*?\\(.*?\\)");
		Matcher ma =pattern.matcher(content);
		while(ma.find()){
			iterface+=ma.group().replaceAll("~", "\n")+";\n\t";
		}
		iterface+="\n}";
		File targetFile = new File(srcFile.getParent(),"/I"+fileName);
		FileUtils.writeFile(targetFile.getPath(), iterface);
		return targetFile;
	}
	
	public static void modifySrcFile(File srcFile){
		String content = StringUtils.toString(srcFile);
		if(content.indexOf("implements")<0){
			content=content.replaceFirst("class([^\\{]*?)\\{", "class$1implements I"+srcFile.getName().replace(".java", "")+"{");
		}else{
			content=content.replaceFirst("class([^\\{]*?)implements([^\\{]*?)\\{", "class $1 implements $2,I"+srcFile.getName().replace(".java", "")+"{");
		}
		FileUtils.writeFile(srcFile.getPath(), content);
	}
	
}
