package com.eurlanda.datashire.utility;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class SysConf {
	public SysConf(){}
	private static Properties props = new Properties(); 
	static{
		try {
			props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config/config.properties"));
//			props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config/config_101.properties"));
//			props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config/config_130.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static String getValue(String key){
			return props.getProperty(key);
	}

    public static void updateProperties(String key,String value) {    
        props.setProperty(key, value); 
    }
}
