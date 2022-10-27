package com.eurlanda.datashire.utility;

import java.security.MessageDigest;

public class RandomGUID{

	
	public static void main(String[] args) {
		System.out.println(newGuid().length());
	}
	
	public static String newGuid(){
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
		}
		char[] charArray = Long.toString(System.currentTimeMillis()).toCharArray();
		byte[] byteArray = new byte[charArray.length];
		for (int i = 0; i < charArray.length; i++)
			byteArray[i] = (byte) charArray[i];
		byte[] md5Bytes = md5.digest(byteArray);
		StringBuffer hexValue = new StringBuffer(32);
		for (int i = 0; i < md5Bytes.length; i++) {
			int val = ((int) md5Bytes[i]) & 0xff;
			if (val < 16)
				hexValue.append("0");
			hexValue.append(Integer.toHexString(val));
		}
		String valueAfterMD5 = hexValue.toString().toUpperCase();
		
       	return new StringBuffer(36)
	        .append(valueAfterMD5.substring(0, 8))
	        .append("-")
	        .append(valueAfterMD5.substring(8, 12))
	        .append("-")
	        .append(valueAfterMD5.substring(12, 16))
	        .append("-")
	        .append(valueAfterMD5.substring(16, 20))
	        .append("-")
	        .append(valueAfterMD5.substring(20))
	        .toString();
	}
	
    /*
     * Convert to the standard format for GUID
     * (Useful for SQL Server UniqueIdentifiers, etc.)
     * Example: C2FEEEAC-CFCD-11D1-8B05-00600806D9B6
     */
    public String toString() {
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
		}
		char[] charArray = Long.toString(System.currentTimeMillis()).toCharArray();
		byte[] byteArray = new byte[charArray.length];
		for (int i = 0; i < charArray.length; i++)
			byteArray[i] = (byte) charArray[i];
		byte[] md5Bytes = md5.digest(byteArray);
		StringBuffer hexValue = new StringBuffer(32);
		for (int i = 0; i < md5Bytes.length; i++) {
			int val = ((int) md5Bytes[i]) & 0xff;
			if (val < 16)
				hexValue.append("0");
			hexValue.append(Integer.toHexString(val));
		}
		String valueAfterMD5 = hexValue.toString().toUpperCase();
		
       	return new StringBuffer(36)
	        .append(valueAfterMD5.substring(0, 8))
	        .append("-")
	        .append(valueAfterMD5.substring(8, 12))
	        .append("-")
	        .append(valueAfterMD5.substring(12, 16))
	        .append("-")
	        .append(valueAfterMD5.substring(16, 20))
	        .append("-")
	        .append(valueAfterMD5.substring(20))
	        .toString();
    }
    

}
