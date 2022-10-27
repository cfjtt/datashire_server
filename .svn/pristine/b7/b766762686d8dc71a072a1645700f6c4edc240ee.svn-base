package com.eurlanda.datashire.utility;

public class IntUtils {
	/**
	 * （大端） 将int类型的数据转换为byte数组 原理：将int数据中的四个byte取出，分别存储
	 * @param n int数据
	 * @return 生成的byte数组
	 */
	public static byte[] intToBytes(int n) {
		byte[] b = new byte[4];
		for (int i = 0; i < 4; i++) {
			b[i] = (byte) (n >> (24 - i * 8));
		}
		return b;
	}

	// byte[] -> int
	/**
	 * （大端）网络字节 将byte数组转换为int数据
	 * @param b 字节数组
	 * @return 生成的int数据
	 */
	public static int bytesToInt(byte[] b) {
		int firstByte = 0;  
        int secondByte = 0;  
        int thirdByte = 0;  
        int fourthByte = 0;  
        firstByte = (0x000000FF & ((int) b[0]));  
        secondByte = (0x000000FF & ((int) b[1]));  
        thirdByte = (0x000000FF & ((int) b[2]));  
        fourthByte = (0x000000FF & ((int) b[3]));
        long l = ((long) (firstByte << 24 | secondByte << 16 | thirdByte << 8 | fourthByte)) & 0xFFFFFFFFL;
        return Integer.parseInt(l+"");
	}
    
	/**
	 * （小端）32位int转byte[]
	 * @param res
	 * @return
	 */
    public static byte[] int2byte(int res) {  
        byte[] targets = new byte[4];  
        targets[0] = (byte) (res & 0xff);// 最低位  
        targets[1] = (byte) ((res >> 8) & 0xff);// 次低位  
        targets[2] = (byte) ((res >> 16) & 0xff);// 次高位  
        targets[3] = (byte) (res >> 24);// 最高位,无符号右移。  
        return targets;
    }
    
    /**
     * （小端）byte[]转int:32位
     * @param bytes
     * @return
     */
    public static int byte2int(byte[] bytes) {
        int addr = bytes[0] & 0xFF;
        addr |= ((bytes[1] << 8) & 0xFF00);
        addr |= ((bytes[2] << 16) & 0xFF0000);
        addr |= ((bytes[3] << 24) & 0xFF000000);
        return addr;
    }
    
	public static void main(String[] args) {
		byte[] bt = int2byte(1);
		byte[] bt2 = intToBytes(1);
		int c = bytesToInt(bt);
		int n = byte2int(bt);
		
		int c2 = bytesToInt(bt2);
		int n2 = byte2int(bt2);
		
		if ((c2 & 0x1) == 1){
			System.err.println(true);
		}
		System.err.println(c);
	}
}
