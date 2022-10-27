package com.eurlanda.datashire.utility;

import com.jcraft.jsch.Logger;
/**
 * sftp日志
 * @author lei.bin
 *
 */
public class SettleLogger implements Logger {

	@Override
	public boolean isEnabled(int i) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void log(int i, String s) {
		// TODO Auto-generated method stub
		System.out.println(s);
	}

}
