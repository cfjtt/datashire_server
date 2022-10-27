package com.eurlanda.datashire.socket.protocol;

public class TableMonitor {
	
	private String tableName;
	private boolean isLocked;
	private String lockTreadName;
	private long lockTime;
	private int lockCounts;
	
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public boolean isLocked() {
		return isLocked;
	}
	public void setLocked(boolean isLocked) {
		this.isLocked = isLocked;
	}
	public String getLockTreadName() {
		return lockTreadName;
	}
	public void setLockTreadName(String lockTreadName) {
		this.lockTreadName = lockTreadName;
	}
	public long getLockTime() {
		return lockTime;
	}
	public void setLockTime(long lockTime) {
		this.lockTime = lockTime;
	}
	public int getLockCounts() {
		return lockCounts;
	}
	public void setLockCounts(int lockCounts) {
		this.lockCounts = lockCounts;
	}
	
	@Override
	public String toString() {
		return "TableMonitor-" + lockCounts + " [tableName="
				+ tableName + ", isLocked=" + isLocked + ", lockTreadName="
				+ lockTreadName + ", lockTime=" + lockTime + "]";
	}
	
}
