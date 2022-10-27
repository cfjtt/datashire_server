package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;

import java.sql.Types;

;

/**
 * 消息气泡 后台根据具体数据校验情况发送成功/失败消息气泡 (以list集合形式)
 * 
 * @author dang.lu 2013.11.22
 * 
 */
public class MessageBubble {

    private int squid_id;

    private int child_id;

    private String name;

    @ColumnMpping(name = "squid_key", desc = "Squid的key", nullable = true, precision = 36, type = Types.VARCHAR, valueReg = "")
    private String squidKey;

    @ColumnMpping(name = "child_key", desc = "Key子对象（如果没有，则跟squidKey填一样）", nullable = true, precision = 36, type = Types.VARCHAR, valueReg = "")
    private String key;

    @ColumnMpping(name = "bubble_code", desc = "错误码", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int bubble_code;

    @ColumnMpping(name = "succeed", desc = "状态(成功/失败)", nullable = true, precision = 1, type = Types.VARCHAR, valueReg = "")
    private boolean status;

    @ColumnMpping(name = "level", desc = "级别(0：WARN, 1：ERROR)", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int level;

    @ColumnMpping(name = "text", desc = "消息内容", nullable = true, precision = 500, type = Types.VARCHAR, valueReg = "")
    private String text;

    public MessageBubble() {
    }

    public MessageBubble(String squidKey, String key, int bubble_code, boolean status,
            int level) {
        this.squidKey = squidKey;
        this.key = key;
        this.bubble_code = bubble_code;
        this.status = status;
        this.level = level;
    }

    public MessageBubble(String squidKey, String key, int bubble_code, boolean status) {
        this.squidKey = squidKey;
        this.key = key;
        this.bubble_code = bubble_code;
        this.status = status;
        this.level = MessageBubbleCode.level(this.bubble_code);
    }

    public MessageBubble(String squidKey, String key, int bubble_code, boolean status,
            String text) {
        this.squidKey = squidKey;
        this.key = key;
        this.bubble_code = bubble_code;
        this.status = status;
        this.level = MessageBubbleCode.level(this.bubble_code);
        this.text = text;
    }

    public MessageBubble(boolean status, int squidId, int childId, String name, int bubble_code, int level,
            String text) {
        this.status = status;
        this.squid_id = squidId;
        this.child_id = childId;
        this.name = name;
        this.bubble_code = bubble_code;
        this.level = level;
        this.text = text;
    }

    public MessageBubble(int squidId, int childId, String name, int bubble_code) {
        this.squid_id = squidId;
        this.child_id = childId;
        this.name = name;
        this.bubble_code = bubble_code;
    }
    
    public String getSquidKey() {
        return squidKey;
    }

    public void setSquidKey(String squidKey) {
        this.squidKey = squidKey;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    @Override
    public String toString() {
        return "MessageBubble [squidKey=" + squidKey + ", key=" + key
                + ", bubble_code=" + bubble_code + ", status=" + status + ", level=" + level
                + "]";
    }

    public int getSquid_id() {
        return squid_id;
    }

    public void setSquid_id(int squid_id) {
        this.squid_id = squid_id;
    }

    public int getChild_id() {
        return child_id;
    }

    public void setChild_id(int child_id) {
        this.child_id = child_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

	public int getBubble_code() {
		return bubble_code;
	}

	public void setBubble_code(int bubble_code) {
		this.bubble_code = bubble_code;
	}

}
