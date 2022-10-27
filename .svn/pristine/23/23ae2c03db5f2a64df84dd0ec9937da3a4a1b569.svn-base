package com.eurlanda.datashire.utility;

import java.text.MessageFormat;

/**
 * 枚举异常类
 * @author Fumin
 *
 */
public class EnumException extends Exception {
    private static final long serialVersionUID = 4511943699562765447L;

	public EnumException() {
		super();
	}
	
	public EnumException(String message) {
		super(message);
	}

	public EnumException(Throwable cause) {
		super(cause);
	}
	
	public EnumException(String message, Throwable cause) {
		super(message, cause);
	}
	
    public EnumException(String pattern, Object... arguments) {
        super(MessageFormat.format(pattern, arguments));
    }
 
    public EnumException(Throwable cause, String pattern, Object... arguments) {
        super(MessageFormat.format(pattern, arguments), cause);
    }
    
    /*
     * MessageFormat.format性能貌似优于String.format
     * MessageFormat.format("first:{0},second:{1},third:{2}.", new String[]{"a","b","c"});
     * String.format("first:%s,second:%s,third:%s.", new String[]{"a","b","c"});
     */
}
