package com.eurlanda.datashire.entity.operation;

import java.sql.SQLException;

/**
 * 自定义异常类 (Squid超出了限定的数量)
 * @author yi.zhou
 *
 */
public class BeyondSquidException extends SQLException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BeyondSquidException() {
        super();
    }
    
    public BeyondSquidException(String msg) {
        super(msg);
    }
    
    public BeyondSquidException(String msg, Throwable cause) {
        super(msg, cause);
    }
    
    public BeyondSquidException(Throwable cause) {
        super(cause);
    }
}
