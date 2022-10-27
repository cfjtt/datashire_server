package com.eurlanda.datashire.server.exception;

/**
 * Created by Haochen.Ye on 6/26/2017.
 * 用来将错误码从深层次传递到API接口层使用的特殊Exception
 */
public class ErrorMessageException extends Exception {

    private int errorCode;

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public ErrorMessageException(){}

    public ErrorMessageException(int errorCode) {
        this.errorCode = errorCode;
    }

    public ErrorMessageException(int errorCode, Exception e){
        super(e);
        errorCode = errorCode;
    }
}
