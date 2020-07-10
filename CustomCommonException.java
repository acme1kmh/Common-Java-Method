package com.lguplus.vr.admin.util;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import com.lguplus.vr.admin.common.ErrorCode;

public class CustomCommonException extends RuntimeException {
	
	private static final long serialVersionUID = -8365782366158736757L;
	
	private ErrorCode errorCode;
    private Object[] errorParams;
    private String message;
    private List<ConcurrentHashMap<String, String>> errDataList;

    public CustomCommonException(ErrorCode errorCode) {
        super();

        this.errorCode = errorCode;
    }
    
    public CustomCommonException(String message) {
    	super();
    	
    	this.message = message;
    }

    public CustomCommonException(String message, ErrorCode errorCode) {
        super();

        this.errorCode = errorCode;
        this.message = message;
    }

    public CustomCommonException(ErrorCode errorCode, String errorParam) {
        super();

        this.errorCode = errorCode;
        this.errorParams = new Object[1];
        errorParams[0] = errorParam;
    }

    public CustomCommonException(ErrorCode errorCode, String[] errorParams) {
        super();

        this.errorCode = errorCode;
        this.errorParams = errorParams;
    }

    public CustomCommonException(Throwable e, ErrorCode errorCode) {
        super(e);

        this.errorCode = errorCode;
    }

    public CustomCommonException(Throwable e, ErrorCode errorCode, String errorParam) {
        super(e);

        this.errorCode = errorCode;
        this.errorParams = new Object[1];

        errorParams[0] = errorParam;
    }

    public CustomCommonException(Throwable e, ErrorCode errorCode, String[] errorParams) {
        super(e);

        this.errorCode = errorCode;
        this.errorParams = errorParams;
    }

    public CustomCommonException(ErrorCode errorCode, List<ConcurrentHashMap<String, String>> errDataList) {
        super();

        this.errorCode = errorCode;
        this.errDataList = errDataList;
    }

    public CustomCommonException(String message, ErrorCode errorCode, List<ConcurrentHashMap<String, String>> errDataList) {
        super();

        this.errorCode = errorCode;
        this.message = message;
        this.errDataList = errDataList;
    }

    public CustomCommonException(ErrorCode errorCode, String errorParam, List<ConcurrentHashMap<String, String>> errDataList) {
        super();

        this.errorCode = errorCode;
        this.errorParams = new Object[1];
        this.errorParams[0] = errorParam;
        this.errDataList = errDataList;
    }

    public CustomCommonException(ErrorCode errorCode, String[] errorParams, List<ConcurrentHashMap<String, String>> errDataList) {
        super();

        this.errorCode = errorCode;
        this.errorParams = errorParams;
        this.errDataList = errDataList;
    }

    public CustomCommonException(Throwable e, ErrorCode errorCode, List<ConcurrentHashMap<String, String>> errDataList) {
        super(e);

        this.errorCode = errorCode;
        this.message = e.getMessage();
        this.errDataList = errDataList;
    }

    public CustomCommonException(Throwable e, ErrorCode errorCode, String errorParam, List<ConcurrentHashMap<String, String>> errDataList) {
        super(e);

        this.errorCode = errorCode;
        this.message = e.getMessage();
        this.errorParams = new Object[1];
        this.errorParams[0] = errorParam;
        this.errDataList = errDataList;
    }

    public CustomCommonException(Throwable e, ErrorCode errorCode, String[] errorParams, List<ConcurrentHashMap<String, String>> errDataList) {
        super(e);

        this.errorCode = errorCode;
        this.errorParams = errorParams;
        this.message = e.getMessage();
        this.errDataList = errDataList;
    }

    public CustomCommonException(CustomCommonException e) {
        super(e);

        this.errorCode = e.getErrorCode();
        this.message = e.getMessage();
        this.errorParams = e.getErrorParams();
        this.errDataList = e.getErrDataList();
    }

    public CustomCommonException(Throwable e) {
        super(e);

        if(e instanceof CustomCommonException) {
            CustomCommonException exception = (CustomCommonException) e;

            this.errorCode = exception.getErrorCode();
            this.message = exception.getMessage();
            this.errorParams = exception.getErrorParams();
            this.errDataList = exception.getErrDataList();
        }
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public Object[] getErrorParams() {
        return errorParams;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<ConcurrentHashMap<String, String>> getErrDataList() {
        return errDataList;
    }

    public void setErrDataList(List<ConcurrentHashMap<String, String>> errDataList) {
        this.errDataList = errDataList;
    }
	
}
