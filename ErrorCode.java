package com.lguplus.vr.admin.common;

public interface ErrorCode {

    public String getErrorCode();

    public default int getResponseCode() {
        return Integer.parseInt(this.getErrorCode().split("-")[1]);
    }
}
