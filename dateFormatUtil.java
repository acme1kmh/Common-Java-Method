package com.lguplus.vr.admin.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class dateFormatUtil {

	
	/**
	 * String 형식 date format 입력 시 호출 시점의 날짜 생성
	 * @param dateFormat
	 * @return
	 */
	public String getNowDate(String dateFormat){
		return new SimpleDateFormat(dateFormat).format(new Date()).toString();
	}
	
}