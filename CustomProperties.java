package com.lguplus.vr.admin.util;

import java.util.Locale;

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.MessageSource;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
 
public class CustomProperties {
 
    private static MessageSource resources = new ClassPathXmlApplicationContext("config/spring/security-context.xml");
    
    private CustomProperties() {
    }
    
    public static String getMessage(String code){
    	HttpServletRequest request = ((ServletRequestAttributes)RequestContextHolder.getRequestAttributes()).getRequest();
		String getSessionAttr = (String)request.getSession().getAttribute("lang");
		String messageResult = "";
		if("en".equals(getSessionAttr)) {
			messageResult = resources.getMessage(code, null, Locale.ENGLISH);
		}else if("ko".equals(getSessionAttr)){
			messageResult = resources.getMessage(code, null, Locale.KOREAN);
		}else {
			messageResult = resources.getMessage(code, null, Locale.ENGLISH);
		}
		return messageResult;
    }
    /** func add 20200316*/
    public static String getMessage(String code, String args){
    	String[] covArgs = {args};
    	HttpServletRequest request = ((ServletRequestAttributes)RequestContextHolder.getRequestAttributes()).getRequest();
		String getSessionAttr = (String)request.getSession().getAttribute("lang");
		String messageResult = "";
		if("en".equals(getSessionAttr)) {
			messageResult = resources.getMessage(code, covArgs, Locale.ENGLISH); 
		}else if("ko".equals(getSessionAttr)){
			messageResult = resources.getMessage(code, covArgs, Locale.KOREAN); 
		}else {
			messageResult = resources.getMessage(code, covArgs, Locale.ENGLISH); 
		}
        return messageResult;
    }
    
    public static String getMessage(String code, String args[]){
    	HttpServletRequest request = ((ServletRequestAttributes)RequestContextHolder.getRequestAttributes()).getRequest();
		String getSessionAttr = (String)request.getSession().getAttribute("lang");
		String messageResult = "";
		if("en".equals(getSessionAttr)) {
			messageResult = resources.getMessage(code, args, Locale.ENGLISH);
		}else if("ko".equals(getSessionAttr)){
			messageResult = resources.getMessage(code, args, Locale.KOREAN);
		}else {
			messageResult = resources.getMessage(code, args, Locale.ENGLISH);
		}
        return messageResult;
    }
    
 
}