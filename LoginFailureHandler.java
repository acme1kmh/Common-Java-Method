package com.lguplus.vr.admin.common;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;

import com.lguplus.vr.admin.util.CustomProperties;

public class LoginFailureHandler implements AuthenticationFailureHandler{

	private String loginidname;
	private String loginpwdname;
	private String errormsgname;
	private String defaultFailureUrl;
	
	@Override
	public void onAuthenticationFailure(
			HttpServletRequest req
			, HttpServletResponse res
			, AuthenticationException exception)
			throws IOException, ServletException {
		String username = req.getParameter(loginidname);
		String password = req.getParameter(loginpwdname);
		String errormsg = exception.getMessage();

		if(exception instanceof BadCredentialsException) {
            errormsg = CustomProperties.getMessage("ERRIPC31");
        } else {
        	errormsg = CustomProperties.getMessage("ERRSEE20");
        }
		
		req.setAttribute(loginidname, username);
		req.setAttribute(loginpwdname, password);
		req.setAttribute(errormsgname, errormsg);
		
		req.getRequestDispatcher(defaultFailureUrl).forward(req, res);
		
	}

	public String getLoginidname() {
		return loginidname;
	}

	public void setLoginidname(String loginidname) {
		this.loginidname = loginidname;
	}

	public String getLoginpwdname() {
		return loginpwdname;
	}

	public void setLoginpwdname(String loginpwdname) {
		this.loginpwdname = loginpwdname;
	}

	public String getErrormsgname() {
		return errormsgname;
	}

	public void setErrormsgname(String errormsgname) {
		this.errormsgname = errormsgname;
	}

	public String getDefaultFailureUrl() {
		return defaultFailureUrl;
	}

	public void setDefaultFailureUrl(String defaultFailureUrl) {
		this.defaultFailureUrl = defaultFailureUrl;
	}
}
