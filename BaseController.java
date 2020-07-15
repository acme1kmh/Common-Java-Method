package com.lguplus.vr.admin.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class BaseController {
	public Logger logger = LoggerFactory.getLogger(this.getClass());
	
	protected ResponseEntity<?> success() {
		return new ResponseEntity<>(HttpStatus.OK);
	}
	
	protected <T> ResponseEntity<?> success(T obj) {
		ResponseEntity<?> response = new ResponseEntity<>(obj, HttpStatus.OK);
		
		return response;
	}
}
