package com.fnmps.poc.jms.seqconc.app.model;

import javax.jms.Message;

public class KeyAwareMessage {
	
	private Message message;
	private String key;
	
	public KeyAwareMessage(Message m, String key) {
			this.message = m;
			this.key = key;
	}
	
	public String getKey() {
		return key;
	}
	
	public Message getMessage() {
		return message;
	}

}
