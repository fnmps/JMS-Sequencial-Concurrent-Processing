package com.fnmps.poc.jms.seqconc.app.model;

import javax.jms.JMSException;
import javax.jms.Message;

public class MyMessage {
	private String key;
	private String messageStr;
	private Message message;

	public MyMessage() {
	}

	public MyMessage(Message m) {
		try {
			messageStr = m.getBody(String.class);
			String[] s = messageStr.split("\\.");
			key = s[0];
			message = m;
		} catch (JMSException e) {
			e.printStackTrace();
		}

	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getMessageStr() {
		return messageStr;
	}

	public void setMessageStr(String messageStr) {
		this.messageStr = messageStr;
	}

	public Message getMessage() {
		return message;
	}

	public void setMessage(Message message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return messageStr;
	}

}
