package com.fnmps.poc.jms.seqconc.app.example;

import javax.jms.JMSException;
import javax.jms.Message;

import com.fnmps.poc.jms.seqconc.app.model.MessageKeyExtractor;

public class MyMessageKeyExtractor implements MessageKeyExtractor{

	@Override
	public String extractKey(Message message) throws JMSException {
		return message.getBody(String.class).split("\\.")[0];
	}

}
