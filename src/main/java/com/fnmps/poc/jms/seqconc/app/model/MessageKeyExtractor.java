package com.fnmps.poc.jms.seqconc.app.model;

import javax.jms.JMSException;
import javax.jms.Message;

public interface MessageKeyExtractor {

	/**
	 * Extracts the key of the Message
	 * 
	 * @param message
	 * @return
	 * @throws JMSException
	 */
	String extractKey(Message message) throws JMSException;
}
