package com.fnmps.test.jmstest.app;

import java.util.Collections;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

public class MessageLoader {

	public static final int NUMBER_OF_MESSAGE_KEYS = 20;
	public static final int NUMBER_OF_MESSAGE_VERSION_PER_KEY = 5;

	public static void main(String[] args) throws JMSException, InterruptedException {
		MQConnectionFactory factory = new MQConnectionFactory();
		factory.setHostName("localhost");
		factory.setPort(1414);
		factory.setQueueManager("QM1");
		factory.setChannel("DEV.APP.SVRCONN");
		factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
		Connection connection = factory.createConnection();
		Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
		
		long elapsedTime = fillAndWaitProcessing(session, "DEV.QUEUE.1");
		
		System.out.println("(Concurrent) All messages processed in " + elapsedTime + "ms");

		elapsedTime = fillAndWaitProcessing(session, "DEV.QUEUE.2");
		System.out.println("(Simple) All messages processed in " + elapsedTime + "ms");
	}

	private static long fillAndWaitProcessing(Session session, String queueName) throws JMSException, InterruptedException {
		long start = System.currentTimeMillis();
		for (int i = 0; i < NUMBER_OF_MESSAGE_KEYS; i++) {
			for (int j = 0; j < NUMBER_OF_MESSAGE_VERSION_PER_KEY; j++) {
				MessageProducer producer = session.createProducer(session.createQueue(queueName));
				Message m = session.createTextMessage(i + "." + j);
				producer.send(m);
				session.commit();
				producer.close();
			}
		}

		while (getMessageCount(session, queueName) > 0) {
			Thread.sleep(100);
		}
		long finish = System.currentTimeMillis();
		return finish - start;
		
	}

	@SuppressWarnings("unchecked")
	public static int getMessageCount(Session session, String queueName) throws JMSException {
		QueueBrowser browser = session.createBrowser(session.createQueue(queueName));
		return Collections.list(browser.getEnumeration()).size();
	}

}
