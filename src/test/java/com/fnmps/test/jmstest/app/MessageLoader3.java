package com.fnmps.test.jmstest.app;

import java.util.Collections;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.springframework.jms.listener.DefaultMessageListenerContainer;

import com.fnmps.poc.jms.seqconc.app.example.MyMessageKeyExtractor;
import com.fnmps.poc.jms.seqconc.app.example.MyMessageListener;
import com.fnmps.poc.jms.seqconc.app.example.MySimpleListener;
import com.fnmps.poc.jms.seqconc.app.managers.SequenceManager;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

public class MessageLoader3 {

	private static MQConnectionFactory createFactory() throws JMSException {
		MQConnectionFactory factory = new MQConnectionFactory();
		factory.setHostName("localhost");
		factory.setPort(1414);
		factory.setQueueManager("QM1");
		factory.setChannel("DEV.APP.SVRCONN");
		factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
		return factory;
	}

	public static DefaultMessageListenerContainer createListenerContainer(ConnectionFactory connectionFactory,
			String queueName, String concurrency) {
		MySimpleListener listener = new MySimpleListener();
		DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.setDestinationName(queueName);
		container.setMessageListener(listener);
		container.setSessionTransacted(true);
		container.setConcurrency(concurrency);
		container.initialize();
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	public static SequenceManager createSequenceManager(ConnectionFactory connectionFactory, String queueName,
			int nbThreads) throws JMSException {
		SequenceManager seqMgr = new SequenceManager();
		seqMgr.setConnectionFactory(connectionFactory);
		seqMgr.setQueueName(queueName);
		seqMgr.setListener(new MyMessageListener(nbThreads));
		seqMgr.setKeyExtractor(new MyMessageKeyExtractor());
		seqMgr.start();
		return seqMgr;
	}

	public static void main(String[] args) throws JMSException, InterruptedException {

		System.setProperty("java.util.logging.config.file",
				MessageLoader3.class.getClassLoader().getResource("logging.properties").getPath());
		MQConnectionFactory factory = createFactory();
		Connection connection = factory.createConnection();
		Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

		DefaultMessageListenerContainer seqContainer = createListenerContainer(factory, "DEV.QUEUE.1", "1");
		SequenceManager seqConcManager = createSequenceManager(factory, "DEV.QUEUE.2", 100);
		DefaultMessageListenerContainer concContainer = createListenerContainer(factory, "DEV.QUEUE.3", "1-100");

		MySimpleListener seqListener = (MySimpleListener) seqContainer.getMessageListener();
		MyMessageListener seqConcListener = (MyMessageListener) seqConcManager.getListener();
		MySimpleListener concListener = (MySimpleListener) concContainer.getMessageListener();

		System.out.println("Warming up...");
		performTestSuite(factory, session, null, seqConcListener, concListener, 100, 1); // 10
		seqListener.getOrderOfExecution().clear();
		seqConcListener.getOrderOfExecution().clear();
		concListener.getOrderOfExecution().clear();
		System.out.println("Warming up finished!\n");

//		for (int i = 0; i < 3; i++) {
//			System.out.println("\nStarting Lot " + (i + 1) + "...\n");
		System.out.println("\nStarting test 1 (80)...");
		performTestSuite(factory, session, null, seqConcListener, concListener, 40, 2); // 80
		seqListener.getOrderOfExecution().clear();
		seqConcListener.getOrderOfExecution().clear();
		concListener.getOrderOfExecution().clear();
		System.out.println("\nStarting test 2 (320)...");
		performTestSuite(factory, session, null, seqConcListener, concListener, 80, 4); // 320
		seqListener.getOrderOfExecution().clear();
		seqConcListener.getOrderOfExecution().clear();
		concListener.getOrderOfExecution().clear();
		System.out.println("\nStarting test 3 (1 280)...");
		performTestSuite(factory, session, null, seqConcListener, concListener, 160, 8); // 1 280
		seqListener.getOrderOfExecution().clear();
		seqConcListener.getOrderOfExecution().clear();
		concListener.getOrderOfExecution().clear();
//			System.out.println("\nStarting test 4 (5 120)...");
//			performTestSuite(factory, session, null, seqConcListener, concListener, 320, 16); // 5 120
//			seqListener.getOrderOfExecution().clear();
//			seqConcListener.getOrderOfExecution().clear();
//			concListener.getOrderOfExecution().clear();
//			System.out.println("\nStarting test 5 (11 520)...");
//			performTestSuite(factory, session, null, seqConcListener, concListener, 480, 24); // 11 520
//			seqListener.getOrderOfExecution().clear();
//			seqConcListener.getOrderOfExecution().clear();
//			concListener.getOrderOfExecution().clear();
//		}

		seqContainer.stop();
		seqContainer.shutdown();
		seqConcManager.stop();
		seqConcManager.shutdown();
		concContainer.stop();
		concContainer.shutdown();
	}

	@SuppressWarnings("unchecked")
	public static int getMessageCount(Session session, String queueName) throws JMSException {
		QueueBrowser browser = session.createBrowser(session.createQueue(queueName));
		return Collections.list(browser.getEnumeration()).size();
	}

	private static void performTestSuite(MQConnectionFactory factory, Session session, MySimpleListener seqListener,
			MyMessageListener seqConcListener, MySimpleListener concListener, int nbrMessageKeys,
			int nbrMessageVersions) throws JMSException, InterruptedException {
		long start;
		long finish;
		if (seqListener != null) {
			start = System.currentTimeMillis();
			performTest(session, seqListener, "DEV.QUEUE.1", nbrMessageKeys, nbrMessageVersions);
			finish = System.currentTimeMillis();
			printPerformance("Sequential", nbrMessageKeys * nbrMessageVersions, finish - start);
		}
		if (seqConcListener != null) {
			start = System.currentTimeMillis();
			performTest(session, seqConcListener, "DEV.QUEUE.2", nbrMessageKeys, nbrMessageVersions);
			finish = System.currentTimeMillis();
			printPerformance("Sequential-Concurrent", nbrMessageKeys * nbrMessageVersions, finish - start);
		}
		if (concListener != null) {
			start = System.currentTimeMillis();
			performTest(session, concListener, "DEV.QUEUE.3", nbrMessageKeys, nbrMessageVersions);
			finish = System.currentTimeMillis();
			printPerformance("Concurrent", nbrMessageKeys * nbrMessageVersions, finish - start);
		}
	}

	private static void printPerformance(String type, int nbrMessages, long elapsedTime) {
		System.out.println("(" + type + ") " + nbrMessages + " processed in " + elapsedTime + "ms");
		Double elapsedTimeInSec = elapsedTime / 1000d;
		System.out.println("(" + type + ") " + "Avg. messages processed per second: " + (nbrMessages / elapsedTimeInSec)
				+ " messages");
	}

	private static void performTest(Session session, MySimpleListener listener, String queueName, int nbrMessageKeys,
			int nbrVersions) throws JMSException, InterruptedException {
		fillQueue(session, queueName, nbrMessageKeys, nbrVersions);
		while (listener.getOrderOfExecution().size() < (nbrMessageKeys * nbrVersions)) {
			Thread.sleep(100);
		}

	}

	private static void performTest(Session session, MyMessageListener listener, String queueName, int nbrMessageKeys,
			int nbrVersions) throws JMSException, InterruptedException {
		fillQueue(session, queueName, nbrMessageKeys, nbrVersions);
		while (listener.getOrderOfExecution().size() < (nbrMessageKeys * nbrVersions)) {
			Thread.sleep(100);
		}
	}

	private static void fillQueue(Session session, String queueName, int nbrMessageKeys, int nbrVersions)
			throws JMSException {
		for (int j = 0; j < nbrVersions; j++) {
			for (int i = 0; i < nbrMessageKeys; i++) {
				MessageProducer producer = session.createProducer(session.createQueue(queueName));
				Message m = session.createTextMessage(i + "." + j);
				producer.send(m);
				session.commit();
				producer.close();
			}
		}
	}
}
