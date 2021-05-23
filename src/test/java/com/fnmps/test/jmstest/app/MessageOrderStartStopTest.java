package com.fnmps.test.jmstest.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import com.fnmps.poc.jms.seqconc.app.example.MyMessageKeyExtractor;
import com.fnmps.poc.jms.seqconc.app.example.MyMessageListener;
import com.fnmps.poc.jms.seqconc.app.managers.SequenceManager;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

public class MessageOrderStartStopTest {

	private static MQConnectionFactory createFactory() throws JMSException {
		MQConnectionFactory factory = new MQConnectionFactory();
		factory.setHostName("localhost");
		factory.setPort(1414);
		factory.setQueueManager("QM1");
		factory.setChannel("DEV.APP.SVRCONN");
		factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
		return factory;
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
				MessageLoader2.class.getClassLoader().getResource("logging.properties").getPath());
		MQConnectionFactory factory = createFactory();
		Connection connection = factory.createConnection();
		Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

		SequenceManager seqConcManager = createSequenceManager(factory, "DEV.QUEUE.2", 100);

		MyMessageListener seqConcListener = (MyMessageListener) seqConcManager.getListener();

		System.out.println("\nStarting test (1 000)...");
		performTest(session, seqConcManager, seqConcListener, "DEV.QUEUE.2", 100, 10);

		validateOrder(seqConcListener.getOrderOfExecution());

		seqConcManager.stop();
		seqConcManager.shutdown();
	}

	private static void validateOrder(List<String> orderOfExecution) {
		Map<String, ArrayList<String>> sequences = getSequencing(orderOfExecution);
		String sequence = "";
		for (int i = 0; i < sequences.size(); i++) {
			sequence += Arrays.toString(sequences.get(Integer.toString(i)).toArray());
		}
		boolean orderOk = true;
		System.out.println(sequence);
		for (Entry<String, ArrayList<String>> sequenceEntry : sequences.entrySet()) {
			int previousVersion = -1;
			for (String msg : sequenceEntry.getValue()) {
				String[] split = msg.split("\\.");
				int curVersion = Integer.parseInt(split[1]);
				if (previousVersion > curVersion) {
					System.err.println("msg order wrong for key " + sequenceEntry.getKey() + " : " + previousVersion
							+ " > " + curVersion);
					orderOk = false;
				}
				previousVersion = curVersion;
			}
		}
		if(orderOk) {
			System.out.println("Order is ok!");
		}

	}

	private static Map<String, ArrayList<String>> getSequencing(List<String> executionOrder) {
		Map<String, ArrayList<String>> sequences = new HashMap<String, ArrayList<String>>();
		for (String msg : executionOrder) {
			String[] split = msg.split("\\.");
			String key = split[0];
			ArrayList<String> lst = sequences.getOrDefault(key, new ArrayList<>());
			lst.add(msg);
			sequences.putIfAbsent(key, lst);
		}

		return sequences;
	}

	private static void performTest(Session session, SequenceManager seqConcManager, MyMessageListener listener,
			String queueName, int nbrMessageKeys, int nbrVersions) throws JMSException, InterruptedException {
		seqConcManager.stop();
		System.out.println("Filling queue...");
		fillQueue(session, queueName, nbrMessageKeys, nbrVersions);
		System.out.println("Queue filled...");
		Set<String> tmpSet = new LinkedHashSet<>(listener.getOrderOfExecution());
		while (tmpSet.size() < (nbrMessageKeys * nbrVersions)) {
			System.out.println("Starting...");
			seqConcManager.start();
			Thread.sleep(1000);
			System.out.println("Stopping...");
			tmpSet = new LinkedHashSet<>(listener.getOrderOfExecution());
			seqConcManager.stop();
			Thread.sleep(1000);
			System.out.println(tmpSet.size() + " messages processed...");
			tmpSet = new LinkedHashSet<>(listener.getOrderOfExecution());
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
