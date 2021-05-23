package com.fnmps.poc.jms.seqconc.app.example;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.stereotype.Component;

import com.fnmps.poc.jms.seqconc.app.managers.SequenceManager;

@Component
public class AppMessageManager {

	private Map<String, SequenceManager> sequenceManagers;
	private Map<String, DefaultMessageListenerContainer> springListenerContainers;

	@Autowired
	private ConnectionFactory connectionFactory;

	@PostConstruct
	private void init() {
		sequenceManagers = new HashMap<>();
		sequenceManagers.put("DEV.QUEUE.1", createSequenceManager("DEV.QUEUE.1"));
		springListenerContainers = new HashMap<>();
		springListenerContainers.put("DEV.QUEUE.2", createListenerContainer("DEV.QUEUE.2", "1-1"));
		springListenerContainers.put("DEV.QUEUE.3", createListenerContainer("DEV.QUEUE.3", "5-10"));
	}
	
	public SequenceManager createSequenceManager(String queueName) {
		SequenceManager seqMgr = new SequenceManager();
		try {
			seqMgr.setConnectionFactory(connectionFactory);
			seqMgr.setQueueName(queueName);
			seqMgr.setListener(new MyMessageListener(100));
			seqMgr.setKeyExtractor(new MyMessageKeyExtractor());
			seqMgr.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return seqMgr;
	}

	public DefaultMessageListenerContainer createListenerContainer(String queueName, String concurrency) {
		DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
		try {
			container.setConnectionFactory(connectionFactory);
			container.setDestinationName(queueName);
			container.setMessageListener(new MySimpleListener());
			container.setSessionTransacted(true);
			container.setConcurrency(concurrency);
			container.initialize();
			container.afterPropertiesSet();
			container.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return container;
	}

	public SequenceManager getSequenceManager(String queueName) {
		return sequenceManagers.get(queueName);
	}
	
	public DefaultMessageListenerContainer getListenerContainer(String queueName) {
		return springListenerContainers.get(queueName);
	}

	public void stopAll() throws JMSException {
		for (SequenceManager sMgr : sequenceManagers.values()) {
			sMgr.stop();
		}
		for (DefaultMessageListenerContainer container : springListenerContainers.values()) {
			container.stop();
		}
	}

	public void startAll() throws JMSException {
		for (SequenceManager sMgr : sequenceManagers.values()) {
			sMgr.start();
		}
		for (DefaultMessageListenerContainer container : springListenerContainers.values()) {
			container.start();
		}
	}

	@PreDestroy
	public void onShutdown() throws JMSException {
		for (SequenceManager sMgr : sequenceManagers.values()) {
			sMgr.shutdown();
		}
		for (DefaultMessageListenerContainer container : springListenerContainers.values()) {
			container.shutdown();
		}
	}
}
