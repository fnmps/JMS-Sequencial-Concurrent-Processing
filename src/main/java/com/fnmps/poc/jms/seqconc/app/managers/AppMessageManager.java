package com.fnmps.poc.jms.seqconc.app.managers;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.stereotype.Component;

import com.fnmps.poc.jms.seqconc.app.listeners.MySimpleListener;

@Component
public class AppMessageManager {

	private Map<String, SequenceManager> sequenceManagers;
	private Map<String, DefaultMessageListenerContainer> simpleMessageListernContainers;
	
	@Autowired
	private ConnectionFactory connectionFactory;
	
	@PostConstruct
	private void init() throws JMSException {
		sequenceManagers = new HashMap<>();
		sequenceManagers.put("DEV.QUEUE.1", new SequenceManager("DEV.QUEUE.1", connectionFactory));
		simpleMessageListernContainers = new HashMap<>();
		simpleMessageListernContainers.put("DEV.QUEUE.2", createDefaultMessageListenerContainer("DEV.QUEUE.2"));
	}
	
	public DefaultMessageListenerContainer createDefaultMessageListenerContainer(String queueName) {
		DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
	    try {
	    	container.setConnectionFactory(connectionFactory);
	    	container.setDestinationName(queueName);
	    	container.setMessageListener(new MySimpleListener());
	    	container.setSessionTransacted(true);
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
	
	public void stopAll() throws JMSException {
		for(SequenceManager sMgr : sequenceManagers.values()) {
			sMgr.stop();
		}
		for(DefaultMessageListenerContainer container : simpleMessageListernContainers.values()) {
			container.stop();
		}
	}
	
	public void startAll() throws JMSException {
		for(SequenceManager sMgr : sequenceManagers.values()) {
			sMgr.start();
		}
		for(DefaultMessageListenerContainer container : simpleMessageListernContainers.values()) {
			container.start();
		}
	}
	
	@PreDestroy
	public void onShutdown() throws JMSException {
		for(SequenceManager sMgr : sequenceManagers.values()) {
			sMgr.shutdown();
		}
		for(DefaultMessageListenerContainer container : simpleMessageListernContainers.values()) {
			container.shutdown();
		}
	}
}
