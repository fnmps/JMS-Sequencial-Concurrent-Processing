package com.fnmps.poc.jms.seqconc.app.example;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

public class MySimpleListener implements MessageListener {

	private static final long SIMULATED_TASK_DURATION = 250;
	
	private List<String> orderOfExecution = new CopyOnWriteArrayList<>();
	
	@Override
	public void onMessage(Message message) {
		try {
			String m = message.getBody(String.class);
			doTask(m);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	private void doTask(String message) {
		try {
			Thread.sleep(SIMULATED_TASK_DURATION);
			orderOfExecution.add(message);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public List<String> getOrderOfExecution() {
		return orderOfExecution;
	}

}
