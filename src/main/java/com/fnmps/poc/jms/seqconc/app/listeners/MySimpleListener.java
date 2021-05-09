package com.fnmps.poc.jms.seqconc.app.listeners;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

public class MySimpleListener implements MessageListener {

	private static final long SIMULATED_TASK_DURATION = 250;
	@Override
	public void onMessage(Message message) {
		try {
			String m = message.getBody(String.class);
			System.out.println("Received message " + m + "...");
			doTask(m);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	private void doTask(String message) {
		System.out.println("Performing Task for message " + message + "...");
		try {
			Thread.sleep(SIMULATED_TASK_DURATION);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Ended Task for message " + message + "!");
	}

}
