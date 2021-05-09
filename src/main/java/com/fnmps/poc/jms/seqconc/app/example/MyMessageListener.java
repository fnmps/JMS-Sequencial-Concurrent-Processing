package com.fnmps.poc.jms.seqconc.app.example;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.JMSException;
import javax.jms.Message;

import com.fnmps.poc.jms.seqconc.app.listeners.AbstractKeySequenceMessageListener;

public class MyMessageListener extends AbstractKeySequenceMessageListener {

	private static final int SIMULATED_TASK_DURATION = 250;
	private List<String> orderOfExecution = new CopyOnWriteArrayList<>();

	public MyMessageListener(int maxNbThreads) {
		super(maxNbThreads);
	}

	@Override
	public void doTask(Message message) {
		try {
			// simulate task to do
			Thread.sleep(SIMULATED_TASK_DURATION);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		try {
			orderOfExecution.add(message.getBody(String.class));
		} catch (JMSException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void onMessageError(Message message, Exception e) {
		e.printStackTrace();
	}

	public List<String> getOrderOfExecution() {
		return orderOfExecution;
	}

}
