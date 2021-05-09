package com.fnmps.poc.jms.seqconc.app.listeners;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

import javax.jms.JMSException;
import javax.jms.Message;

import com.fnmps.poc.jms.seqconc.app.model.MyMessage;
import com.fnmps.poc.jms.seqconc.app.model.MySessionHolder;

public class KeySequenceMessageListener {

	private static final int MAX_CONCURRENT_EXECUTIONS = 100;

	private static final int SIMULATED_TASK_DURATION = 250;
	private static final int RECHECK_TURN_TIME = 500;

	public List<String> orderExecution = new CopyOnWriteArrayList<>();
	private ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENT_EXECUTIONS);

	private Map<String, ConcurrentLinkedQueue<MyMessage>> waitingToBeProcessed = new HashMap<>();
	private Semaphore semaphore = new Semaphore(MAX_CONCURRENT_EXECUTIONS);

	public void onMessage(Message message, MySessionHolder session) {
		MyMessage myMessage = new MyMessage(message);
		System.out.println("Received message " + myMessage + "...");
		// if already max number of executions, wait for one to end
		semaphore.acquireUninterruptibly();
		ConcurrentLinkedQueue<MyMessage> queue = getInternalQueue(myMessage);
		executor.execute(new ExecuteTaskInOrderThread(queue, myMessage, session));
	}

	/**
	 * 
	 * check if a message with the same key is already being processed if yes, then
	 * return the queue it is on if not, create it and return the queue
	 * 
	 * @param message
	 * @return
	 */
	private ConcurrentLinkedQueue<MyMessage> getInternalQueue(final MyMessage message) {
		ConcurrentLinkedQueue<MyMessage> result;

		// Make sure no other thread is adding or removing queues while the updating of
		// the queues is ongoing
		synchronized (waitingToBeProcessed) {
			result = waitingToBeProcessed.get(message.getKey());
			if (result == null) {
				result = new ConcurrentLinkedQueue<>();
				waitingToBeProcessed.put(message.getKey(), result);
			}
			if (!result.contains(message)) {
				result.add(message);
			}
		}
		return result;
	}

	/**
	 * if the message being processed is the first in line then process it, if not,
	 * then it will have to wait and try again later
	 *
	 */
	class ExecuteTaskInOrderThread implements Runnable {

		private ConcurrentLinkedQueue<MyMessage> queue;
		private MyMessage message;
		private MySessionHolder session;

		public ExecuteTaskInOrderThread(ConcurrentLinkedQueue<MyMessage> queue, MyMessage message,
				MySessionHolder session) {
			this.queue = queue;
			this.message = message;
			this.session = session;
		}

		@Override
		public void run() {
			while (!queue.peek().equals(message)) {
				try {
					synchronized (message.getMessage()) {
						message.getMessage().wait(RECHECK_TURN_TIME);
					}
				} catch (InterruptedException e) {
					// ignore and try again
				}
			}
			executeTask(queue, message, session);
		}
	}

	/**
	 * Manage the execution of the task, unblocking the processing of the next
	 * sequenced message and committing and freeing the session
	 * 
	 * @param queue
	 * @param message
	 * @param session
	 */
	private void executeTask(ConcurrentLinkedQueue<MyMessage> queue, MyMessage message, MySessionHolder session) {
		System.out.println("Executing Task for message " + message + "...");
		try {
			doTask(message);
			message.getMessage().acknowledge();
			session.getSession().commit();
			session.setAvailable(true);
			System.out.println("Finished Task for message " + message + "...");
		} catch (JMSException e) {
			e.printStackTrace();
		} finally {
			// Make sure no other thread is adding or removing queues while the updating of
			// the queues is ongoing
			synchronized (waitingToBeProcessed) {
				queue.poll();
				if (queue.isEmpty()) {
					waitingToBeProcessed.remove(message.getKey());
				}
			}
			synchronized (message.getMessage()) {
				message.getMessage().notifyAll();
			}
			semaphore.release();
		}
	}

	/**
	 * Do the needed task for the message
	 * 
	 * @param message
	 */
	public void doTask(MyMessage message) {
		try {
			// simulate task to do
			Thread.sleep(SIMULATED_TASK_DURATION);
		} catch (InterruptedException e) {
		}
		orderExecution.add(message.toString());
	}

}
