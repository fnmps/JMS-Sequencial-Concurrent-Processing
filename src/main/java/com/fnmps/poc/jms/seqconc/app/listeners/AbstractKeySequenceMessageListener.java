package com.fnmps.poc.jms.seqconc.app.listeners;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

import javax.jms.JMSException;
import javax.jms.Message;

import com.fnmps.poc.jms.seqconc.app.model.KeyAwareMessage;
import com.fnmps.poc.jms.seqconc.app.model.SessionHolder;

public abstract class AbstractKeySequenceMessageListener {

	private static final int RECHECK_TURN_TIME = 500;

	private Map<String, ConcurrentLinkedQueue<KeyAwareMessage>> waitingToBeProcessed;
	private ThreadPoolExecutor executor;
	private Semaphore semaphore;

	public AbstractKeySequenceMessageListener(int maxNbThreads) {
		executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxNbThreads);
		semaphore = new Semaphore(maxNbThreads);
		waitingToBeProcessed = new HashMap<>();
	}

	public void onMessage(KeyAwareMessage message, SessionHolder session) {
		System.out.println("Received message " + message + "...");
		// if already max number of executions, wait for one to end
		semaphore.acquireUninterruptibly();
		ConcurrentLinkedQueue<KeyAwareMessage> queue = getInternalQueue(message);
		executor.execute(new ExecuteTaskInOrderThread(queue, message, session));
	}

	/**
	 * 
	 * check if a message with the same key is already being processed if yes, then
	 * return the queue it is on if not, create it and return the queue
	 * 
	 * @param message
	 * @return
	 */
	private ConcurrentLinkedQueue<KeyAwareMessage> getInternalQueue(KeyAwareMessage message) {
		ConcurrentLinkedQueue<KeyAwareMessage> result;

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

		private Queue<KeyAwareMessage> queue;
		private KeyAwareMessage message;
		private SessionHolder session;

		public ExecuteTaskInOrderThread(Queue<KeyAwareMessage> queue, KeyAwareMessage message, SessionHolder session) {
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

		/**
		 * Manage the execution of the task, unblocking the processing of the next
		 * sequenced message and committing and freeing the session
		 * 
		 * @param queue
		 * @param message
		 * @param session
		 */
		private void executeTask(Queue<KeyAwareMessage> queue, KeyAwareMessage message, SessionHolder session) {
			try {
				doTask(message.getMessage());
				message.getMessage().acknowledge();
				session.getSession().commit();
				session.setAvailable(true);
			} catch (JMSException e) {
				onMessageError(message.getMessage(), e);
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
	}

	/**
	 * Perform the needed task for the message once the message turn has been
	 * reached
	 * 
	 * @param message
	 */
	public abstract void doTask(Message message);

	/**
	 * In case of error during message processing and acknowledgment/commit this
	 * method is called before removing message from internal queue
	 * 
	 * @param message
	 * @param e
	 */
	public abstract void onMessageError(Message message, Exception e);

}
