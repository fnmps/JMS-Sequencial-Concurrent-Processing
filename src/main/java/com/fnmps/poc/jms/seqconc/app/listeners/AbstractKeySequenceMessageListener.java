package com.fnmps.poc.jms.seqconc.app.listeners;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.JMSException;
import javax.jms.Message;

import com.fnmps.poc.jms.seqconc.app.model.KeyAwareMessage;
import com.fnmps.poc.jms.seqconc.app.model.SessionHolder;

public abstract class AbstractKeySequenceMessageListener {

	private static final Logger LOGGER = Logger.getLogger(AbstractKeySequenceMessageListener.class.getName());

	private Map<String, ConcurrentLinkedQueue<KeyAwareMessage>> waitingToBeProcessed;
	private ThreadPoolExecutor executor;
	private Semaphore semaphore;

	public AbstractKeySequenceMessageListener(int maxNbThreads) {
		executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxNbThreads);
		semaphore = new Semaphore(maxNbThreads);
		waitingToBeProcessed = new HashMap<>();
	}

	public final void onMessage(KeyAwareMessage message, SessionHolder session) {
		LOGGER.log(Level.FINE, "Received message {0}...", message);
		// if already max number of executions, wait for one to end
		semaphore.acquireUninterruptibly();
		ConcurrentLinkedQueue<KeyAwareMessage> queue = getInternalQueue(message);
		executor.execute(new ExecuteTaskInOrderThread(queue, message, session));
	}

	public final void shutdown() {
		executor.shutdown();
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
	final class ExecuteTaskInOrderThread implements Runnable {

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
					synchronized (message) {
						message.wait();
					}
				} catch (InterruptedException e) {
					LOGGER.log(Level.FINER, e.getMessage(), e);
					Thread.currentThread().interrupt();
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
		private void executeTask(Queue<KeyAwareMessage> queue, final KeyAwareMessage message, SessionHolder session) {
			try {
				doTask(message.getMessage());
				message.getMessage().acknowledge();
				session.getSession().commit();
				session.setAvailable(true);
			} catch (JMSException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
				onMessageError(message.getMessage(), e);
			} finally {
				// Make sure no other thread is adding or removing queues while the updating of
				// the queues is ongoing
				synchronized (waitingToBeProcessed) {
					LOGGER.log(Level.FINE, "Queue status for key {0}: {1}", new Object[]{message.getKey(), queue.size()});
					queue.poll();
					if (queue.isEmpty()) {
						waitingToBeProcessed.remove(message.getKey());
					}
				}
				KeyAwareMessage nextMessage = queue.peek();
				if(nextMessage != null) {
					synchronized (nextMessage) {
						nextMessage.notifyAll();
					}
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
