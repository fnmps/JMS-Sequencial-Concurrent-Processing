package com.fnmps.poc.jms.seqconc.app.managers;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import com.fnmps.poc.jms.seqconc.app.listeners.KeySequenceMessageListener;
import com.fnmps.poc.jms.seqconc.app.model.MySessionHolder;

public class SequenceManager {

	private ThreadPoolExecutor executor;
	private ExecutorService mainExecutor;
	private MessageProcessor messageProcessorThread;

	private Connection connection;
	private KeySequenceMessageListener listener;
	private String queueName;

	public List<MySessionHolder> sessionPool = new CopyOnWriteArrayList<>();

	public KeySequenceMessageListener getListener() {
		return listener;
	}

	public SequenceManager(String queueName, ConnectionFactory connectionFactory) throws JMSException {
		this.queueName = queueName;
		this.connection = connectionFactory.createConnection();
		this.listener = new KeySequenceMessageListener();
		start();
	}

	public void start() throws JMSException {
		connection.start();
		mainExecutor = Executors.newSingleThreadExecutor();
		executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		messageProcessorThread = new MessageProcessor(listener, executor);
		mainExecutor.execute(messageProcessorThread);
	}

	public void stop() throws JMSException {
		try {
			messageProcessorThread.shutdown();
			mainExecutor.shutdown();
			mainExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
			executor.shutdown();
			executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
			connection.stop();
		} catch (InterruptedException e) {
		}
	}

	public void shutdown() throws JMSException {
		stop();
		executor.shutdownNow();
		mainExecutor.shutdownNow();
		connection.close();
	}

	/**
	 * Receives the messages in a sequential manner. The onMessage of the listener
	 * will return as soon as the next message can be received
	 * 
	 * The next message will use a separate session so the acknowledgment and commit
	 * of the message that is still in processing is not affected
	 *
	 */
	class MessageProcessor implements Runnable {

		private KeySequenceMessageListener messageProcessor;
		private ExecutorService executor;
		private MessageConsumer currentConsumer;
		private Session currentSession;
		volatile boolean shouldShutdown = false;

		public MessageProcessor(KeySequenceMessageListener messageProcessor, ExecutorService executor) {
			this.messageProcessor = messageProcessor;
			this.executor = executor;
		}
		
		@Override
		public void run() {
			try {
				while (!executor.isShutdown() && !shouldShutdown && !Thread.currentThread().isInterrupted()) {
					MySessionHolder sessionHolder = getAvailableSession();
					currentSession = sessionHolder.getSession();
					try {
						currentConsumer = currentSession.createConsumer(currentSession.createQueue(queueName));
						Message message = currentConsumer.receive();
						if (message != null) {
							messageProcessor.onMessage(message, sessionHolder);
						}
						currentConsumer.close();
					} catch (JMSException e) {
						currentSession.rollback();
						currentSession.close();
					}
				}
			} catch (Exception e) {
				try {
					e.printStackTrace();
					shutdown();
				} catch (JMSException e1) {
					e1.printStackTrace();
				}
			}
		}

		public void shutdown() throws JMSException {
			shouldShutdown = true;
			if (currentSession != null) {
				currentSession.close();
			}
			if (currentConsumer != null) {
				currentConsumer.close();
			}
		}
	}

	/**
	 * Fetches an existing unused session If none exists creates it and adds it to
	 * the pool
	 * 
	 * (this should not be needed if application server already has configured a JMS
	 * session pool)
	 * 
	 * @return
	 * @throws JMSException
	 */
	private synchronized MySessionHolder getAvailableSession() throws JMSException {
		MySessionHolder result = null;
		for (MySessionHolder sessionHolder : sessionPool) {
			if (sessionHolder.isAvailable()) {
				result = sessionHolder;
			}
		}

		if (result == null) {
			System.out.println("No session available! Creating new session...");
			Session session = connection.createSession(Session.SESSION_TRANSACTED);
			result = new MySessionHolder(session);
			result.setAvailable(false);
			sessionPool.add(result);
			System.out.println("Session created! Number of sessions is " + sessionPool.size());
		} else {
			System.out.println("Reusing existing session...");
			result.setAvailable(false);
		}
		return result;
	}

}
