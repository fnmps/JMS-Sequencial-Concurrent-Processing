package com.fnmps.poc.jms.seqconc.app.managers;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import com.fnmps.poc.jms.seqconc.app.listeners.AbstractKeySequenceMessageListener;
import com.fnmps.poc.jms.seqconc.app.model.KeyAwareMessage;
import com.fnmps.poc.jms.seqconc.app.model.MessageKeyExtractor;
import com.fnmps.poc.jms.seqconc.app.model.SessionHolder;

public class SequenceManager {

	private static final Logger LOGGER = Logger.getLogger(SequenceManager.class.getName());

	private ExecutorService mainExecutor;
	private MessageProcessor messageProcessorThread;

	private Connection connection;
	private AbstractKeySequenceMessageListener listener;
	private String queueName;
	private MessageKeyExtractor keyExtractor;

	private List<SessionHolder> sessionPool = new CopyOnWriteArrayList<>();

	public AbstractKeySequenceMessageListener getListener() {
		return listener;
	}

	public SequenceManager(String queueName, ConnectionFactory connectionFactory,
			AbstractKeySequenceMessageListener listener, MessageKeyExtractor keyExtractor) throws JMSException {
		this.queueName = queueName;
		this.connection = connectionFactory.createConnection();
		this.listener = listener;
		this.keyExtractor = keyExtractor;
		start();
	}

	public void start() throws JMSException {
		connection.start();
		mainExecutor = Executors.newSingleThreadExecutor();
		messageProcessorThread = new MessageProcessor(listener);
		mainExecutor.execute(messageProcessorThread);
	}

	public void stop() throws JMSException {
		try {
			messageProcessorThread.shutdown();
			mainExecutor.shutdown();
			mainExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
			connection.stop();
		} catch (InterruptedException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
			Thread.currentThread().interrupt();
		}
	}

	public void shutdown() throws JMSException {
		stop();
		listener.shutdown();
		mainExecutor.shutdownNow();
		for (SessionHolder session : sessionPool) {
			try {
				session.getSession().close();
			} catch (IllegalStateException | JMSException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		sessionPool.clear();
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
	class MessageProcessor extends Thread {

		private AbstractKeySequenceMessageListener messageListener;
		private boolean shouldShutdown;

		public MessageProcessor(AbstractKeySequenceMessageListener messageProcessor) {
			this.messageListener = messageProcessor;
			this.shouldShutdown = false;
		}

		@Override
		public void run() {
			try {
				LOGGER.log(Level.FINEST, "Starting processing messages...");
				while (!(shouldShutdown || Thread.currentThread().isInterrupted())) {
					SessionHolder sessionHolder = getAvailableSession();
					receiveAndProcessMessage(sessionHolder);
				}
				LOGGER.log(Level.FINEST, "No longer processing messages...");
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}

		}

		private void receiveAndProcessMessage(SessionHolder sessionHolder) throws JMSException {
			Session currentSession = sessionHolder.getSession();
			try (MessageConsumer currentConsumer = currentSession
					.createConsumer(currentSession.createQueue(queueName))) {
				Message message = currentConsumer.receive();
				if (message != null) {
					String key = keyExtractor.extractKey(message);
					messageListener.onMessage(new KeyAwareMessage(message, key), sessionHolder);
				}
				currentConsumer.close();
			} catch (JMSException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
				if (!e.getErrorCode().equals("JMSCC0020")) {
					currentSession.rollback();
				}
			}
		}

		public void shutdown() {
			shouldShutdown = true;
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
		private synchronized SessionHolder getAvailableSession() throws JMSException {
			SessionHolder result = null;
			for (SessionHolder sessionHolder : sessionPool) {
				if (sessionHolder.isAvailable()) {
					result = sessionHolder;
				}
			}

			if (result == null) {
				LOGGER.info("No session available! Creating new session...");
				Session session = connection.createSession(Session.SESSION_TRANSACTED);
				result = new SessionHolder(session);
				result.setAvailable(false);
				LOGGER.log(Level.INFO, "Session created! Number of sessions is {0}", sessionPool.size());
			} else {
				LOGGER.info("Reusing existing session...");
				result.setAvailable(false);
				sessionPool.add(result);
			}
			sessionPool.add(result);
			return result;
		}
	}
}
