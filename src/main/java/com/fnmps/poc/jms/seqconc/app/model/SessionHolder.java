package com.fnmps.poc.jms.seqconc.app.model;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

public class SessionHolder {
	private Connection connection;
	private Session session;
	private boolean isAvailable = true;

	public SessionHolder(Connection connection) {
		this.connection = connection;
	}

	public Connection getConnection() {
		return connection;
	}

	public Session getSession() throws JMSException {
		if (session == null) {
			this.connection.start();
			this.session = connection.createSession(Session.SESSION_TRANSACTED);
		}
		return session;
	}

	public boolean isAvailable() {
		return isAvailable;
	}

	public void setAvailable(boolean isAvailable) {
		this.isAvailable = isAvailable;
	}

}
