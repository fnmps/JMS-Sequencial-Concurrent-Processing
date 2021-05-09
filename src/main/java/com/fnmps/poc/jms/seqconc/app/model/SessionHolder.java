package com.fnmps.poc.jms.seqconc.app.model;

import javax.jms.Session;

public class SessionHolder {
	private Session session;
	private boolean isAvailable = true;

	public SessionHolder(Session session) {
		this.session = session;
	}

	public Session getSession() {
		return session;
	}

	public boolean isAvailable() {
		return isAvailable;
	}

	public void setAvailable(boolean isAvailable) {
		this.isAvailable = isAvailable;
	}
	
}
