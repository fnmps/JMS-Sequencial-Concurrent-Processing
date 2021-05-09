package com.fnmps.poc.jms.seqconc.app.model;

import javax.jms.Session;

public class MySessionHolder {
	private Session session;
	private boolean isAvailable = true;

	public MySessionHolder(Session session) {
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
