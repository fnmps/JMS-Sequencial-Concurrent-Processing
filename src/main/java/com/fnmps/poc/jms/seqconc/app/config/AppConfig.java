package com.fnmps.poc.jms.seqconc.app.config;

import javax.jms.ConnectionFactory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

@Configuration
public class AppConfig {

	@Bean
	public ConnectionFactory connectionFactory() {
		MQConnectionFactory factory = new MQConnectionFactory();
		try {
			factory.setHostName("localhost");
			factory.setPort(1414);
			factory.setQueueManager("QM1");
			factory.setChannel("DEV.APP.SVRCONN");
			factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return factory;
	}

}
