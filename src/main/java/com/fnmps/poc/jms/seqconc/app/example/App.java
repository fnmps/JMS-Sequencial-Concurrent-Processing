package com.fnmps.poc.jms.seqconc.app.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.fnmps.poc.jms.seqconc.app.managers.SequenceManager;

@RestController
public class App {

	@Autowired
	private AppMessageManager appMessageManager;

	@GetMapping("/ExecutionOrder")
	public String getExecutionOrder(@RequestParam String queueName) {
		SequenceManager sMgr = appMessageManager.getSequenceManager(queueName);
		DefaultMessageListenerContainer container = appMessageManager.getListenerContainer(queueName);
		if (sMgr == null && container == null) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unable to find resource");
		}
		if(sMgr != null) {
			return getSequencing(((MyMessageListener)sMgr.getListener()).getOrderOfExecution());
		} else {
			return getSequencing(((MySimpleListener)container.getMessageListener()).getOrderOfExecution());
		}
	}

	@GetMapping("/stop")
	public String stopListners() throws JMSException {
		appMessageManager.stopAll();
		return "Stopped!";
	}

	@GetMapping("/start")
	public String startListners() throws JMSException {
		appMessageManager.startAll();
		return "Started!";
	}

	private String getSequencing(List<String> executionOrder) {
		Map<String, ArrayList<String>> sequences = new HashMap<String, ArrayList<String>>();
		for(String msg : executionOrder) {
			String[] split = msg.split("\\.");
			String key = split[0];
			ArrayList<String> lst = sequences.getOrDefault(key, new ArrayList<>());
			lst.add(msg);
			sequences.putIfAbsent(key, lst);
		}
		
		String sequence = "";
		for(int i = 0; i < sequences.size(); i++) {
			sequence += Arrays.toString(sequences.get(Integer.toString(i)).toArray());
		}
		
		return sequence;
	}
}
