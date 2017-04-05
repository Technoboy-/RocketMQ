package com.alibaba.rocketmq.client.echo;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.log.ClientLogger;

public class LogEchoErrorHandler implements EchoErrorHandler{
	
	private final Logger log = ClientLogger.getLog();

	@Override
	public void handle() {
		log.error("echo check error");
	}

}
