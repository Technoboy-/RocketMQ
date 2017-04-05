package com.alibaba.rocketmq.common.message;

import java.io.Serializable;

public class EchoMessage extends Message implements Serializable {

	private static final long serialVersionUID = 1L;

	public EchoMessage() {
		//NOP
	}

	public EchoMessage(String topic, byte[] body) {
		super(topic, body);
	}
}
