package com.alibaba.rocketmq.client.echo;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.EchoMessage;
import com.alibaba.rocketmq.common.message.Message;

public class EchoService {
	
	private final Logger log = ClientLogger.getLog();

	protected DefaultMQProducer sendProducer;
	private String producerGroup;
	private String topic; // Topic
	private String namesrvAddr; // NameSrv 的地址
	
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("echoService"));
	
	private EchoErrorHandler handler;
	
	private long initialDelay; 
	private long period; 
	private long threshold = 20;
	
	private final EchoBean echoBean = new EchoBean();
	
	
	public EchoService(){
		//
	}

	public void start() {
		if(UtilAll.isBlank(this.getTopic())){
			throw new RuntimeException("topic is null, please specify a unique echo topic for each broker");
		}
		if(UtilAll.isBlank(producerGroup) || UtilAll.isBlank(namesrvAddr)){
			throw new RuntimeException("producerGroup and namesrvAddr should not be null");
		}
		sendProducer = new DefaultMQProducer(this.producerGroup);
        sendProducer.setNamesrvAddr(namesrvAddr);
        try {
			sendProducer.start();
			log.info("sendProducer started!");
		} catch (MQClientException e) {
			log.error("sendProducer start error.", e);
			throw new RuntimeException(e);
		}
        executor.scheduleAtFixedRate(new EchoTask(), getInitialDelay() == 0 ? 0 : getInitialDelay(), getPeriod() == 0 ? 5 : getPeriod(), TimeUnit.SECONDS);
	}
	
	public void close(){
		this.sendProducer.shutdown();
		this.executor.shutdown();
	}
	
	
	class EchoTask implements Runnable{

		public void run() {
			boolean echo = echo();
			if(echo){
				echoBean.setLastUpdateTime(System.currentTimeMillis());
			} else{
				if(System.currentTimeMillis() - echoBean.getLastUpdateTime() > TimeUnit.SECONDS.toMillis(threshold)){
					echoBean.setLastUpdateTime(System.currentTimeMillis());
					if(handler == null){
						handler = new LogEchoErrorHandler();
					}
					handler.handle();
				}
			}
		}
	}
	
	class EchoBean {
		private long lastUpdateTime;
		
		public EchoBean(){
			this.lastUpdateTime = System.currentTimeMillis();
		}

		public long getLastUpdateTime() {
			return lastUpdateTime;
		}

		public void setLastUpdateTime(long lastUpdateTime) {
			this.lastUpdateTime = lastUpdateTime;
		}
	}
	
	private boolean echo(){
    	try {
    		byte[] body = "echo".getBytes();
    		Message message = new EchoMessage(this.getTopic(), body);
    		SendResult result = sendProducer.send(message);
    		return "echoMsgId".equalsIgnoreCase(result.getOffsetMsgId());
    	} catch (Exception e) {
    		log.error("echo error {}", e);
            return false;
        }
    }
	
	
	public long getThreshold() {
		return threshold;
	}

	public void setThreshold(long threshold) {
		this.threshold = threshold;
	}

	public long getInitialDelay() {
		return initialDelay;
	}

	public void setInitialDelay(long initialDelay) {
		this.initialDelay = initialDelay;
	}

	public long getPeriod() {
		return period;
	}

	public void setPeriod(long period) {
		this.period = period;
	}

	public EchoErrorHandler getHandler() {
		return handler;
	}

	public void setHandler(EchoErrorHandler handler) {
		this.handler = handler;
	}

	public String getProducerGroup() {
		return producerGroup;
	}

	public void setProducerGroup(String producerGroup) {
		this.producerGroup = producerGroup;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getNamesrvAddr() {
		return namesrvAddr;
	}

	public void setNamesrvAddr(String namesrvAddr) {
		this.namesrvAddr = namesrvAddr;
	}
}
