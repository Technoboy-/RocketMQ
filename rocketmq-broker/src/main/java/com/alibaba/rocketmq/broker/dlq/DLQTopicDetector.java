package com.alibaba.rocketmq.broker.dlq;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.store.ConsumeQueue;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;
import com.alibaba.rocketmq.store.config.StorePathConfigHelper;

public class DLQTopicDetector{
	
	private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
	
	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DLQTopicDetector"));
	
	public static final int INTERVAL = 30;

	public static final String DLQ_TOPIC_PREFIX = "%DLQ%";
	
	private BrokerController brokerController;
	
	private DefaultMessageStore dms;
	
	public DLQTopicDetector(BrokerController brokerController){
		this.brokerController = brokerController;
		this.dms = (DefaultMessageStore)this.brokerController.getMessageStore();
	}
	
	public void start(){
		this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				detect();
			}
		}, 1, INTERVAL, TimeUnit.MINUTES);
	}
	
	
	public void detect() {
		try {
			File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(brokerController.getMessageStoreConfig().getStorePathRootDir()));
			File[] fileTopicList = dirLogic.listFiles();
			if (fileTopicList != null) {
				for (File fileTopic : fileTopicList) {
					String topic = fileTopic.getName();
					if(topic.startsWith(DLQ_TOPIC_PREFIX)){
						File[] fileQueueIdList = fileTopic.listFiles();
						if (fileQueueIdList != null) {
							for (File fileQueueId : fileQueueIdList) {
								File[] fileQueueList = fileQueueId.listFiles();
								if(fileQueueList != null){
									for(File fileQueue : fileQueueList){
										File moved = move(fileQueue);
										ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> consumeQueueTable = this.dms.getConsumeQueueTable();
										ConcurrentHashMap<Integer, ConsumeQueue> concurrentHashMap = consumeQueueTable.remove(topic);
										if(concurrentHashMap != null){
											for(ConsumeQueue logic : concurrentHashMap.values()){
												logic.destroy();
											};
										}
										this.brokerController.getTopicConfigManager().deleteTopicConfig(topic);
										process(moved, topic);
									}
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("DLQTopicDetector error {}", e);
		}
	}
	
	private File move(File src) throws Exception{
		File dir = new File(brokerController.getMessageStoreConfig().getStorePathRootDir(), "dlp");
		if(!dir.exists()){
			dir.mkdirs();
		}
		File destFile = new File(dir, src.getName());
		FileUtils.copyFile(src, destFile);
		return destFile;
	}
	
	public void process(File fileQueue, String topic){
		List<MessageExt> msgList = new ArrayList<MessageExt>();
		RandomAccessFile raf = null;
		FileChannel channel = null;
		MappedByteBuffer mbb = null;
		try {
			raf = new RandomAccessFile(fileQueue, "rw");
			channel = raf.getChannel();
			int mapedFileSizeLogics = brokerController.getMessageStoreConfig().getMapedFileSizeConsumeQueue();
			mbb = channel.map(MapMode.READ_WRITE, 0, mapedFileSizeLogics);
			ByteBuffer bb = mbb.slice();
			for (int i = 0; i < mapedFileSizeLogics; i += ConsumeQueue.CQStoreUnitSize){
				long offset = bb.getLong();
				int size = bb.getInt();
				bb.getLong();
				if (offset >= 0 && size > 0) {
					SelectMapedBufferResult smbr = dms.getCommitLog().getMessage(offset, size);
					if(smbr != null){
						MessageExt msg = MessageDecoder.decode(smbr.getByteBuffer());
						msgList.add(msg);
					} else{
						log.warn("SelectMapedBufferResult is null.");
					}
				} else {
					break;
				}
			}
		} catch (Exception e) {
			log.error("DLQTopicDetector process error {}", e);
		} finally{
			clean(mbb);
			if(channel != null){
				try {
					channel.close();
				} catch (IOException e) {
					//
				}
			}
			if(raf != null){
				try {
					raf.close();
				} catch (IOException e) {
					//
				}
			}
		}
		if(!msgList.isEmpty()){
			save(msgList, topic);
			fileQueue.delete();
		}
	}
	
	public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }
	
	private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";


        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }
	
	private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }
	
	private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

	private void save(List<MessageExt> msgList, String topic){
		File file = new File(brokerController.getMessageStoreConfig().getStorePathRootDir() + File.separator + "dlp", topic + ".txt");
		for(MessageExt msg : msgList){
			try {
				FileUtils.writeStringToFile(file, msg.toString(), "UTF-8", true);
			} catch (IOException e) {
				log.error("save dlq msg {} error {} ", msg, e);
			}
		}
	}
	
	public void shutdown(){
		scheduledExecutorService.shutdown();
	}
}
