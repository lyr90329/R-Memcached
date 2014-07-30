package common;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

/**
 * 
 * @author Yanran Lu
 * 
 */
public class MessageManager {     //create messageMap to manage all messages
	private static Map<Integer, MessageLite> messageMap = new HashMap<Integer, MessageLite>();

	public static void addMessageCla(int id,    //add message to map
			Class<? extends GeneratedMessage> msgCla)
			throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		if (msgCla == null)
			return;
		Method method = msgCla.getMethod("getDefaultInstance");
		MessageLite lite = (MessageLite) method.invoke(null, null);
		messageMap.put(id, lite);
	}

	public static MessageLite getMessage(int id, byte[] body)   //get message from map
			throws InvalidProtocolBufferException {
		MessageLite list = messageMap.get(id);
		if (list == null) {
			System.err.printf("msg %d no register", id);
			return null;
		}
		return list.newBuilderForType().mergeFrom(body).build();
	}
}
