package common;

import java.lang.reflect.InvocationTargetException;

import messageBody.memcachedmsg.nm_Connected;
import messageBody.memcachedmsg.nm_Connected_mem_back;
import messageBody.memcachedmsg.nm_read;
import messageBody.memcachedmsg.nm_read_recovery;
import messageBody.memcachedmsg.nm_write_1;
import messageBody.memcachedmsg.nm_write_1_res;
import messageBody.memcachedmsg.nm_write_2;
import messageBody.requestMsg.nr_Connected_mem;
import messageBody.requestMsg.nr_Read;
import messageBody.requestMsg.nr_write;

import com.google.protobuf.GeneratedMessage;

public class RegisterHandler {
	public static void initHandler() // register all messages created by
								     // protobuf and will be handled by R-Memcached
	{
		initHandler(EMSGID.nm_connected.ordinal(), nm_Connected.class);
		initHandler(EMSGID.nm_connected_mem_back.ordinal(),
				nm_Connected_mem_back.class);
		initHandler(EMSGID.nr_connected_mem.ordinal(), nr_Connected_mem.class);
		initHandler(EMSGID.nr_read.ordinal(), nr_Read.class);
		initHandler(EMSGID.nm_read.ordinal(), nm_read.class);
		initHandler(EMSGID.nm_read_recovery.ordinal(), nm_read_recovery.class);
		initHandler(EMSGID.nr_write.ordinal(), nr_write.class);
		initHandler(EMSGID.nm_write_1.ordinal(), nm_write_1.class);
		initHandler(EMSGID.nm_write_1_res.ordinal(), nm_write_1_res.class);
		initHandler(EMSGID.nm_write_2.ordinal(), nm_write_2.class);
	}

	private static void initHandler(int id,
			Class<? extends GeneratedMessage> msgCla) {
		try {
			MessageManager.addMessageCla(id, msgCla);
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
