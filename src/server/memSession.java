package server;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import messageBody.memcachedmsg.nm_Connected;
import messageBody.memcachedmsg.nm_Connected_mem_back;
import messageBody.memcachedmsg.nm_read;
import messageBody.memcachedmsg.nm_read_recovery;
import messageBody.memcachedmsg.nm_write_1;
import messageBody.memcachedmsg.nm_write_1_res;
import messageBody.memcachedmsg.nm_write_2;
import messageBody.requestMsg.nr_Connected_mem_back;
import messageBody.requestMsg.nr_Read;
import messageBody.requestMsg.nr_Read_res;
import messageBody.requestMsg.nr_write;
import messageBody.requestMsg.nr_write_res;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import memcached.MemcachedClient;
import client.ClientMgr;

import common.EMSGID;

public class memSession implements Runnable {
	ConcurrentLinkedQueue<MessageEvent> recvQueue = new ConcurrentLinkedQueue<MessageEvent>();
	ConcurrentHashMap<Integer, Channel> ClientChannelMap = new ConcurrentHashMap<Integer, Channel>();
	ConcurrentHashMap<String, LockKey> LockKeyMap = new ConcurrentHashMap<String, LockKey>();
	public MemcachedClient client;
	static memSession session = null;

	Channel webServeChannel = null;

	public static memSession getInstance() {
		if (session == null) {
			session = new memSession();
		}
		return session;
	}

	public void start(String host) {
		client = new MemcachedClient(host);
		new Thread(session).start();
		System.out.println("session start");
	}

	// 增加client连接
	public void addClientChannel(Integer num, Channel ch) {
		ClientChannelMap.put(num, ch);
	}

	public Channel getClientChannel(Integer id) {
		return ClientChannelMap.get(id);
	}

	// 删掉client连接
	@SuppressWarnings("rawtypes")
	public void removeClientChannel(Channel ch) {
		Iterator iter = ClientChannelMap.entrySet().iterator();
		while (iter.hasNext()) {
			Entry entry = (Entry) iter.next();
			if ((Channel) entry.getValue() == ch) {
				ClientChannelMap.remove((Integer) entry.getKey());
				break;
			}
		}
	}

	// ////////////////////////////////////////////////////////
	public void run() {
		long curTime = System.currentTimeMillis();
		while (true) {
			MessageEvent event = recvQueue.poll();
			while (event != null) {
				handle(event);
				event = recvQueue.poll();
				if (System.currentTimeMillis() - curTime > 20000) {
					curTime = System.currentTimeMillis();
					HandleBadLock(curTime);
				}
			}
			try {
				Thread.sleep((long) 0.00001);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("rawtypes")
	public void HandleBadLock(long curTime) {
		Iterator it = LockKeyMap.keySet().iterator();
		while(it.hasNext()){
			String key = (String) it.next();
			LockKey value = LockKeyMap.get(key);
			if (value.state == LockKey.waitLock) {
				if (curTime - value.time > 20000) {
					value.state = LockKey.badLock;
					LockKeyMap.remove(key);
					LockKeyMap.put(key, value);
				}
			}
		}
	}

	public int getLockState(String key) {
		LockKey lock = LockKeyMap.get(key);
		if (lock == null) {
			return LockKey.unLock;
		}
		return lock.state;
	}

	public void setLockState(String key, Integer state) {
		LockKey lock = LockKeyMap.get(key);
		if (lock != null) {
			lock.state = state;
			LockKeyMap.put(key, lock);
		} else {
			System.out.println("set Lock state error");
			return;
		}
	}

	public boolean lockKey(String key, LockKey lock) {
		LockKey lockKey = LockKeyMap.put(key, lock);
		if (lockKey != null && (lockKey.state == 0 || lockKey.state == 1)) {
			return true;
		}
		return lockKey == null;
	}

	public int desLockKeyCount(String key) {
		LockKey lock = LockKeyMap.get(key);
		if (lock != null) {
			lock.ncount--;
			LockKeyMap.put(key, lock);
			return lock.ncount;
		}
		return 0;
	}

	public boolean removeLock(String key) {
		return LockKeyMap.remove(key) != null;
	}

	public void handle(MessageEvent e) {
		NetMsg msg = (NetMsg) e.getMessage();

		switch (msg.getMsgID()) {
		case nm_connected: {
			nm_Connected msgLite = msg.getMessageLite();
			addClientChannel(msgLite.getNum(), e.getChannel());

			nm_Connected_mem_back.Builder builder = nm_Connected_mem_back
					.newBuilder();
			builder.setNum(ClientMgr.getInstance().mClientNumber);

			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nm_connected_mem_back);

			e.getChannel().write(send);
		}
			break;
		case nm_connected_mem_back: {
			nm_Connected_mem_back msgLite = msg.getMessageLite();
			addClientChannel(msgLite.getNum(), e.getChannel());
		}
			break;
		case nr_connected_mem: {
			webServeChannel = e.getChannel();

			nr_Connected_mem_back.Builder builder = nr_Connected_mem_back
					.newBuilder();
			builder.setMemID(ClientMgr.getInstance().mClientNumber);

			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nr_connected_mem_back);

			e.getChannel().write(send);
		}
			break;
		case nm_connected_web_back: {
			webServeChannel = e.getChannel();
		}
			break;
		case nr_read: {
			nr_Read msgLite = msg.getMessageLite();

			Integer state = getLockState(msgLite.getKey());
			if (state == LockKey.waitLock) {
				nr_Read_res.Builder builder = nr_Read_res.newBuilder();
				builder.setKey(msgLite.getKey());
				builder.setValue("");
				builder.setTime(msgLite.getTime());

				NetMsg send = NetMsg.newMessage();
				send.setMessageLite(builder);
				send.setMsgID(EMSGID.nr_read_res);

				webServeChannel.write(send);
				return;
			} else if (state == LockKey.unLock) {
				String value = (String) client.get(msgLite.getKey());
				if (value != null) {
					nr_Read_res.Builder builder = nr_Read_res.newBuilder();
					builder.setKey(msgLite.getKey());
					builder.setValue(value);
					builder.setTime(msgLite.getTime());

					NetMsg send = NetMsg.newMessage();
					send.setMessageLite(builder);
					send.setMsgID(EMSGID.nr_read_res);
					webServeChannel.write(send);

					return;
				}
			} 
			// Can't get data in local Memcached server, try to ask for data from another cache node
			nm_read.Builder builder = nm_read.newBuilder();
			builder.setKey(msgLite.getKey());
			builder.setTime(msgLite.getTime());

			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nm_read);

			if (sendOtherCopyMsg(gethashMem(msgLite.getKey()), send) == false) {
				nr_Read_res.Builder builder1 = nr_Read_res.newBuilder();
				builder1.setKey(msgLite.getKey());
				builder.setValue("");
				builder1.setTime(msgLite.getTime());

				NetMsg send1 = NetMsg.newMessage();
				send1.setMessageLite(builder1);
				send1.setMsgID(EMSGID.nr_read_res);

				webServeChannel.write(send);

			}
		}
			break;
		case nm_read: {
			nm_read msgLite = msg.getMessageLite();
			Integer state = getLockState(msgLite.getKey());
			if (state == LockKey.unLock) {
				String value = (String) client.get(msgLite.getKey());
				if (value != null) {
					nr_Read_res.Builder builder = nr_Read_res.newBuilder();
					builder.setKey(msgLite.getKey());
					builder.setTime(msgLite.getTime());
					builder.setValue(value);
					NetMsg send = NetMsg.newMessage();
					send.setMessageLite(builder);
					send.setMsgID(EMSGID.nr_read_res);
					webServeChannel.write(send);
					
					nm_read_recovery.Builder builder1 = nm_read_recovery.newBuilder();
					builder1.setKey(msgLite.getKey());
					builder1.setTime(msgLite.getTime());
					builder1.setValue(value);
					NetMsg send1 = NetMsg.newMessage();
					send.setMessageLite(builder1);
					send.setMsgID(EMSGID.nm_read_recovery);
					e.getChannel().write(send1);
					return;
				} 
			} 
			nr_Read_res.Builder builder = nr_Read_res.newBuilder();
			builder.setKey(msgLite.getKey());
			builder.setValue("");
			builder.setTime(msgLite.getTime());

			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nr_read_res);

			webServeChannel.write(send);
		}
			break;
		case nm_read_recovery: {
			nm_read_recovery msgLite = msg.getMessageLite();
			Integer state = getLockState(msgLite.getKey());
			if(state == LockKey.waitLock) {
				System.out.println("recovery fail because of waitlock.");
				return;
			}else if (state == LockKey.badLock) {
				removeLock(msgLite.getKey());
			}
			boolean res = client.set(msgLite.getKey(), msgLite.getValue());
			if (!res) {
				setLockState(msgLite.getKey(), LockKey.badLock);
				System.err.println("read recovery fail");
			}	
		}
			break;
			
		case nr_write: {
			nr_write msgLite = msg.getMessageLite();
			Integer state = getLockState(msgLite.getKey());
			if (state == LockKey.waitLock) {
				System.out.println("write conflict, please request again.");
				nr_write_res.Builder builder2 = nr_write_res.newBuilder();
				builder2.setKey(msgLite.getKey());
				builder2.setValue("");
				builder2.setTime(msgLite.getTime());

				NetMsg send2 = NetMsg.newMessage();
				send2.setMessageLite(builder2);
				send2.setMsgID(EMSGID.nr_write_res);
				webServeChannel.write(send2);
				return;
			} 
//			else if (state == LockKey.badLock) {
//				removeLock(msgLite.getKey());
//			}
			LockKey lockKey = new LockKey(
					ClientMgr.getInstance().mClientNumber, ClientMgr.nCopyNode-1,
					System.currentTimeMillis(), LockKey.waitLock);
			if (lockKey(msgLite.getKey(), lockKey) == false) {
				System.out.println("nr_write lock fail");
				nr_write_res.Builder builder2 = nr_write_res.newBuilder();
				builder2.setKey(msgLite.getKey());
				builder2.setValue("");
				builder2.setTime(msgLite.getTime());

				NetMsg send2 = NetMsg.newMessage();
				send2.setMessageLite(builder2);
				send2.setMsgID(EMSGID.nr_write_res);
				webServeChannel.write(send2);
				System.out.println("write lock conflict, please request again.");
				return;
			}
			nm_write_1.Builder builder = nm_write_1.newBuilder();
			builder.setKey(msgLite.getKey());
			builder.setValue(msgLite.getValue());
			builder.setMemID(ClientMgr.getInstance().mClientNumber);
			builder.setTime(msgLite.getTime());

			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nm_write_1);
			int count = sendOtherAllCopyMsg(gethashMem(msgLite.getKey()), send);

			if (count != ClientMgr.nCopyNode-1) {
				System.out.println("write failed in send invalidation msg to remote node.");
				
				nr_write_res.Builder builder2 = nr_write_res.newBuilder();
				builder2.setKey(msgLite.getKey());
				builder2.setValue("");
				builder2.setTime(msgLite.getTime());

				NetMsg send2 = NetMsg.newMessage();
				send2.setMessageLite(builder2);
				send2.setMsgID(EMSGID.nr_write_res);
				webServeChannel.write(send2);

			}
		}
			break;

		case nm_write_1: {
			nm_write_1 msgLite = msg.getMessageLite();

			Integer state = getLockState(msgLite.getKey());
			if (state == LockKey.waitLock) {
				removeLock(msgLite.getKey());
			} 
			else if (state == LockKey.badLock) {
				removeLock(msgLite.getKey());
			}

//			System.out.println("ready to write key: "+msgLite.getKey());
			
			LockKey lockKey = new LockKey(
					ClientMgr.getInstance().mClientNumber, 0,
					System.currentTimeMillis(), LockKey.waitLock);
			if (lockKey(msgLite.getKey(), lockKey) == false) {
				System.out.println("nm_write_1 Lock fail");
			}
			
			nm_write_1_res.Builder builder = nm_write_1_res.newBuilder();
			builder.setKey(msgLite.getKey());
			builder.setValue(msgLite.getValue());
			builder.setTime(msgLite.getTime());
			builder.setMemID(msgLite.getMemID());

			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nm_write_1_res);
			getClientChannel(msgLite.getMemID()).write(send);


		}
			break;
		case nm_write_1_res: {
			nm_write_1_res msgLite = msg.getMessageLite();

			if (desLockKeyCount(msgLite.getKey()) == ClientMgr.protocol) {
				boolean res = client.set(msgLite.getKey(),
						msgLite.getValue());
				if (res) {
					removeLock(msgLite.getKey());
//					System.out.println("key:"+msgLite.getKey()+", value:"+msgLite.getValue());
					nr_write_res.Builder builder2 = nr_write_res.newBuilder();
					builder2.setKey(msgLite.getKey());
					builder2.setValue(msgLite.getValue());
					builder2.setTime(msgLite.getTime());
					NetMsg send2 = NetMsg.newMessage();
					send2.setMessageLite(builder2);
					send2.setMsgID(EMSGID.nr_write_res);
					webServeChannel.write(send2);
					
					nm_write_2.Builder builder = nm_write_2.newBuilder();
					builder.setKey(msgLite.getKey());
					builder.setValue(msgLite.getValue());
					builder.setMemID(msgLite.getMemID());
					builder.setTime(msgLite.getTime());
					NetMsg send = NetMsg.newMessage();
					send.setMessageLite(builder);
					send.setMsgID(EMSGID.nm_write_2);
					sendOtherAllCopyMsg(gethashMem(msgLite.getKey()),send);
				} else {
					setLockState(msgLite.getKey(), LockKey.badLock);
					System.err.println("write to memcached server error");
					nr_write_res.Builder builder2 = nr_write_res.newBuilder();
					builder2.setKey(msgLite.getKey());
					builder2.setValue("");
					builder2.setTime(msgLite.getTime());

					NetMsg send2 = NetMsg.newMessage();
					send2.setMessageLite(builder2);
					send2.setMsgID(EMSGID.nr_write_res);
					webServeChannel.write(send2);
				}
			}
		}
			break;
		case nm_write_2: {
			nm_write_2 msgLite = msg.getMessageLite();

			boolean res = client.set(msgLite.getKey(), msgLite.getValue());
			if (res) {
				removeLock(msgLite.getKey());
//				System.out.println("key:"+msgLite.getKey()+", value:"+msgLite.getValue());
			} else {
				setLockState(msgLite.getKey(), LockKey.badLock);
				System.err.println("write in write_2 fail");
			}
		}
			break;
		default:
			System.err.println(msg.getMsgID().toString());
			break;
		}
	}

	public void addSession(MessageEvent e) {
		recvQueue.offer(e);
	}

	public int gethashMem(String key) {
		return Math.abs(key.hashCode() % ClientMgr.getInstance().getSize());
	}

	public boolean sendOtherCopyMsg(Integer hash, NetMsg msg) {
		for (int i = 0; i < ClientMgr.nCopyNode; i++) {
			Integer index = (hash + i + ClientMgr.getInstance().getSize())
					% ClientMgr.getInstance().getSize();
			if (index == ClientMgr.getInstance().mClientNumber)
				continue;

			Channel eChannel = getClientChannel(index);
			if (eChannel != null) {
				eChannel.write(msg);
				return true;
			}
		}
		return false;
	}

	public int sendOtherAllCopyMsg(int hash, NetMsg msg) {
		int count = 0;
		for (int i = 0; i < ClientMgr.nCopyNode; i++) {
			int index = (hash + i + ClientMgr.getInstance().getSize())
					% ClientMgr.getInstance().getSize();

			if (index == ClientMgr.getInstance().mClientNumber)
				continue;

			Channel eChannel = getClientChannel(index);
			if (eChannel != null) {
				eChannel.write(msg);
				count++;
			}
		}
		return count;
	}
}
