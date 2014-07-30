package com.myself.server;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.MessageLite;

import common.EMSGID;
import common.MessageManager;

public class NetMsg    //   package different messages
{
	EMSGID msgID;
	MessageLite messageLite;
	
	private NetMsg(){};
	public static NetMsg newMessage()
	{
		return new NetMsg();
	}
	
	NetMsg(byte[] decoded, int id) throws Exception 
	{
		messageLite = MessageManager.getMessage(id, decoded);
	}
	
	public byte[] getBytes()   //get data in messageLite
	{
		return messageLite.toByteArray();
	}

	public EMSGID getMsgID()   //get message catagory
	{
		return msgID;
	}

	public void setMsgID(EMSGID id) {   //according EMSGID.java set the message ID
		this.msgID = id;
		
	}

	@SuppressWarnings("unchecked")
	public <T extends MessageLite> T getMessageLite() //get  messageLite
	{
		return (T)messageLite;
	}
	
	@SuppressWarnings("rawtypes")
	public void setMessageLite(GeneratedMessage.Builder builder) 
	{
		this.messageLite = builder.build();
	}

}
