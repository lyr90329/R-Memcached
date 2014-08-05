package client;

import messageBody.memcachedmsg.nm_Connected;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import server.NetMsg;
import common.EMSGID;


public class MClientHandler extends SimpleChannelUpstreamHandler 
{ 
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) 
    {
		nm_Connected.Builder builder = nm_Connected.newBuilder();
		builder.setNum(ClientMgr.getInstance().mClientNumber);
		
		NetMsg sendMsg = NetMsg.newMessage();
		sendMsg.setMsgID(EMSGID.nm_connected);
		sendMsg.setMessageLite(builder);
		e.getChannel().write(sendMsg);	
    }
 
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) 
    {  	
    	 if (!(e.getMessage() instanceof NetMsg)) 
         {
             return;//(1)
         }
    }    
 
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) 
    {       
    	if (e.getChannel().getLocalAddress() == null) {
			return;
		}
    }
}