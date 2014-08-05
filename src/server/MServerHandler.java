package server;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;


public class MServerHandler extends SimpleChannelUpstreamHandler 
{
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) 
    {
        if (!(e.getMessage() instanceof NetMsg)) 
        {
            return;
        }        
        memSession.getInstance().addSession(e);
    }
 
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
    {  
		Channel channel = e.getChannel();
		channel.close();
    }
    
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)throws Exception 
	{

	}
}