package server;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

public class MServerPipelineFactory implements ChannelPipelineFactory 
{
	public ChannelPipeline getPipeline() throws Exception 
	{
		ChannelPipeline pipeline = Channels.pipeline();		
		
		pipeline.addLast("decoder", new MDecoder());
		pipeline.addLast("encoder", new MEncoder());
		pipeline.addLast("handler", new MServerHandler());		
		return pipeline;
	}
}