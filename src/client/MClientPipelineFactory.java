package client;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

import server.MDecoder;
import server.MEncoder;

public class MClientPipelineFactory implements ChannelPipelineFactory 
{

	public ChannelPipeline getPipeline() throws Exception 
	{
		ChannelPipeline pipeline = Channels.pipeline();
		
		pipeline.addLast("decoder", new MDecoder());
		pipeline.addLast("encoder", new MEncoder());	
		return pipeline;
	}
}