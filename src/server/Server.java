package server;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class Server 
{
	static Server server;
	ServerBootstrap bootstrap;

	public void init(int port) 
	{
		bootstrap = new ServerBootstrap(
		                new NioServerSocketChannelFactory(
		                        Executors.newCachedThreadPool(),
		                        Executors.newCachedThreadPool()));
		 
		// Set up the default event pipeline.
		bootstrap.setPipelineFactory(new MServerPipelineFactory());
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);
		bootstrap.setOption("reuseAddress", true);		 
		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(port));
		
		System.out.println("server init complete");
	}

	public void stop()
	{
		bootstrap.releaseExternalResources();
	}

	public static Server getInstance()
	{
		if (server == null)
		{
			server = new Server();	
		}
		return server;
	}
}