package client;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

public class Client
{
	ClientBootstrap bootstrap;
	ChannelFuture channelFuture;
	String host;
	int port;
	int id;

	public boolean init(String host, int port)
	{
		bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new MClientPipelineFactory());
        // Start the connection attempt.
        channelFuture = bootstrap.connect(new InetSocketAddress(host, port));        
        return channelFuture.isSuccess();
	}
		
	public void stop() 
	{
		channelFuture.awaitUninterruptibly();
		if (!channelFuture.isSuccess()) 
		{
			channelFuture.getCause().printStackTrace();
		}
		channelFuture.getChannel().getCloseFuture().awaitUninterruptibly();
		bootstrap.releaseExternalResources();
	}
}