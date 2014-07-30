package server;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import common.EMSGID;

public class MDecoder extends FrameDecoder  //Decoder stream
{	 
    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        if (buffer.readableBytes() < 4)
        {
            return null;//(1)
        }
        int dataLength = buffer.getInt(buffer.readerIndex());
        if (buffer.readableBytes() < dataLength + 4) 
        {
            return null;//(2)
        }
 
        buffer.skipBytes(4);//(3)
        int id = buffer.readInt();
        byte[] decoded = new byte[dataLength-4];
        
        buffer.readBytes(decoded);
        NetMsg msg = new NetMsg(decoded, id);//(4)
        msg.setMsgID(EMSGID.values()[id]);
        return msg;
    }
}