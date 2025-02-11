package com.wiqer.redis;

import com.wiqer.redis.resp.Resp;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.List;

public class ResponseEncoder extends MessageToByteEncoder<Resp> {

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Resp resp, ByteBuf byteBuf) {
        try {
            Resp.write(List.of(resp), byteBuf);//msg.encode();
            byteBuf.writeBytes(byteBuf);
        } catch (Exception e) {
            channelHandlerContext.close();
        }
    }

}
