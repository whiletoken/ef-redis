package com.wiqer.redis;

import com.wiqer.redis.aof.Aof;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommonCommandFactory;
import com.wiqer.redis.command.WriteCommandFactory;
import com.wiqer.redis.resp.*;
import com.wiqer.redis.util.TraceIdUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author lilan
 */
@Slf4j
public class CommandDecoder extends LengthFieldBasedFrameDecoder {

    private static final int MAX_FRAME_LENGTH = Integer.MAX_VALUE;

    private Aof aof = null;

    public CommandDecoder(Aof aof) {
        this();
        this.aof = aof;
    }

    public CommandDecoder() {
        super(MAX_FRAME_LENGTH, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) {
        TraceIdUtil.newTraceId();
        while (in.readableBytes() != 0) {
            int mark = in.readerIndex();
            try {
                List<Resp> list = Resp.decode(in);
                Command command;
                if (list.size() > 1) {
                    command = WriteCommandFactory.create(aof.getRedisCore()).from(list);
                    if (aof != null && command != null) {
                        aof.put(list);
                    }
                } else {
                    command = CommonCommandFactory.create(aof.getRedisCore()).from(list);
                }
                if (command == null) {
                    BulkString bulkString = (BulkString) list.get(0);
                    ctx.writeAndFlush(new Errors("unsupport command:" + bulkString.getContent().toUtf8String()));
                }
                return command;
            } catch (Exception e) {
                in.readerIndex(mark);
                log.error("解码命令", e);
                break;
            }
        }
        return null;
    }
}
