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
    private static final int INITIAL_LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = 4;
    private static final String UNSUPPORTED_COMMAND_FORMAT = "unsupport command:%s";

    private final Aof aof;

    public CommandDecoder(Aof aof) {
        super(MAX_FRAME_LENGTH, INITIAL_LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH);
        this.aof = aof;
    }

    public CommandDecoder() {
        this(null);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) {
        TraceIdUtil.newTraceId();
        if (!in.isReadable()) {
            return null;
        }
        return decodeCommand(ctx, in);
    }

    private Object decodeCommand(ChannelHandlerContext ctx, ByteBuf in) {
        int mark = in.readerIndex();
        try {
            List<Resp> respList = Resp.decode(in);
            if (respList == null || respList.isEmpty()) {
                return null;
            }
            return processCommand(ctx, respList);
        } catch (Exception e) {
            in.readerIndex(mark);
            log.error("命令解码失败", e);
            return null;
        }
    }

    private Object processCommand(ChannelHandlerContext ctx, List<Resp> respList) {
        Command command;
        if (respList.size() > 1) {
            command = processWriteCommand(respList);
        } else {
            command = processReadCommand(respList);
        }
        if (command == null) {
            handleUnsupportedCommand(ctx, respList);
        }
        return command;
    }

    private Command processWriteCommand(List<Resp> respList) {
        if (aof == null) {
            return null;
        }
        Command command = WriteCommandFactory.create(aof.getRedisCore()).from(respList);
        if (command != null) {
            aof.put(respList);
        }
        return command;
    }

    private Command processReadCommand(List<Resp> respList) {
        if (aof == null) {
            return null;
        }
        return CommonCommandFactory.create(aof.getRedisCore()).from(respList);
    }

    private void handleUnsupportedCommand(ChannelHandlerContext ctx, List<Resp> respList) {
        if (!respList.isEmpty() && respList.get(0) instanceof BulkString bulkString) {
            String errorMessage = String.format(UNSUPPORTED_COMMAND_FORMAT,
                    bulkString.getContent().toUtf8String());
            ctx.writeAndFlush(new Errors(errorMessage));
        }
    }
}
