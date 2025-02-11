package com.wiqer.redis.resp;

import com.wiqer.redis.datatype.BytesWrapper;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Administrator
 */
public interface Resp {

    static void write(List<Resp> list, ByteBuf buffer) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException();
        }
        if (list.size() == 1) {
            Resp resp = list.get(0);
            if (resp instanceof SimpleString) {
                buffer.writeByte(RespType.STATUS.getCode());
                String content = ((SimpleString) resp).getContent();
                char[] charArray = content.toCharArray();
                for (char each : charArray) {
                    buffer.writeByte((byte) each);
                }
                buffer.writeByte(RespType.R.getCode());
                buffer.writeByte(RespType.N.getCode());
            } else if (resp instanceof Errors) {
                buffer.writeByte(RespType.ERROR.getCode());
                String content = ((Errors) resp).getContent();
                char[] charArray = content.toCharArray();
                for (char each : charArray) {
                    buffer.writeByte((byte) each);
                }
                buffer.writeByte(RespType.R.getCode());
                buffer.writeByte(RespType.N.getCode());
            } else if (resp instanceof RespInt) {
                buffer.writeByte(RespType.INTEGER.getCode());
                String content = String.valueOf(((RespInt) resp).getValue());
                char[] charArray = content.toCharArray();
                for (char each : charArray) {
                    buffer.writeByte((byte) each);
                }
                buffer.writeByte(RespType.R.getCode());
                buffer.writeByte(RespType.N.getCode());
            } else if (resp instanceof BulkString) {
                buffer.writeByte(RespType.BULK.getCode());
                BytesWrapper content = ((BulkString) resp).getContent();
                if (content == null) {
                    buffer.writeByte(RespType.ERROR.getCode());
                    buffer.writeByte(RespType.ONE.getCode());
                    buffer.writeByte(RespType.R.getCode());
                    buffer.writeByte(RespType.N.getCode());
                } else if (content.getByteArray().length == 0) {
                    buffer.writeByte(RespType.ZERO.getCode());
                    buffer.writeByte(RespType.R.getCode());
                    buffer.writeByte(RespType.N.getCode());
                    buffer.writeByte(RespType.R.getCode());
                    buffer.writeByte(RespType.N.getCode());
                } else {
                    String length = String.valueOf(content.getByteArray().length);
                    char[] charArray = length.toCharArray();
                    for (char each : charArray) {
                        buffer.writeByte((byte) each);
                    }
                    buffer.writeByte(RespType.R.getCode());
                    buffer.writeByte(RespType.N.getCode());
                    buffer.writeBytes(content.getByteArray());
                    buffer.writeByte(RespType.R.getCode());
                    buffer.writeByte(RespType.N.getCode());
                }
            }
        } else {
            buffer.writeByte(RespType.MULTYBULK.getCode());
            String length = String.valueOf(list.size());
            char[] charArray = length.toCharArray();
            for (char each : charArray) {
                buffer.writeByte((byte) each);
            }
            buffer.writeByte(RespType.R.getCode());
            buffer.writeByte(RespType.N.getCode());
            for (Resp each : list) {
                write(List.of(each), buffer);
            }
        }
    }

    /**
     * 无法解码压测客户端
     */
    static List<Resp> decode(ByteBuf buffer) {
        if (buffer.readableBytes() <= 0) {
            throw new IllegalStateException("没有读取到完整的命令");
        }
        byte c = buffer.readByte();
        RespType respType = RespType.getByCode(c);
        switch (respType) {
            case STATUS -> {
                return List.of(new SimpleString(getString(buffer)));
            }
            case ERROR -> {
                return List.of(new Errors(getString(buffer)));
            }
            case INTEGER -> {
                int value = getNumber(buffer);
                return List.of(new RespInt(value));
            }
            case BULK -> {
                int length = getNumber(buffer);
                if (buffer.readableBytes() < length + 2) {
                    throw new IllegalStateException("没有读取到完整的命令");
                }
                byte[] content;
                if (length == -1) {
                    content = null;
                } else {
                    content = new byte[length];
                    buffer.readBytes(content);
                }
                if (buffer.readByte() != RespType.R.getCode() || buffer.readByte() != RespType.N.getCode()) {
                    throw new IllegalStateException("没有读取到完整的命令");
                }
                return List.of(new BulkString(new BytesWrapper(content)));
            }
            case MULTYBULK -> {
                int numOfElement = getNumber(buffer);
                List<Resp> list = new ArrayList<>(numOfElement);
                for (int i = 0; i < numOfElement; i++) {
                    list.addAll(decode(buffer));
                }
                return list;
            }
            default -> {
                if (c > 64 && c < 91) {
                    return List.of(new SimpleString(c + getString(buffer)));
                } else {
                    return decode(buffer);
                }
            }
        }
    }

    static int getNumber(ByteBuf buffer) {
        char t;
        t = (char) buffer.readByte();
        boolean positive = true;
        int value = 0;
        // 错误（Errors）： 响应的首字节是 "-"
        if (t == RespType.ERROR.getCode()) {
            positive = false;
        } else {
            value = t - RespType.ZERO.getCode();
        }
        while (buffer.readableBytes() > 0 && (t = (char) buffer.readByte()) != RespType.R.getCode()) {
            value = value * 10 + (t - RespType.ZERO.getCode());
        }
        if (buffer.readableBytes() == 0 || buffer.readByte() != RespType.N.getCode()) {
            throw new IllegalStateException("没有读取到完整的命令");
        }
        if (!positive) {
            value = -value;
        }
        return value;
    }

    static String getString(ByteBuf buffer) {
        char c;
        StringBuilder builder = new StringBuilder();
        while (buffer.readableBytes() > 0 && (c = (char) buffer.readByte()) != RespType.R.getCode()) {
            builder.append(c);
        }
        if (buffer.readableBytes() == 0 || buffer.readByte() != RespType.N.getCode()) {
            throw new IllegalStateException("没有读取到完整的命令");
        }
        return builder.toString();
    }
}
