package com.wiqer.redis.resp;

import com.wiqer.redis.datatype.BytesWrapper;
import lombok.Getter;

import static com.wiqer.redis.command.Command.CHARSET;

@Getter
public class BulkString implements Resp {

    public static final BulkString NullBulkString = new BulkString();

    public static final BulkString DATABASES = new BulkString(new BytesWrapper("databases".getBytes(CHARSET)));

    public static final BulkString ONE = new BulkString(new BytesWrapper("1".getBytes(CHARSET)));

    public static final BulkString ZERO = new BulkString(new BytesWrapper("0".getBytes(CHARSET)));

    private BytesWrapper content;

    public BulkString(BytesWrapper content) {
        this.content = content;
    }

    public BulkString() {
    }

    public BulkString(byte[] content) {
        this.content = new BytesWrapper(content);
    }
}
