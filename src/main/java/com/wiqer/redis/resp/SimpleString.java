package com.wiqer.redis.resp;

import lombok.Getter;

/**
 * @author lilan
 */
@Getter
public class SimpleString implements Resp {

    public static final SimpleString OK = new SimpleString("OK");

    public static final SimpleString ERROR = new SimpleString("-ERR invalid DB index");

    public static final SimpleString PONG = new SimpleString("PONG");

    private final String content;

    public SimpleString(String content) {
        this.content = content;
    }

}
