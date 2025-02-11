package com.wiqer.redis.resp;

import lombok.Getter;

@Getter
public class RespInt implements Resp {

    int value;

    public static final RespInt ZERO = new RespInt(0);

    public static final RespInt ONE = new RespInt(1);

    public RespInt(int value) {
        this.value = value;
    }

}
