package com.wiqer.redis.command.impl;

import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.datatype.RedisList;

public class Rpush extends Push {

    public Rpush() {
        super(RedisList::rpush);
    }

    @Override
    public String type() {
        return CommonCommandType.rpush.name();
    }
}
