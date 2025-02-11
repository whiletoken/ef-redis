package com.wiqer.redis.command.impl;

import com.wiqer.redis.command.WriteCommandType;
import com.wiqer.redis.datatype.RedisList;

public class Rpush extends Push {

    public Rpush() {
        super(RedisList::rpush);
    }

    @Override
    public String type() {
        return WriteCommandType.rpush.name();
    }
}
