package com.wiqer.redis.command.impl.list;

import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.command.impl.Push;
import com.wiqer.redis.datatype.RedisList;

public class Lpush extends Push {

    public Lpush() {
        super(RedisList::lpush);
    }

    @Override
    public String type() {
        return CommonCommandType.lpush.name();
    }
}
