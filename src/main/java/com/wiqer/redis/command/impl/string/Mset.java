package com.wiqer.redis.command.impl.string;

import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisString;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Mset extends AbstractCore<RedisString> implements WriteCommand {

    private List<BytesWrapper> kvList;

    @Override
    public String type() {
        return CommonCommandType.mset.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        kvList = Stream.of(array).skip(1).map(resp -> ((BulkString) resp).getContent()).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        for (int i = 0; i < kvList.size(); i += 2) {
            put(kvList.get(i), new RedisString(kvList.get(i + 1)));
        }
        return SimpleString.OK;
    }
}
