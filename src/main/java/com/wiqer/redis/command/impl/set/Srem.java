package com.wiqer.redis.command.impl.set;

import com.wiqer.redis.command.CommonCommandType;
import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisSet;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespInt;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Srem extends AbstractCore<RedisSet> implements WriteCommand {

    private BytesWrapper key;

    private List<BytesWrapper> members;

    @Override
    public String type() {
        return CommonCommandType.srem.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore(redisCore);
        key = ((BulkString) array.get(1)).getContent();
        members = Stream.of(array).skip(2).map(resp -> ((BulkString) resp).getContent()).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisSet redisSet = get(key);
        return new RespInt(redisSet.srem(members));
    }
}
