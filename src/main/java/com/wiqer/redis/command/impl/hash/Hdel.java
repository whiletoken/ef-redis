package com.wiqer.redis.command.impl.hash;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.command.WriteCommandType;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.core.RedisHashCore;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisHash;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespInt;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Hdel extends AbstractCore<RedisHashCore, RedisHash> implements WriteCommand {

    private BytesWrapper key;
    private List<BytesWrapper> fields;

    @Override
    public String type() {
        return WriteCommandType.hdel.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore((RedisHashCore) redisCore);
        key = ((BulkString) array.get(1)).getContent();
        fields = Stream.of(array).skip(2).map(resp -> ((BulkString) resp).getContent()).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisHash redisHash = get(key);
        return new RespInt(redisHash.del(fields));
    }
}
