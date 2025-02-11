package com.wiqer.redis.command.impl.zset;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.core.RedisZsetCore;
import com.wiqer.redis.command.WriteCommandType;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisZset;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespInt;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Zrem extends AbstractCore<RedisZsetCore, RedisZset> implements WriteCommand {

    private BytesWrapper key;

    private List<BytesWrapper> members;

    @Override
    public String type() {
        return WriteCommandType.zrem.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore((RedisZsetCore) redisCore);
        this.key = ((BulkString) array.get(1)).getContent();
        this.members = Stream.of(array).skip(2).map(resp -> ((BulkString) resp).getContent()).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisZset redisZset = get(key);
        return new RespInt(redisZset.remove(members));
    }
}
