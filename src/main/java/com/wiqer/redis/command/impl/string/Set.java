package com.wiqer.redis.command.impl.string;

import com.wiqer.redis.core.AbstractCore;
import com.wiqer.redis.core.RedisCore;
import com.wiqer.redis.core.RedisStringCore;
import com.wiqer.redis.command.WriteCommandType;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisString;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.SimpleString;

import java.util.List;

public class Set extends AbstractCore<RedisStringCore, RedisString> implements WriteCommand {

    private BytesWrapper key;
    private BytesWrapper value;
    private long timeout = -1;
    private boolean notExistSet = false;
    private boolean existSet = false;

    @Override
    public String type() {
        return WriteCommandType.set.name();
    }

    @Override
    public void init(RedisCore redisCore, List<Resp> array) {
        setRedisCore((RedisStringCore) redisCore);
        key = ((BulkString) array.get(1)).getContent();
        value = ((BulkString) array.get(2)).getContent();
        int index = 3;
        while (index < array.size()) {
            String string = ((BulkString) array.get(index)).getContent().toUtf8String();
            index++;
            if (string.startsWith("EX")) {
                String seconds = ((BulkString) array.get(index)).getContent().toUtf8String();
                timeout = Integer.parseInt(seconds) * 1000L;
            } else if (string.startsWith("PX")) {
                String seconds = ((BulkString) array.get(index)).getContent().toUtf8String();
                timeout = Integer.parseInt(seconds);
            } else if (string.equals("NX")) {
                notExistSet = true;
            } else if (string.equals("XX")) {
                existSet = true;
            }
        }
    }

    @Override
    public Resp handle() {
        if ((!notExistSet || !exist(key))
                && (!existSet || exist(key))) {
            if (timeout != -1) {
                timeout += System.currentTimeMillis();
            }
            RedisString stringData = new RedisString();
            stringData.setValue(value);
            stringData.setTimeout(timeout);
            put(key, stringData);
            return SimpleString.OK;
        }
        return BulkString.NullBulkString;
    }
}
