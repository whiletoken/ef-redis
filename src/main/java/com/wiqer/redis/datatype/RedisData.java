package com.wiqer.redis.datatype;

import lombok.Setter;

/**
 * @author lilan
 */
@Setter
public class RedisData {

    private long timeout = -1;

    public long timeout() {
        return timeout;
    }

}
