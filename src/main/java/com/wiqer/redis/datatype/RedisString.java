package com.wiqer.redis.datatype;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author lilan
 */
@Getter
@Setter
@NoArgsConstructor
public class RedisString extends RedisData {

    private BytesWrapper value;

    public RedisString(BytesWrapper value) {
        this.value = value;
    }

    public static RedisString ZERO() {
        RedisString stringData = new RedisString();
        stringData.setValue(BytesWrapper.ZERO);
        return stringData;
    }
}
