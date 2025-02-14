package com.wiqer.redis.datatype;

import lombok.EqualsAndHashCode;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author lilan
 */
@EqualsAndHashCode
public class BytesWrapper implements Comparable<BytesWrapper> {

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    public static final BytesWrapper ZERO = new BytesWrapper("0".getBytes(CHARSET));

    private final byte[] content;

    public BytesWrapper(byte[] content) {
        this.content = content.clone();
    }

    public byte[] getByteArray() {
        return content.clone();
    }

    public String toUtf8String() {
        return new String(content, CHARSET);
    }

    @Override
    public int compareTo(BytesWrapper o) {
        if (this == o) return 0;
        if (o == null) return 1;
        
        byte[] v2 = o.content;
        int len1 = content.length;
        int len2 = v2.length;
        int lim = Math.min(len1, len2);

        for (int k = 0; k < lim; k++) {
            byte c1 = content[k];
            byte c2 = v2[k];
            if (c1 != c2) {
                return c1 - c2;
            }
        }
        return len1 - len2;
    }
}
