package com.wiqer.redis.datatype;

import lombok.EqualsAndHashCode;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author lilan
 */
@EqualsAndHashCode
public class BytesWrapper implements Comparable<BytesWrapper> {

    static final Charset CHARSET = StandardCharsets.UTF_8;

    private final byte[] content;

    public static final BytesWrapper ZERO = new BytesWrapper("0".getBytes(UTF_8));

    public BytesWrapper(byte[] content) {
        this.content = content;
    }

    public byte[] getByteArray() {
        return content;
    }

    public String toUtf8String() {
        return new String(content, CHARSET);
    }

    @Override
    public int compareTo(BytesWrapper o) {
        int len1 = content.length;
        int len2 = o.getByteArray().length;
        int lim = Math.min(len1, len2);
        byte[] v2 = o.getByteArray();

        int k = 0;
        while (k < lim) {
            byte c1 = content[k];
            byte c2 = v2[k];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return len1 - len2;
    }
}
