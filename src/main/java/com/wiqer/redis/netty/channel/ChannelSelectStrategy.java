package com.wiqer.redis.netty.channel;

/**
 * @author Administrator
 */
public interface ChannelSelectStrategy {
    LocalChannelOption select();
}
