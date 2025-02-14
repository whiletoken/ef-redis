package com.wiqer.redis.netty.channel;

import com.wiqer.redis.netty.channel.epoll.EpollChannelOption;
import com.wiqer.redis.netty.channel.kqueue.KqueueChannelOption;
import com.wiqer.redis.netty.channel.select.NioSelectChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;

public class DefaultChannelSelectStrategy implements ChannelSelectStrategy {
    @Override
    public LocalChannelOption select() {

        if (KQueue.isAvailable()) {
            return new KqueueChannelOption();
        }
        if (Epoll.isAvailable()) {
            return new EpollChannelOption();
        }
        return new NioSelectChannelOption();
    }
}
