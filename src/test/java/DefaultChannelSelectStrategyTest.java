import com.wiqer.redis.netty.channel.DefaultChannelSelectStrategy;
import com.wiqer.redis.netty.channel.LocalChannelOption;
import com.wiqer.redis.netty.channel.epoll.EpollChannelOption;
import com.wiqer.redis.netty.channel.kqueue.KqueueChannelOption;
import com.wiqer.redis.netty.channel.select.NioSelectChannelOption;
import org.junit.Test;

public class DefaultChannelSelectStrategyTest {
    DefaultChannelSelectStrategy selectStrategy = new DefaultChannelSelectStrategy();

    @Test
    public void testChannelSelect() {
        LocalChannelOption localChannelOption = selectStrategy.select();
        System.out.println("KqueueChannelOption:" + (localChannelOption instanceof KqueueChannelOption));
        System.out.println("EpollChannelOption:" + (localChannelOption instanceof EpollChannelOption));
        System.out.println("NioSelectChannelOption:" + (localChannelOption instanceof NioSelectChannelOption));

    }
}
