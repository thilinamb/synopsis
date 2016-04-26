import ds.granules.communication.direct.dispatch.ControlMessageDispatcher;
import ds.granules.communication.direct.netty.server.MessageReceiver;
import ds.granules.communication.direct.netty.server.ServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;

import java.util.concurrent.CountDownLatch;

/**
 * @author Thilina Buddhika
 */
public class StatsServer {
    private Logger logger = Logger.getLogger(StatsServer.class);
    private int ctrlPort;
    private CountDownLatch dispatcherStarted = new CountDownLatch(1);

    private StatsServer(int ctrlPort) {
        this.ctrlPort = ctrlPort;
    }

    void notifyDispatcherStartup() {
        dispatcherStarted.countDown();
    }

    private boolean isDispatcherStarted() {
        try {
            dispatcherStarted.await();
            logger.info("Message Dispatcher has started.");
            return true;
        } catch (InterruptedException e) {
            logger.error("Error waiting till dispatcher start up!", e);
            return false;
        }
    }

    private void start() {
        EventLoopGroup ctrlBossGroup = new NioEventLoopGroup(1);
        EventLoopGroup ctrlWorkerGroup = new NioEventLoopGroup(1);

        ServerBootstrap ctrlBootstrap = new ServerBootstrap();
        ctrlBootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        ctrlBootstrap.group(ctrlBossGroup, ctrlWorkerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new MessageReceiver(),
                                new ServerHandler(ControlMessageDispatcher.getInstance()));
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        try {
            ChannelFuture future = ctrlBootstrap.bind(this.ctrlPort).sync();
            logger.info("StatServer is running on " + this.ctrlPort);
            future.channel().closeFuture();
            logger.info("Shutting down the stat server.");
        } catch (InterruptedException e) {
            logger.error("Error starting the statistics server. ", e);
        } finally {
            ctrlBossGroup.shutdownGracefully();
            ctrlWorkerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) {
        int port = 12345;
        if (args.length >= 1) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println(String.format("Error parsing provided port: %s, using the deault port.", args[0]));
            }
        }
        StatsServer server = new StatsServer(port);
        new Thread(new MessageDispatcher(server)).start();
        boolean isDispatcherStarted = server.isDispatcherStarted();
        if (isDispatcherStarted) {
            server.start();
        } else {
            System.err.println("Dispatcher did not start. Exiting!");
        }
    }
}
