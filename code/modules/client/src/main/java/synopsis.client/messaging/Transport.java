package synopsis.client.messaging;

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
public class Transport implements Runnable{

    private Logger logger = Logger.getLogger(Transport.class);
    private final int ctrlPort;
    private final CountDownLatch startedFlag;

    public Transport(int ctrlPort, CountDownLatch startedFlag) {
        this.ctrlPort = ctrlPort;
        this.startedFlag = startedFlag;
    }

    @Override
    public void run() {
        start();
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
        logger.info("Trying to bind to port: " + ctrlPort);
        try {
            ChannelFuture future = ctrlBootstrap.bind(this.ctrlPort).sync();
            logger.info("Synopsis client is listening on " + this.ctrlPort);
            startedFlag.countDown();
            future.channel().closeFuture().sync();
            logger.info("Shutting down the transport module of Synopsis client.");
        } catch (InterruptedException e) {
            logger.error("Error starting the transport module Synopsis client. ", e);
        } finally {
            ctrlBossGroup.shutdownGracefully();
            ctrlWorkerGroup.shutdownGracefully();
        }

    }
}
