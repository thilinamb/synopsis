package synopsis.statserver;

import ds.granules.communication.direct.dispatch.ControlMessageDispatcher;
import ds.granules.communication.direct.netty.server.MessageReceiver;
import ds.granules.communication.direct.netty.server.ServerHandler;
import ds.granules.util.Constants;
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
import synopsis.statserver.processors.CumulProcessedMsgCountProcessor;
import synopsis.statserver.processors.CumulThroughputProcessor;
import synopsis.statserver.processors.CumulativeMemoryUsageProcessor;

import java.util.concurrent.CountDownLatch;

/**
 * Statistics server: launches a server endpoint
 *
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
        init();

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
            logger.info("StatServer is running on " + this.ctrlPort);
            future.channel().closeFuture().sync();
            logger.info("Shutting down the stat server.");
            StatRegistry.getInstance().shutDown();
        } catch (InterruptedException e) {
            logger.error("Error starting the statistics server. ", e);
        } finally {
            ctrlBossGroup.shutdownGracefully();
            ctrlWorkerGroup.shutdownGracefully();
        }

    }

    private void init(){
        // register processors
        try {
            StatRegistry.getInstance().registerProcessor(new CumulativeMemoryUsageProcessor());
            StatRegistry.getInstance().registerProcessor(new CumulProcessedMsgCountProcessor());
            StatRegistry.getInstance().registerProcessor(new CumulThroughputProcessor());
            // start the dispatcher
            MessageDispatcher dispatcher = new MessageDispatcher(this);
            new Thread(dispatcher).start();
            boolean isDispatcherStarted = this.isDispatcherStarted();
            ControlMessageDispatcher.getInstance().registerCallback(Constants.WILD_CARD_CALLBACK, dispatcher);
            // ready to accept traffic
            if (isDispatcherStarted) {
                logger.info("Dispatcher start up.");
            } else {
                logger.warn("Dispatcher did not start. Exiting!");
            }
        } catch (StatServerException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int port = 12345;
        if (args.length >= 1) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println(String.format("Error parsing provided port: %s, using the deault port.", args[0]));
            }
        }
        StatsServer server = new StatsServer(port);
        server.start();
    }
}
