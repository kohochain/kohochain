package io.fortest.common.overlay.server;

import io.fortest.core.config.args.Args;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Slf4j(topic = "net")
@Component
public class PeerServer {

  private Args args = Args.getInstance();

  private ApplicationContext ctx;

  private boolean listening;

  private ChannelFuture channelFuture;

  @Autowired
  public PeerServer(final Args args, final ApplicationContext ctx) {
    this.ctx = ctx;
  }

  public void start(int port) {

    EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    EventLoopGroup workerGroup = new NioEventLoopGroup(args.getTcpNettyWorkThreadNum());
    khcChannelInitializer khcChannelInitializer = ctx.getBean(khcChannelInitializer.class, "");

    try {
      ServerBootstrap b = new ServerBootstrap();

      b.group(bossGroup, workerGroup);
      b.channel(NioServerSocketChannel.class);

      b.option(ChannelOption.SO_KEEPALIVE, true);
      b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, DefaultMessageSizeEstimator.DEFAULT);
      b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, this.args.getNodeConnectionTimeout());

      b.handler(new LoggingHandler());
      b.childHandler(khcChannelInitializer);

      // Start the client.
      logger.info("TCP listener started, bind port {}", port);

      channelFuture = b.bind(port).sync();

      listening = true;

      // Wait until the connection is closed.
      channelFuture.channel().closeFuture().sync();

      logger.info("TCP listener is closed");

    } catch (Exception e) {
      logger.error("Start TCP server failed.", e);
    } finally {
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
      listening = false;
    }
  }

  public void close() {
    if (listening && channelFuture != null && channelFuture.channel().isOpen()) {
      try {
        logger.info("Closing TCP server...");
        channelFuture.channel().close().sync();
      } catch (Exception e) {
        logger.warn("Closing TCP server failed.", e);
      }
    }
  }
}
