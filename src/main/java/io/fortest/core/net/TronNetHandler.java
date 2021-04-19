package io.fortest.core.net;

import io.fortest.common.overlay.server.Channel;
import io.fortest.common.overlay.server.MessageQueue;
import io.fortest.core.net.peer.PeerConnection;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import io.fortest.core.net.message.khcMessage;

@Component
@Scope("prototype")
public class khcNetHandler extends SimpleChannelInboundHandler<khcMessage> {

  protected PeerConnection peer;

  private MessageQueue msgQueue;

  @Autowired
  private khcNetService khcNetService;

//  @Autowired
//  private khcNetHandler (final ApplicationContext ctx){
//    khcNetService = ctx.getBean(khcNetService.class);
//  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, khcMessage msg) throws Exception {
    msgQueue.receivedMessage(msg);
    khcNetService.onMessage(peer, msg);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    peer.processException(cause);
  }

  public void setMsgQueue(MessageQueue msgQueue) {
    this.msgQueue = msgQueue;
  }

  public void setChannel(Channel channel) {
    this.peer = (PeerConnection) channel;
  }

}