package io.fortest.core.net.peer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.fortest.core.config.Parameter;
import io.fortest.core.net.khcNetDelegate;
import io.fortest.protos.Protocol;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j(topic = "net")
@Component
public class PeerStatusCheck {

  @Autowired
  private khcNetDelegate khcNetDelegate;

  private ScheduledExecutorService peerStatusCheckExecutor = Executors
      .newSingleThreadScheduledExecutor();

  private int blockUpdateTimeout = 30_000;

  public void init() {
    peerStatusCheckExecutor.scheduleWithFixedDelay(() -> {
      try {
        statusCheck();
      } catch (Throwable t) {
        logger.error("Unhandled exception", t);
      }
    }, 5, 2, TimeUnit.SECONDS);
  }

  public void close() {
    peerStatusCheckExecutor.shutdown();
  }

  public void statusCheck() {

    long now = System.currentTimeMillis();

    khcNetDelegate.getActivePeer().forEach(peer -> {

      boolean isDisconnected = false;

      if (peer.isNeedSyncFromPeer()
          && peer.getBlockBothHaveUpdateTime() < now - blockUpdateTimeout) {
        logger.warn("Peer {} not sync for a long time.", peer.getInetAddress());
        isDisconnected = true;
      }

      if (!isDisconnected) {
        isDisconnected = peer.getAdvInvRequest().values().stream()
            .anyMatch(time -> time < now - Parameter.NetConstants.ADV_TIME_OUT);
      }

      if (!isDisconnected) {
        isDisconnected = peer.getSyncBlockRequested().values().stream()
            .anyMatch(time -> time < now - Parameter.NetConstants.SYNC_TIME_OUT);
      }

      if (isDisconnected) {
        peer.disconnect(Protocol.ReasonCode.TIME_OUT);
      }
    });
  }

}
