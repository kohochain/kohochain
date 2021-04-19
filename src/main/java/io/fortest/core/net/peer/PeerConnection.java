package io.fortest.core.net.peer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import io.fortest.common.overlay.message.HelloMessage;
import io.fortest.common.overlay.message.Message;
import io.fortest.common.overlay.server.Channel;
import io.fortest.common.utils.Sha256Hash;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.config.Parameter;
import io.fortest.core.net.service.AdvService;
import javafx.util.Pair;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import io.fortest.core.net.khcNetDelegate;
import io.fortest.core.net.service.SyncService;

@Slf4j(topic = "net")
@Component
@Scope("prototype")
public class PeerConnection extends Channel {

  @Autowired
  private khcNetDelegate khcNetDelegate;

  @Autowired
  private SyncService syncService;

  @Autowired
  private AdvService advService;

  @Setter
  @Getter
  private HelloMessage helloMessage;

  private int invCacheSize = 100_000;

  @Setter
  @Getter
  private Cache<Item, Long> advInvReceive = CacheBuilder.newBuilder().maximumSize(invCacheSize)
      .expireAfterWrite(1, TimeUnit.HOURS).recordStats().build();

  @Setter
  @Getter
  private Cache<Item, Long> advInvSpread = CacheBuilder.newBuilder().maximumSize(invCacheSize)
      .expireAfterWrite(1, TimeUnit.HOURS).recordStats().build();

  @Setter
  @Getter
  private Map<Item, Long> advInvRequest = new ConcurrentHashMap<>();

  @Setter
  private BlockCapsule.BlockId fastForwardBlock;

  @Getter
  private BlockCapsule.BlockId blockBothHave = new BlockCapsule.BlockId();

  public void setBlockBothHave(BlockCapsule.BlockId blockId) {
    this.blockBothHave = blockId;
    this.blockBothHaveUpdateTime = System.currentTimeMillis();
  }

  @Getter
  private volatile long blockBothHaveUpdateTime = System.currentTimeMillis();

  @Setter
  @Getter
  private BlockCapsule.BlockId lastSyncBlockId;

  @Setter
  @Getter
  private volatile long remainNum;

  @Getter
  private Cache<Sha256Hash, Long> syncBlockIdCache = CacheBuilder.newBuilder()
      .maximumSize(2 * Parameter.NodeConstant.SYNC_FETCH_BATCH_NUM).recordStats().build();

  @Setter
  @Getter
  private Deque<BlockCapsule.BlockId> syncBlockToFetch = new ConcurrentLinkedDeque<>();

  @Setter
  @Getter
  private Map<BlockCapsule.BlockId, Long> syncBlockRequested = new ConcurrentHashMap<>();

  @Setter
  @Getter
  private Pair<Deque<BlockCapsule.BlockId>, Long> syncChainRequested = null;

  @Setter
  @Getter
  private Set<BlockCapsule.BlockId> syncBlockInProcess = new HashSet<>();

  @Setter
  @Getter
  private volatile boolean needSyncFromPeer;

  @Setter
  @Getter
  private volatile boolean needSyncFromUs;

  public boolean isIdle() {
    return advInvRequest.isEmpty() && syncBlockRequested.isEmpty() && syncChainRequested == null;
  }

  public void sendMessage(Message message) {
    msgQueue.sendMessage(message);
  }

  public void onConnect() {
    if (getHelloMessage().getHeadBlockId().getNum() > khcNetDelegate.getHeadBlockId().getNum()) {
      setkhcState(khcState.SYNCING);
      syncService.startSync(this);
    } else {
      setkhcState(khcState.SYNC_COMPLETED);
    }
  }

  public void onDisconnect() {
    syncService.onDisconnect(this);
    advService.onDisconnect(this);
    advInvReceive.cleanUp();
    advInvSpread.cleanUp();
    advInvRequest.clear();
    syncBlockIdCache.cleanUp();
    syncBlockToFetch.clear();
    syncBlockRequested.clear();
    syncBlockInProcess.clear();
    syncBlockInProcess.clear();
  }

  public String log() {
    long now = System.currentTimeMillis();
//    logger.info("Peer {}:{} [ {}, ping {} ms]-----------\n"
//            + "connect time: {}\n"
//            + "last know block num: {}\n"
//            + "needSyncFromPeer:{}\n"
//            + "needSyncFromUs:{}\n"
//            + "syncToFetchSize:{}\n"
//            + "syncToFetchSizePeekNum:{}\n"
//            + "syncBlockRequestedSize:{}\n"
//            + "remainNum:{}\n"
//            + "syncChainRequested:{}\n"
//            + "blockInProcess:{}\n"
//            + "{}",
//        this.getNode().getHost(), this.getNode().getPort(), this.getNode().getHexIdShort(),
//        (int) this.getPeerStats().getAvgLatency(),
//        (now - super.getStartTime()) / 1000,
//        blockBothHave.getNum(),
//        isNeedSyncFromPeer(),
//        isNeedSyncFromUs(),
//        syncBlockToFetch.size(),
//        syncBlockToFetch.size() > 0 ? syncBlockToFetch.peek().getNum() : -1,
//        syncBlockRequested.size(),
//        remainNum,
//        syncChainRequested == null ? 0 : (now - syncChainRequested.getValue()) / 1000,
//        syncBlockInProcess.size(),
//        nodeStatistics.toString());
////
    return String.format(
        "Peer %s [%8s]\n"
            + "ping msg: count %d, max-average-min-last: %d %d %d %d\n"
            + "connect time: %ds\n"
            + "last know block num: %s\n"
            + "needSyncFromPeer:%b\n"
            + "needSyncFromUs:%b\n"
            + "syncToFetchSize:%d\n"
            + "syncToFetchSizePeekNum:%d\n"
            + "syncBlockRequestedSize:%d\n"
            + "remainNum:%d\n"
            + "syncChainRequested:%d\n"
            + "blockInProcess:%d\n",
        getNode().getHost() + ":" + getNode().getPort(),
        getNode().getHexIdShort(),

        getNodeStatistics().pingMessageLatency.getCount(),
        getNodeStatistics().pingMessageLatency.getMax(),
        getNodeStatistics().pingMessageLatency.getAvrg(),
        getNodeStatistics().pingMessageLatency.getMin(),
        getNodeStatistics().pingMessageLatency.getLast(),

        (now - getStartTime()) / 1000,
        fastForwardBlock != null ? fastForwardBlock.getNum() : blockBothHave.getNum(),
        isNeedSyncFromPeer(),
        isNeedSyncFromUs(),
        syncBlockToFetch.size(),
        syncBlockToFetch.size() > 0 ? syncBlockToFetch.peek().getNum() : -1,
        syncBlockRequested.size(),
        remainNum,
        syncChainRequested == null ? 0 : (now - syncChainRequested.getValue()) / 1000,
        syncBlockInProcess.size())
        + nodeStatistics.toString() + "\n";
  }

}
