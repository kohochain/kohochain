package io.fortest.core.net.messagehandler;

import static io.fortest.core.config.Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL;
import static io.fortest.core.config.Parameter.ChainConstant.BLOCK_SIZE;

import lombok.extern.slf4j.Slf4j;
import org.spongycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.BlockCapsule.BlockId;
import io.fortest.core.config.args.Args;
import io.fortest.core.exception.P2pException;
import io.fortest.core.exception.P2pException.TypeEnum;
import io.fortest.core.net.khcNetDelegate;
import io.fortest.core.net.message.BlockMessage;
import io.fortest.core.net.message.khcMessage;
import io.fortest.core.net.peer.Item;
import io.fortest.core.net.peer.PeerConnection;
import io.fortest.core.net.service.AdvService;
import io.fortest.core.net.service.SyncService;
import io.fortest.core.services.WitnessProductBlockService;
import io.fortest.protos.Protocol.Inventory.InventoryType;

@Slf4j(topic = "net")
@Component
public class BlockMsgHandler implements khcMsgHandler {

  @Autowired
  private khcNetDelegate khcNetDelegate;

  @Autowired
  private AdvService advService;

  @Autowired
  private SyncService syncService;

  @Autowired
  private WitnessProductBlockService witnessProductBlockService;

  private int maxBlockSize = BLOCK_SIZE + 1000;

  private boolean fastForward = Args.getInstance().isFastForward();

  @Override
  public void processMessage(PeerConnection peer, khcMessage msg) throws P2pException {

    BlockMessage blockMessage = (BlockMessage) msg;
    BlockId blockId = blockMessage.getBlockId();

    if (!fastForward && !peer.isFastForwardPeer()) {
      check(peer, blockMessage);
    }

    if (peer.getSyncBlockRequested().containsKey(blockId)) {
      peer.getSyncBlockRequested().remove(blockId);
      syncService.processBlock(peer, blockMessage);
    } else {
      Long time = peer.getAdvInvRequest().remove(new Item(blockId, InventoryType.BLOCK));
      long now = System.currentTimeMillis();
      long interval = blockId.getNum() - khcNetDelegate.getHeadBlockId().getNum();
      processBlock(peer, blockMessage.getBlockCapsule());
      logger.info(
          "Receive block/interval {}/{} from {} fetch/delay {}/{}ms, txs/process {}/{}ms, witness: {}",
          blockId.getNum(),
          interval,
          peer.getInetAddress(),
          time == null ? 0 : now - time,
          now - blockMessage.getBlockCapsule().getTimeStamp(),
          ((BlockMessage) msg).getBlockCapsule().getTransactions().size(),
          System.currentTimeMillis() - now,
          Hex.toHexString(blockMessage.getBlockCapsule().getWitnessAddress().toByteArray()));
    }
  }

  private void check(PeerConnection peer, BlockMessage msg) throws P2pException {
    Item item = new Item(msg.getBlockId(), InventoryType.BLOCK);
    if (!peer.getSyncBlockRequested().containsKey(msg.getBlockId()) && !peer.getAdvInvRequest()
        .containsKey(item)) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "no request");
    }
    BlockCapsule blockCapsule = msg.getBlockCapsule();
    if (blockCapsule.getInstance().getSerializedSize() > maxBlockSize) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "block size over limit");
    }
    long gap = blockCapsule.getTimeStamp() - System.currentTimeMillis();
    if (gap >= BLOCK_PRODUCED_INTERVAL) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "block time error");
    }
  }

  private void processBlock(PeerConnection peer, BlockCapsule block) throws P2pException {
    BlockId blockId = block.getBlockId();
    if (!khcNetDelegate.containBlock(block.getParentBlockId())) {
      logger.warn("Get unlink block {} from {}, head is {}.", blockId.getString(),
          peer.getInetAddress(), khcNetDelegate.getHeadBlockId().getString());
      syncService.startSync(peer);
      return;
    }

    Item item = new Item(blockId, InventoryType.BLOCK);
    if (fastForward || peer.isFastForwardPeer()) {
      peer.getAdvInvReceive().put(item, System.currentTimeMillis());
      advService.addInvToCache(item);
    }

    if (fastForward) {
      if (block.getNum() < khcNetDelegate.getHeadBlockId().getNum()) {
        logger.warn("Receive a low block {}, head {}",
            blockId.getString(), khcNetDelegate.getHeadBlockId().getString());
        return;
      }
      if (khcNetDelegate.validBlock(block)) {
        advService.fastForward(new BlockMessage(block));
        khcNetDelegate.trustNode(peer);
      }
    }

    khcNetDelegate.processBlock(block);
    witnessProductBlockService.validWitnessProductTwoBlock(block);
    khcNetDelegate.getActivePeer().forEach(p -> {
      if (p.getAdvInvReceive().getIfPresent(blockId) != null) {
        p.setBlockBothHave(blockId);
      }
    });

    if (!fastForward) {
      advService.broadcast(new BlockMessage(block));
    }
  }

}
