package io.fortest.core.net;

import io.fortest.common.overlay.message.Message;
import io.fortest.common.overlay.server.ChannelManager;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.P2pException;
import io.fortest.core.net.service.AdvService;
import io.fortest.protos.Protocol;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.fortest.core.net.message.BlockMessage;
import io.fortest.core.net.message.khcMessage;
import io.fortest.core.net.messagehandler.BlockMsgHandler;
import io.fortest.core.net.messagehandler.ChainInventoryMsgHandler;
import io.fortest.core.net.messagehandler.FetchInvDataMsgHandler;
import io.fortest.core.net.messagehandler.InventoryMsgHandler;
import io.fortest.core.net.messagehandler.SyncBlockChainMsgHandler;
import io.fortest.core.net.messagehandler.TransactionsMsgHandler;
import io.fortest.core.net.peer.PeerConnection;
import io.fortest.core.net.peer.PeerStatusCheck;
import io.fortest.core.net.service.SyncService;

@Slf4j(topic = "net")
@Component
public class khcNetService {

  @Autowired
  private ChannelManager channelManager;

  @Autowired
  private AdvService advService;

  @Autowired
  private SyncService syncService;

  @Autowired
  private PeerStatusCheck peerStatusCheck;

  @Autowired
  private SyncBlockChainMsgHandler syncBlockChainMsgHandler;

  @Autowired
  private ChainInventoryMsgHandler chainInventoryMsgHandler;

  @Autowired
  private InventoryMsgHandler inventoryMsgHandler;


  @Autowired
  private FetchInvDataMsgHandler fetchInvDataMsgHandler;

  @Autowired
  private BlockMsgHandler blockMsgHandler;

  @Autowired
  private TransactionsMsgHandler transactionsMsgHandler;

  @Autowired
  private Manager manager;

  public void start() {
    manager.setkhcNetService(this);
    channelManager.init();
    advService.init();
    syncService.init();
    peerStatusCheck.init();
    transactionsMsgHandler.init();
    logger.info("khcNetService start successfully.");
  }

  public void close() {
    channelManager.close();
    advService.close();
    syncService.close();
    peerStatusCheck.close();
    transactionsMsgHandler.close();
    logger.info("khcNetService closed successfully.");
  }

  public void broadcast(Message msg) {
    advService.broadcast(msg);
  }

  public void fastForward(BlockMessage msg) {
    advService.fastForward(msg);
  }

  protected void onMessage(PeerConnection peer, khcMessage msg) {
    try {
      switch (msg.getType()) {
        case SYNC_BLOCK_CHAIN:
          syncBlockChainMsgHandler.processMessage(peer, msg);
          break;
        case BLOCK_CHAIN_INVENTORY:
          chainInventoryMsgHandler.processMessage(peer, msg);
          break;
        case INVENTORY:
          inventoryMsgHandler.processMessage(peer, msg);
          break;
        case FETCH_INV_DATA:
          fetchInvDataMsgHandler.processMessage(peer, msg);
          break;
        case BLOCK:
          blockMsgHandler.processMessage(peer, msg);
          break;
        case khtS:
          transactionsMsgHandler.processMessage(peer, msg);
          break;
        default:
          throw new P2pException(P2pException.TypeEnum.NO_SUCH_MESSAGE, msg.getType().toString());
      }
    } catch (Exception e) {
      processException(peer, msg, e);
    }
  }

  private void processException(PeerConnection peer, khcMessage msg, Exception ex) {
    Protocol.ReasonCode code;

    if (ex instanceof P2pException) {
      P2pException.TypeEnum type = ((P2pException) ex).getType();
      switch (type) {
        case BAD_kht:
          code = Protocol.ReasonCode.BAD_TX;
          break;
        case BAD_BLOCK:
          code = Protocol.ReasonCode.BAD_BLOCK;
          break;
        case NO_SUCH_MESSAGE:
        case MESSAGE_WITH_WRONG_LENGTH:
        case BAD_MESSAGE:
          code = Protocol.ReasonCode.BAD_PROTOCOL;
          break;
        case SYNC_FAILED:
          code = Protocol.ReasonCode.SYNC_FAIL;
          break;
        case UNLINK_BLOCK:
          code = Protocol.ReasonCode.UNLINKABLE;
          break;
        default:
          code = Protocol.ReasonCode.UNKNOWN;
          break;
      }
      logger.error("Message from {} process failed, {} \n type: {}, detail: {}.",
          peer.getInetAddress(), msg, type, ex.getMessage());
    } else {
      code = Protocol.ReasonCode.UNKNOWN;
      logger.error("Message from {} process failed, {}",
          peer.getInetAddress(), msg, ex);
    }

    peer.disconnect(code);
  }
}
