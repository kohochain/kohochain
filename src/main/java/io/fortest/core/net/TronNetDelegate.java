package io.fortest.core.net;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.fortest.common.overlay.message.Message;
import io.fortest.common.overlay.server.ChannelManager;
import io.fortest.common.overlay.server.SyncPool;
import io.fortest.common.utils.Sha256Hash;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.core.db.Manager;
import io.fortest.core.db.WitnessScheduleStore;
import io.fortest.core.net.peer.PeerConnection;
import io.fortest.protos.Protocol;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.spongycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.fortest.core.exception.AccountResourceInsufficientException;
import io.fortest.core.exception.BadBlockException;
import io.fortest.core.exception.BadItemException;
import io.fortest.core.exception.BadNumberBlockException;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractSizeNotEqualToOneException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.core.exception.DupTransactionException;
import io.fortest.core.exception.ItemNotFoundException;
import io.fortest.core.exception.NonCommonBlockException;
import io.fortest.core.exception.P2pException;
import io.fortest.core.exception.ReceiptCheckErrException;
import io.fortest.core.exception.StoreException;
import io.fortest.core.exception.TaposException;
import io.fortest.core.exception.TooBigTransactionException;
import io.fortest.core.exception.TooBigTransactionResultException;
import io.fortest.core.exception.TransactionExpirationException;
import io.fortest.core.exception.UnLinkedBlockException;
import io.fortest.core.exception.VMIllegalException;
import io.fortest.core.exception.ValidateScheduleException;
import io.fortest.core.exception.ValidateSignatureException;
import io.fortest.core.net.message.BlockMessage;
import io.fortest.core.net.message.MessageTypes;
import io.fortest.core.net.message.TransactionMessage;

@Slf4j(topic = "net")
@Component
public class khcNetDelegate {

  @Autowired
  private SyncPool syncPool;

  @Autowired
  private ChannelManager channelManager;

  @Autowired
  private Manager dbManager;

  @Autowired
  private WitnessScheduleStore witnessScheduleStore;

  @Getter
  private Object blockLock = new Object();

  private int blockIdCacheSize = 100;

  private Queue<BlockCapsule.BlockId> freshBlockId = new ConcurrentLinkedQueue<BlockCapsule.BlockId>() {
    @Override
    public boolean offer(BlockCapsule.BlockId blockId) {
      if (size() > blockIdCacheSize) {
        super.poll();
      }
      return super.offer(blockId);
    }
  };

  public void trustNode (PeerConnection peer) {
    channelManager.getTrustNodes().put(peer.getInetAddress(), peer.getNode());
  }

  public Collection<PeerConnection> getActivePeer() {
    return syncPool.getActivePeers();
  }

  public long getSyncBeginNumber() {
    return dbManager.getSyncBeginNumber();
  }

  public long getBlockTime(BlockCapsule.BlockId id) throws P2pException {
    try {
      return dbManager.getBlockById(id).getTimeStamp();
    } catch (BadItemException | ItemNotFoundException e) {
      throw new P2pException(P2pException.TypeEnum.DB_ITEM_NOT_FOUND, id.getString());
    }
  }

  public BlockCapsule.BlockId getHeadBlockId() {
    return dbManager.getHeadBlockId();
  }

  public BlockCapsule.BlockId getSolidBlockId() {
    return dbManager.getSolidBlockId();
  }

  public BlockCapsule.BlockId getGenesisBlockId() {
    return dbManager.getGenesisBlockId();
  }

  public BlockCapsule.BlockId getBlockIdByNum(long num) throws P2pException {
    try {
      return dbManager.getBlockIdByNum(num);
    } catch (ItemNotFoundException e) {
      throw new P2pException(P2pException.TypeEnum.DB_ITEM_NOT_FOUND, "num: " + num);
    }
  }

  public BlockCapsule getGenesisBlock() {
    return dbManager.getGenesisBlock();
  }

  public long getHeadBlockTimeStamp() {
    return dbManager.getHeadBlockTimeStamp();
  }

  public boolean containBlock(BlockCapsule.BlockId id) {
    return dbManager.containBlock(id);
  }

  public boolean containBlockInMainChain(BlockCapsule.BlockId id) {
    return dbManager.containBlockInMainChain(id);
  }

  public LinkedList<BlockCapsule.BlockId> getBlockChainHashesOnFork(BlockCapsule.BlockId forkBlockHash) throws P2pException {
    try {
      return dbManager.getBlockChainHashesOnFork(forkBlockHash);
    } catch (NonCommonBlockException e) {
      throw new P2pException(P2pException.TypeEnum.HARD_FORKED, forkBlockHash.getString());
    }
  }

  public boolean canChainRevoke(long num) {
    return num >= dbManager.getSyncBeginNumber();
  }

  public boolean contain(Sha256Hash hash, MessageTypes type) {
    if (type.equals(MessageTypes.BLOCK)) {
      return dbManager.containBlock(hash);
    } else if (type.equals(MessageTypes.kht)) {
      return dbManager.getTransactionStore().has(hash.getBytes());
    }
    return false;
  }

  public Message getData(Sha256Hash hash, Protocol.Inventory.InventoryType type) throws P2pException {
    try {
      switch (type) {
        case BLOCK:
          return new BlockMessage(dbManager.getBlockById(hash));
        case kht:
          TransactionCapsule tx = dbManager.getTransactionStore().get(hash.getBytes());
          if (tx != null) {
            return new TransactionMessage(tx.getInstance());
          }
          throw new StoreException();
        default:
          throw new StoreException();
      }
    } catch (StoreException e) {
      throw new P2pException(P2pException.TypeEnum.DB_ITEM_NOT_FOUND,
          "type: " + type + ", hash: " + hash.getByteString());
    }
  }

  public void processBlock(BlockCapsule block) throws P2pException {
    synchronized (blockLock) {
      try {
        if (!freshBlockId.contains(block.getBlockId())) {
          if (block.getNum() <= getHeadBlockId().getNum()) {
            logger.warn("Receive a fork block {} witness {}, head {}",
                block.getBlockId().getString(),
                Hex.toHexString(block.getWitnessAddress().toByteArray()),
                getHeadBlockId().getString());
          }
          dbManager.pushBlock(block);
          freshBlockId.add(block.getBlockId());
          logger.info("Success process block {}.", block.getBlockId().getString());
        }
      } catch (ValidateSignatureException
          | ContractValidateException
          | ContractExeException
          | UnLinkedBlockException
          | ValidateScheduleException
          | AccountResourceInsufficientException
          | TaposException
          | TooBigTransactionException
          | TooBigTransactionResultException
          | DupTransactionException
          | TransactionExpirationException
          | BadNumberBlockException
          | BadBlockException
          | NonCommonBlockException
          | ReceiptCheckErrException
          | VMIllegalException e) {
        throw new P2pException(P2pException.TypeEnum.BAD_BLOCK, e);
      }
    }
  }

  public void pushTransaction(TransactionCapsule kht) throws P2pException {
    try {
      dbManager.pushTransaction(kht);
    } catch (ContractSizeNotEqualToOneException
        | VMIllegalException e) {
      throw new P2pException(P2pException.TypeEnum.BAD_kht, e);
    } catch (ContractValidateException
        | ValidateSignatureException
        | ContractExeException
        | DupTransactionException
        | TaposException
        | TooBigTransactionException
        | TransactionExpirationException
        | ReceiptCheckErrException
        | TooBigTransactionResultException
        | AccountResourceInsufficientException e) {
      throw new P2pException(P2pException.TypeEnum.kht_EXE_FAILED, e);
    }
  }

  public boolean validBlock(BlockCapsule block) throws P2pException {
    try {
      return witnessScheduleStore.getActiveWitnesses().contains(block.getWitnessAddress())
          && block.validateSignature(dbManager);
    } catch (ValidateSignatureException e) {
      throw new P2pException(P2pException.TypeEnum.BAD_BLOCK, e);
    }
  }
}
