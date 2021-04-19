package io.fortest.core.net.messagehandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.fortest.core.config.args.Args;
import io.fortest.core.exception.P2pException;
import io.fortest.core.exception.P2pException.TypeEnum;
import io.fortest.core.net.khcNetDelegate;
import io.fortest.core.net.message.TransactionMessage;
import io.fortest.core.net.message.TransactionsMessage;
import io.fortest.core.net.message.khcMessage;
import io.fortest.core.net.peer.Item;
import io.fortest.core.net.service.AdvService;
import io.fortest.core.net.peer.PeerConnection;
import io.fortest.protos.Protocol.Inventory.InventoryType;
import io.fortest.protos.Protocol.ReasonCode;
import io.fortest.protos.Protocol.Transaction;
import io.fortest.protos.Protocol.Transaction.Contract.ContractType;

@Slf4j(topic = "net")
@Component
public class TransactionsMsgHandler implements khcMsgHandler {

  @Autowired
  private khcNetDelegate khcNetDelegate;

  @Autowired
  private AdvService advService;

  private static int MAX_kht_SIZE = 50_000;

  private static int MAX_SMART_CONTRACT_SUBMIT_SIZE = 100;

//  private static int TIME_OUT = 10 * 60 * 1000;

  private BlockingQueue<khtEvent> smartContractQueue = new LinkedBlockingQueue(MAX_kht_SIZE);

  private BlockingQueue<Runnable> queue = new LinkedBlockingQueue();

  private int threadNum = Args.getInstance().getValidateSignThreadNum();
  private ExecutorService khtHandlePool = new ThreadPoolExecutor(threadNum, threadNum, 0L,
      TimeUnit.MILLISECONDS, queue);

  private ScheduledExecutorService smartContractExecutor = Executors
      .newSingleThreadScheduledExecutor();

  class khtEvent {

    @Getter
    private PeerConnection peer;
    @Getter
    private TransactionMessage msg;
    @Getter
    private long time;

    public khtEvent(PeerConnection peer, TransactionMessage msg) {
      this.peer = peer;
      this.msg = msg;
      this.time = System.currentTimeMillis();
    }
  }

  public void init() {
    handleSmartContract();
  }

  public void close() {
    smartContractExecutor.shutdown();
  }

  public boolean isBusy() {
    return queue.size() + smartContractQueue.size() > MAX_kht_SIZE;
  }

  @Override
  public void processMessage(PeerConnection peer, khcMessage msg) throws P2pException {
    TransactionsMessage transactionsMessage = (TransactionsMessage) msg;
    check(peer, transactionsMessage);
    for (Transaction kht : transactionsMessage.getTransactions().getTransactionsList()) {
      int type = kht.getRawData().getContract(0).getType().getNumber();
      if (type == ContractType.TriggerSmartContract_VALUE
          || type == ContractType.CreateSmartContract_VALUE) {
        if (!smartContractQueue.offer(new khtEvent(peer, new TransactionMessage(kht)))) {
          logger.warn("Add smart contract failed, queueSize {}:{}", smartContractQueue.size(),
              queue.size());
        }
      } else {
        khtHandlePool.submit(() -> handleTransaction(peer, new TransactionMessage(kht)));
      }
    }
  }

  private void check(PeerConnection peer, TransactionsMessage msg) throws P2pException {
    for (Transaction kht : msg.getTransactions().getTransactionsList()) {
      Item item = new Item(new TransactionMessage(kht).getMessageId(), InventoryType.kht);
      if (!peer.getAdvInvRequest().containsKey(item)) {
        throw new P2pException(TypeEnum.BAD_MESSAGE,
            "kht: " + msg.getMessageId() + " without request.");
      }
      peer.getAdvInvRequest().remove(item);
    }
  }

  private void handleSmartContract() {
    smartContractExecutor.scheduleWithFixedDelay(() -> {
      try {
        while (queue.size() < MAX_SMART_CONTRACT_SUBMIT_SIZE) {
          khtEvent event = smartContractQueue.take();
          khtHandlePool.submit(() -> handleTransaction(event.getPeer(), event.getMsg()));
        }
      } catch (Exception e) {
        logger.error("Handle smart contract exception.", e);
      }
    }, 1000, 20, TimeUnit.MILLISECONDS);
  }

  private void handleTransaction(PeerConnection peer, TransactionMessage kht) {
    if (peer.isDisconnect()) {
      logger.warn("Drop kht {} from {}, peer is disconnect.", kht.getMessageId(),
          peer.getInetAddress());
      return;
    }

    if (advService.getMessage(new Item(kht.getMessageId(), InventoryType.kht)) != null) {
      return;
    }

    try {
      khcNetDelegate.pushTransaction(kht.getTransactionCapsule());
      advService.broadcast(kht);
    } catch (P2pException e) {
      logger.warn("kht {} from peer {} process failed. type: {}, reason: {}",
          kht.getMessageId(), peer.getInetAddress(), e.getType(), e.getMessage());
      if (e.getType().equals(TypeEnum.BAD_kht)) {
        peer.disconnect(ReasonCode.BAD_TX);
      }
    } catch (Exception e) {
      logger.error("kht {} from peer {} process failed.", kht.getMessageId(), peer.getInetAddress(),
          e);
    }
  }
}