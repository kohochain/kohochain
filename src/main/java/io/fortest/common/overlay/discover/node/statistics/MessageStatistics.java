package io.fortest.common.overlay.discover.node.statistics;

import io.fortest.common.net.udp.message.UdpMessageTypeEnum;
import io.fortest.common.overlay.message.Message;
import lombok.extern.slf4j.Slf4j;
import io.fortest.core.net.message.FetchInvDataMessage;
import io.fortest.core.net.message.InventoryMessage;
import io.fortest.core.net.message.MessageTypes;
import io.fortest.core.net.message.TransactionsMessage;

@Slf4j
public class MessageStatistics {

  //udp discovery
  public final MessageCount discoverInPing = new MessageCount();
  public final MessageCount discoverOutPing = new MessageCount();
  public final MessageCount discoverInPong = new MessageCount();
  public final MessageCount discoverOutPong = new MessageCount();
  public final MessageCount discoverInFindNode = new MessageCount();
  public final MessageCount discoverOutFindNode = new MessageCount();
  public final MessageCount discoverInNeighbours = new MessageCount();
  public final MessageCount discoverOutNeighbours = new MessageCount();

  //tcp p2p
  public final MessageCount p2pInHello = new MessageCount();
  public final MessageCount p2pOutHello = new MessageCount();
  public final MessageCount p2pInPing = new MessageCount();
  public final MessageCount p2pOutPing = new MessageCount();
  public final MessageCount p2pInPong = new MessageCount();
  public final MessageCount p2pOutPong = new MessageCount();
  public final MessageCount p2pInDisconnect = new MessageCount();
  public final MessageCount p2pOutDisconnect = new MessageCount();

  //tcp khc
  public final MessageCount khcInMessage = new MessageCount();
  public final MessageCount khcOutMessage = new MessageCount();

  public final MessageCount khcInSyncBlockChain = new MessageCount();
  public final MessageCount khcOutSyncBlockChain = new MessageCount();
  public final MessageCount khcInBlockChainInventory = new MessageCount();
  public final MessageCount khcOutBlockChainInventory = new MessageCount();

  public final MessageCount khcInkhtInventory = new MessageCount();
  public final MessageCount khcOutkhtInventory = new MessageCount();
  public final MessageCount khcInkhtInventoryElement = new MessageCount();
  public final MessageCount khcOutkhtInventoryElement = new MessageCount();

  public final MessageCount khcInBlockInventory = new MessageCount();
  public final MessageCount khcOutBlockInventory = new MessageCount();
  public final MessageCount khcInBlockInventoryElement = new MessageCount();
  public final MessageCount khcOutBlockInventoryElement = new MessageCount();

  public final MessageCount khcInkhtFetchInvData = new MessageCount();
  public final MessageCount khcOutkhtFetchInvData = new MessageCount();
  public final MessageCount khcInkhtFetchInvDataElement = new MessageCount();
  public final MessageCount khcOutkhtFetchInvDataElement = new MessageCount();

  public final MessageCount khcInBlockFetchInvData = new MessageCount();
  public final MessageCount khcOutBlockFetchInvData = new MessageCount();
  public final MessageCount khcInBlockFetchInvDataElement = new MessageCount();
  public final MessageCount khcOutBlockFetchInvDataElement = new MessageCount();


  public final MessageCount khcInkht = new MessageCount();
  public final MessageCount khcOutkht = new MessageCount();
  public final MessageCount khcInkhts = new MessageCount();
  public final MessageCount khcOutkhts = new MessageCount();
  public final MessageCount khcInBlock = new MessageCount();
  public final MessageCount khcOutBlock = new MessageCount();
  public final MessageCount khcOutAdvBlock = new MessageCount();

  public void addUdpInMessage(UdpMessageTypeEnum type) {
    addUdpMessage(type, true);
  }

  public void addUdpOutMessage(UdpMessageTypeEnum type) {
    addUdpMessage(type, false);
  }

  public void addTcpInMessage(Message msg) {
    addTcpMessage(msg, true);
  }

  public void addTcpOutMessage(Message msg) {
    addTcpMessage(msg, false);
  }

  private void addUdpMessage(UdpMessageTypeEnum type, boolean flag) {
    switch (type) {
      case DISCOVER_PING:
        if (flag) {
          discoverInPing.add();
        } else {
          discoverOutPing.add();
        }
        break;
      case DISCOVER_PONG:
        if (flag) {
          discoverInPong.add();
        } else {
          discoverOutPong.add();
        }
        break;
      case DISCOVER_FIND_NODE:
        if (flag) {
          discoverInFindNode.add();
        } else {
          discoverOutFindNode.add();
        }
        break;
      case DISCOVER_NEIGHBORS:
        if (flag) {
          discoverInNeighbours.add();
        } else {
          discoverOutNeighbours.add();
        }
        break;
      default:
        break;
    }
  }

  private void addTcpMessage(Message msg, boolean flag) {

    if (flag) {
      khcInMessage.add();
    } else {
      khcOutMessage.add();
    }

    switch (msg.getType()) {
      case P2P_HELLO:
        if (flag) {
          p2pInHello.add();
        } else {
          p2pOutHello.add();
        }
        break;
      case P2P_PING:
        if (flag) {
          p2pInPing.add();
        } else {
          p2pOutPing.add();
        }
        break;
      case P2P_PONG:
        if (flag) {
          p2pInPong.add();
        } else {
          p2pOutPong.add();
        }
        break;
      case P2P_DISCONNECT:
        if (flag) {
          p2pInDisconnect.add();
        } else {
          p2pOutDisconnect.add();
        }
        break;
      case SYNC_BLOCK_CHAIN:
        if (flag) {
          khcInSyncBlockChain.add();
        } else {
          khcOutSyncBlockChain.add();
        }
        break;
      case BLOCK_CHAIN_INVENTORY:
        if (flag) {
          khcInBlockChainInventory.add();
        } else {
          khcOutBlockChainInventory.add();
        }
        break;
      case INVENTORY:
        InventoryMessage inventoryMessage = (InventoryMessage) msg;
        int inventorySize = inventoryMessage.getInventory().getIdsCount();
        if (flag) {
          if (inventoryMessage.getInvMessageType() == MessageTypes.kht) {
            khcInkhtInventory.add();
            khcInkhtInventoryElement.add(inventorySize);
          } else {
            khcInBlockInventory.add();
            khcInBlockInventoryElement.add(inventorySize);
          }
        } else {
          if (inventoryMessage.getInvMessageType() == MessageTypes.kht) {
            khcOutkhtInventory.add();
            khcOutkhtInventoryElement.add(inventorySize);
          } else {
            khcOutBlockInventory.add();
            khcOutBlockInventoryElement.add(inventorySize);
          }
        }
        break;
      case FETCH_INV_DATA:
        FetchInvDataMessage fetchInvDataMessage = (FetchInvDataMessage) msg;
        int fetchSize = fetchInvDataMessage.getInventory().getIdsCount();
        if (flag) {
          if (fetchInvDataMessage.getInvMessageType() == MessageTypes.kht) {
            khcInkhtFetchInvData.add();
            khcInkhtFetchInvDataElement.add(fetchSize);
          } else {
            khcInBlockFetchInvData.add();
            khcInBlockFetchInvDataElement.add(fetchSize);
          }
        } else {
          if (fetchInvDataMessage.getInvMessageType() == MessageTypes.kht) {
            khcOutkhtFetchInvData.add();
            khcOutkhtFetchInvDataElement.add(fetchSize);
          } else {
            khcOutBlockFetchInvData.add();
            khcOutBlockFetchInvDataElement.add(fetchSize);
          }
        }
        break;
      case khtS:
        TransactionsMessage transactionsMessage = (TransactionsMessage) msg;
        if (flag) {
          khcInkhts.add();
          khcInkht.add(transactionsMessage.getTransactions().getTransactionsCount());
        } else {
          khcOutkhts.add();
          khcOutkht.add(transactionsMessage.getTransactions().getTransactionsCount());
        }
        break;
      case kht:
        if (flag) {
          khcInMessage.add();
        } else {
          khcOutMessage.add();
        }
        break;
      case BLOCK:
        if (flag) {
          khcInBlock.add();
        }
        khcOutBlock.add();
        break;
      default:
        break;
    }
  }

}
