package io.fortest.core.net.message;

import java.util.List;

import io.fortest.common.utils.Sha256Hash;
import io.fortest.protos.Protocol;

public class TransactionInventoryMessage extends InventoryMessage {

  public TransactionInventoryMessage(byte[] packed) throws Exception {
    super(packed);
  }

  public TransactionInventoryMessage(Protocol.Inventory inv) {
    super(inv);
  }

  public TransactionInventoryMessage(List<Sha256Hash> hashList) {
    super(hashList, Protocol.Inventory.InventoryType.kht);
  }
}
