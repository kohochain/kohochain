package io.fortest.core.net.message;

import java.util.List;

import io.fortest.common.utils.Sha256Hash;
import io.fortest.protos.Protocol;

public class FetchInvDataMessage extends InventoryMessage {


  public FetchInvDataMessage(byte[] packed) throws Exception {
    super(packed);
    this.type = MessageTypes.FETCH_INV_DATA.asByte();
  }

  public FetchInvDataMessage(Protocol.Inventory inv) {
    super(inv);
    this.type = MessageTypes.FETCH_INV_DATA.asByte();
  }

  public FetchInvDataMessage(List<Sha256Hash> hashList, Protocol.Inventory.InventoryType type) {
    super(hashList, type);
    this.type = MessageTypes.FETCH_INV_DATA.asByte();
  }

}
