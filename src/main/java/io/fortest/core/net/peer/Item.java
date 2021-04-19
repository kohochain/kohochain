package io.fortest.core.net.peer;

import io.fortest.common.utils.Sha256Hash;
import io.fortest.protos.Protocol;
import lombok.Getter;

@Getter
public class Item {

  @Getter
  private Sha256Hash hash;
  @Getter
  private Protocol.Inventory.InventoryType type;
  @Getter
  private long time;


  public Item(Sha256Hash hash, Protocol.Inventory.InventoryType type) {
    this.hash = hash;
    this.type = type;
    this.time = System.currentTimeMillis();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    Item item = (Item) o;
    return hash.equals(item.getHash()) &&
        type.equals(item.getType());
  }

  @Override
  public int hashCode() {
    return hash.hashCode();
  }

  @Override
  public String toString() {
    return type + ":" + hash;
  }
}
