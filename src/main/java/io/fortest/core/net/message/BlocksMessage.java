package io.fortest.core.net.message;

import java.util.List;

import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.protos.Protocol;
import org.apache.commons.collections4.CollectionUtils;

public class BlocksMessage extends khcMessage {

  private List<Protocol.Block> blocks;

  public BlocksMessage(byte[] data) throws Exception {
    super(data);
    this.type = MessageTypes.BLOCKS.asByte();
    Protocol.Items items = Protocol.Items.parseFrom(getCodedInputStream(data));
    if (items.getType() == Protocol.Items.ItemType.BLOCK) {
      blocks = items.getBlocksList();
    }
    if (isFilter() && CollectionUtils.isNotEmpty(blocks)) {
      compareBytes(data, items.toByteArray());
      for (Protocol.Block block : blocks) {
        TransactionCapsule.validContractProto(block.getTransactionsList());
      }
    }
  }

  public List<Protocol.Block> getBlocks() {
    return blocks;
  }

  @Override
  public String toString() {
    return super.toString() + "size: " + (CollectionUtils.isNotEmpty(blocks) ? blocks
        .size() : 0);
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }

}
