package io.fortest.common.logsfilter.capsule;

import io.fortest.common.logsfilter.EventPluginLoader;
import io.fortest.common.logsfilter.trigger.BlockLogTrigger;
import io.fortest.core.capsule.BlockCapsule;
import lombok.Getter;
import lombok.Setter;

public class BlockLogTriggerCapsule extends TriggerCapsule {

  @Getter
  @Setter
  BlockLogTrigger blockLogTrigger;

  public BlockLogTriggerCapsule(BlockCapsule block) {
    blockLogTrigger = new BlockLogTrigger();
    blockLogTrigger.setBlockHash(block.getBlockId().toString());
    blockLogTrigger.setTimeStamp(block.getTimeStamp());
    blockLogTrigger.setBlockNumber(block.getNum());
    blockLogTrigger.setTransactionSize(block.getTransactions().size());
    block.getTransactions().forEach(kht ->
        blockLogTrigger.getTransactionList().add(kht.getTransactionId().toString())
    );
  }

  public void setLatestSolidifiedBlockNumber(long latestSolidifiedBlockNumber) {
    blockLogTrigger.setLatestSolidifiedBlockNumber(latestSolidifiedBlockNumber);
  }

  @Override
  public void processTrigger() {
    EventPluginLoader.getInstance().postBlockTrigger(blockLogTrigger);
  }
}
