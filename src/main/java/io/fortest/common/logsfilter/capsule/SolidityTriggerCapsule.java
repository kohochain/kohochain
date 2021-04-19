package io.fortest.common.logsfilter.capsule;

import io.fortest.common.logsfilter.EventPluginLoader;
import io.fortest.common.logsfilter.trigger.SolidityTrigger;
import lombok.Getter;
import lombok.Setter;


public class SolidityTriggerCapsule extends TriggerCapsule {

  @Getter
  @Setter
  private SolidityTrigger solidityTrigger;

  public SolidityTriggerCapsule(long latestSolidifiedBlockNum) {
    solidityTrigger = new SolidityTrigger();
    solidityTrigger.setLatestSolidifiedBlockNumber(latestSolidifiedBlockNum);
  }

  @Override
  public void processTrigger() {
    EventPluginLoader.getInstance().postSolidityTrigger(solidityTrigger);
  }
}