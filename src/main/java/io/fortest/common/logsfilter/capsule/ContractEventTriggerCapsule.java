package io.fortest.common.logsfilter.capsule;

import java.util.List;

import io.fortest.common.logsfilter.ContractEventParserAbi;
import io.fortest.common.logsfilter.EventPluginLoader;
import io.fortest.common.logsfilter.FilterQuery;
import io.fortest.common.logsfilter.trigger.ContractEventTrigger;
import io.fortest.common.runtime.vm.LogEventWrapper;
import lombok.Getter;
import lombok.Setter;
import io.fortest.protos.Protocol.SmartContract.ABI.Entry;

public class ContractEventTriggerCapsule extends TriggerCapsule {

  @Getter
  @Setter
  private List<byte[]> topicList;

  @Getter
  @Setter
  private byte[] data;

  @Getter
  @Setter
  ContractEventTrigger contractEventTrigger;

  @Getter
  @Setter
  private Entry abiEntry;

  public void setLatestSolidifiedBlockNumber(long latestSolidifiedBlockNumber) {
    contractEventTrigger.setLatestSolidifiedBlockNumber(latestSolidifiedBlockNumber);
  }

  public ContractEventTriggerCapsule(LogEventWrapper log) {
    this.contractEventTrigger = new ContractEventTrigger();

    this.contractEventTrigger.setUniqueId(log.getUniqueId());
    this.contractEventTrigger.setTransactionId(log.getTransactionId());
    this.contractEventTrigger.setContractAddress(log.getContractAddress());
    this.contractEventTrigger.setCallerAddress(log.getCallerAddress());
    this.contractEventTrigger.setOriginAddress(log.getOriginAddress());
    this.contractEventTrigger.setCreatorAddress(log.getCreatorAddress());
    this.contractEventTrigger.setBlockNumber(log.getBlockNumber());
    this.contractEventTrigger.setTimeStamp(log.getTimeStamp());

    this.topicList = log.getTopicList();
    this.data = log.getData();
    this.contractEventTrigger.setEventSignature(log.getEventSignature());
    this.contractEventTrigger.setEventSignatureFull(log.getEventSignatureFull());
    this.contractEventTrigger.setEventName(log.getAbiEntry().getName());
    this.abiEntry = log.getAbiEntry();
  }

  @Override
  public void processTrigger() {
    contractEventTrigger.setTopicMap(ContractEventParserAbi.parseTopics(topicList, abiEntry));
    contractEventTrigger
        .setDataMap(ContractEventParserAbi.parseEventData(data, topicList, abiEntry));

    if (FilterQuery.matchFilter(contractEventTrigger)) {
      EventPluginLoader.getInstance().postContractEventTrigger(contractEventTrigger);
    }
  }
}
