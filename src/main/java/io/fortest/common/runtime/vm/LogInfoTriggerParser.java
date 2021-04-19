package io.fortest.common.runtime.vm;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.fortest.core.capsule.ContractCapsule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.spongycastle.util.encoders.Hex;
import io.fortest.common.logsfilter.trigger.ContractTrigger;
import io.fortest.common.runtime.utils.MUtil;
import io.fortest.common.storage.Deposit;
import io.fortest.core.Wallet;
import io.fortest.protos.Protocol.SmartContract.ABI;

@Slf4j
public class LogInfoTriggerParser {

  private Long blockNum;
  private Long blockTimestamp;
  private String txId;
  private String originAddress;

  public LogInfoTriggerParser(Long blockNum,
      Long blockTimestamp,
      byte[] txId, byte[] originAddress) {

    this.blockNum = blockNum;
    this.blockTimestamp = blockTimestamp;
    this.txId = ArrayUtils.isEmpty(txId) ? "" : Hex.toHexString(txId);
    this.originAddress =
        ArrayUtils.isEmpty(originAddress) ? "" : Wallet.encode58Check(originAddress);

  }

  public List<ContractTrigger> parseLogInfos(List<LogInfo> logInfos, Deposit deposit) {

    List<ContractTrigger> list = new LinkedList<>();
    if (logInfos == null || logInfos.size() <= 0) {
      return list;
    }

    Map<String, String> addrMap = new HashMap<>();
    Map<String, ABI> abiMap = new HashMap<>();

    for (LogInfo logInfo : logInfos) {

      byte[] contractAddress = MUtil.convertTokhcAddress(logInfo.getAddress());
      String strContractAddr =
          ArrayUtils.isEmpty(contractAddress) ? "" : Wallet.encode58Check(contractAddress);
      if (addrMap.get(strContractAddr) != null) {
        continue;
      }
      ContractCapsule contract = deposit.getContract(contractAddress);
      if (contract == null) {
        // never
        addrMap.put(strContractAddr, originAddress);
        abiMap.put(strContractAddr, ABI.getDefaultInstance());
        continue;
      }
      ABI abi = contract.getInstance().getAbi();
      String creatorAddr = Wallet.encode58Check(
          MUtil.convertTokhcAddress(contract.getInstance().getOriginAddress().toByteArray()));
      addrMap.put(strContractAddr, creatorAddr);
      abiMap.put(strContractAddr, abi);
    }

    int index = 1;
    for (LogInfo logInfo : logInfos) {

      byte[] contractAddress = MUtil.convertTokhcAddress(logInfo.getAddress());
      String strContractAddr =
          ArrayUtils.isEmpty(contractAddress) ? "" : Wallet.encode58Check(contractAddress);
      ABI abi = abiMap.get(strContractAddr);
      ContractTrigger event = new ContractTrigger();
      String creatorAddr = addrMap.get(strContractAddr);
      event.setUniqueId(txId + "_" + index);
      event.setTransactionId(txId);
      event.setContractAddress(strContractAddr);
      event.setOriginAddress(originAddress);
      event.setCallerAddress("");
      event.setCreatorAddress(StringUtils.isEmpty(creatorAddr) ? "" : creatorAddr);
      event.setBlockNumber(blockNum);
      event.setTimeStamp(blockTimestamp);
      event.setLogInfo(logInfo);
      event.setAbi(abi);

      list.add(event);
      index++;
    }

    return list;
  }

  public static String getEntrySignature(ABI.Entry entry) {
    String signature = entry.getName() + "(";
    StringBuilder builder = new StringBuilder();
    for (ABI.Entry.Param param : entry.getInputsList()) {
      if (builder.length() > 0) {
        builder.append(",");
      }
      builder.append(param.getType());
    }
    signature += builder.toString() + ")";
    return signature;
  }
}
