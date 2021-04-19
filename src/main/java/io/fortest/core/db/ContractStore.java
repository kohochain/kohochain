package io.fortest.core.db;

import com.google.common.collect.Streams;
import io.fortest.core.capsule.ContractCapsule;
import io.fortest.protos.Protocol;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j(topic = "DB")
@Component
public class ContractStore extends khcStoreWithRevoking<ContractCapsule> {

  @Autowired
  private ContractStore(@Value("contract") String dbName) {
    super(dbName);
  }

  @Override
  public ContractCapsule get(byte[] key) {
    return getUnchecked(key);
  }

  /**
   * get total transaction.
   */
  public long getTotalContracts() {
    return Streams.stream(revokingDB.iterator()).count();
  }

  /**
   * find a transaction  by it's id.
   */
  public byte[] findContractByHash(byte[] khtHash) {
    return revokingDB.getUnchecked(khtHash);
  }

  /**
   *
   * @param contractAddress
   * @return
   */
  public Protocol.SmartContract.ABI getABI(byte[] contractAddress) {
    byte[] value = revokingDB.getUnchecked(contractAddress);
    if (ArrayUtils.isEmpty(value)) {
      return null;
    }

    ContractCapsule contractCapsule = new ContractCapsule(value);
    Protocol.SmartContract smartContract = contractCapsule.getInstance();
    if (smartContract == null) {
      return null;
    }

    return smartContract.getAbi();
  }

}
