package io.fortest.core.db;

import com.google.protobuf.ByteString;
import java.util.Objects;

import io.fortest.common.utils.ByteArray;
import io.fortest.core.capsule.TransactionInfoCapsule;
import io.fortest.core.capsule.TransactionRetCapsule;
import io.fortest.core.config.args.Args;
import io.fortest.core.exception.BadItemException;
import io.fortest.protos.Protocol;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j(topic = "DB")
@Component
public class TransactionRetStore extends khcStoreWithRevoking<TransactionRetCapsule>  {

  @Autowired
  private TransactionStore transactionStore;

  @Autowired
  public TransactionRetStore(@Value("transactionRetStore") String dbName) {
    super(dbName);
  }

  @Override
  public void put(byte[] key, TransactionRetCapsule item) {
    if (BooleanUtils.toBoolean(Args.getInstance().getStorage().getTransactionHistoreSwitch())) {
      super.put(key, item);
    }
  }

  public TransactionInfoCapsule getTransactionInfo(byte[] key) throws BadItemException {
    long blockNumber = transactionStore.getBlockNumber(key);
    if (blockNumber == -1) {
      return null;
    }
    byte[] value = revokingDB.getUnchecked(ByteArray.fromLong(blockNumber));
    if (Objects.isNull(value)) {
      return null;
    }

    TransactionRetCapsule result = new TransactionRetCapsule(value);
    if (Objects.isNull(result) || Objects.isNull(result.getInstance())) {
      return null;
    }

    for (Protocol.TransactionInfo transactionResultInfo : result.getInstance().getTransactioninfoList()) {
      if (transactionResultInfo.getId().equals(ByteString.copyFrom(key))) {
        return new TransactionInfoCapsule(transactionResultInfo);
      }
    }
    return null;
  }

}
