package io.fortest.core.db;

import io.fortest.core.capsule.BytesCapsule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import io.fortest.core.db2.common.TxCacheDB;

@Slf4j
public class TransactionCache extends khcStoreWithRevoking<BytesCapsule> {

  @Autowired
  public TransactionCache(@Value("trans-cache") String dbName) {
    super(dbName, TxCacheDB.class);
  }
}
