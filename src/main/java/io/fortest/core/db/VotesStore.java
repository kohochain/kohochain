package io.fortest.core.db;

import io.fortest.core.capsule.VotesCapsule;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class VotesStore extends khcStoreWithRevoking<VotesCapsule> {

  @Autowired
  public VotesStore(@Value("votes") String dbName) {
    super(dbName);
  }

  @Override
  public VotesCapsule get(byte[] key) {
    byte[] value = revokingDB.getUnchecked(key);
    return ArrayUtils.isEmpty(value) ? null : new VotesCapsule(value);
  }
}