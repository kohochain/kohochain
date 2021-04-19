package io.fortest.core.db;

import java.util.Arrays;

import io.fortest.common.utils.ByteArray;
import io.fortest.common.utils.Sha256Hash;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.BytesCapsule;
import io.fortest.core.exception.ItemNotFoundException;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class BlockIndexStore extends khcStoreWithRevoking<BytesCapsule> {


  @Autowired
  public BlockIndexStore(@Value("block-index") String dbName) {
    super(dbName);

  }

  public void put(BlockCapsule.BlockId id) {
    put(ByteArray.fromLong(id.getNum()), new BytesCapsule(id.getBytes()));
  }

  public BlockCapsule.BlockId get(Long num)
      throws ItemNotFoundException {
    BytesCapsule value = getUnchecked(ByteArray.fromLong(num));
    if (value == null || value.getData() == null) {
      throw new ItemNotFoundException("number: " + num + " is not found!");
    }
    return new BlockCapsule.BlockId(Sha256Hash.wrap(value.getData()), num);
  }

  @Override
  public BytesCapsule get(byte[] key)
      throws ItemNotFoundException {
    byte[] value = revokingDB.getUnchecked(key);
    if (ArrayUtils.isEmpty(value)) {
      throw new ItemNotFoundException("number: " + Arrays.toString(key) + " is not found!");
    }
    return new BytesCapsule(value);
  }
}