package io.fortest.common.runtime.vm.program;

import static java.lang.System.arraycopy;

import java.util.HashMap;
import java.util.Map;

import io.fortest.common.crypto.Hash;
import io.fortest.common.runtime.vm.DataWord;
import io.fortest.common.utils.ByteUtil;
import io.fortest.core.capsule.StorageRowCapsule;
import lombok.Getter;
import io.fortest.core.db.StorageRowStore;

public class Storage {

  @Getter
  private byte[] addrHash;
  @Getter
  private StorageRowStore store;
  @Getter
  private final Map<DataWord, StorageRowCapsule> rowCache = new HashMap<>();

  @Getter
  private byte[] address;

  private static final int PREFIX_BYTES = 16;

  public Storage(byte[] address, StorageRowStore store) {
    addrHash = addrHash(address);
    this.address = address;
    this.store = store;
  }

  public void generateAddrHash(byte[] khtId) {
    // update addreHash for create2
    addrHash = addrHash(address, khtId);
  }

  public Storage(Storage storage) {
    this.addrHash = storage.addrHash.clone();
    this.address = storage.getAddress().clone();
    this.store = storage.store;
    storage.getRowCache().forEach((DataWord rowKey, StorageRowCapsule row) -> {
      StorageRowCapsule newRow = new StorageRowCapsule(row);
      this.rowCache.put(rowKey.clone(), newRow);
    });
  }

  public DataWord getValue(DataWord key) {
    if (rowCache.containsKey(key)) {
      return rowCache.get(key).getValue();
    } else {
      StorageRowCapsule row = store.get(compose(key.getData(), addrHash));
      if (row == null || row.getInstance() == null) {
        return null;
      }
      rowCache.put(key, row);
      return row.getValue();
    }
  }

  public void put(DataWord key, DataWord value) {
    if (rowCache.containsKey(key)) {
      rowCache.get(key).setValue(value);
    } else {
      byte[] rowKey = compose(key.getData(), addrHash);
      StorageRowCapsule row = new StorageRowCapsule(rowKey, value.getData());
      rowCache.put(key, row);
    }
  }

  private static byte[] compose(byte[] key, byte[] addrHash) {
    byte[] result = new byte[key.length];
    arraycopy(addrHash, 0, result, 0, PREFIX_BYTES);
    arraycopy(key, PREFIX_BYTES, result, PREFIX_BYTES, PREFIX_BYTES);
    return result;
  }

  // 32 bytes
  private static byte[] addrHash(byte[] address) {
    return Hash.sha3(address);
  }

  private static byte[] addrHash(byte[] address, byte[] khtHash) {
    if (ByteUtil.isNullOrZeroArray(khtHash)) {
      return Hash.sha3(address);
    }
    return Hash.sha3(ByteUtil.merge(address, khtHash));
  }

  public void commit() {
    rowCache.forEach((DataWord rowKey, StorageRowCapsule row) -> {
      if (row.isDirty()) {
        if (row.getValue().isZero()) {
          this.store.delete(row.getRowKey());
        } else {
          this.store.put(row.getRowKey(), row);
        }
      }
    });
  }
}
