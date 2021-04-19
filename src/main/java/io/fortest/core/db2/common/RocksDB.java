package io.fortest.core.db2.common;

import com.google.common.collect.Maps;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import io.fortest.common.storage.WriteOptionsWrapper;
import io.fortest.common.storage.leveldb.RocksDbDataSourceImpl;
import io.fortest.core.config.args.Args;
import io.fortest.core.db.common.WrappedByteArray;
import io.fortest.core.db.common.iterator.DBIterator;
import lombok.Getter;

public class RocksDB implements DB<byte[], byte[]>, Flusher {

  @Getter
  private RocksDbDataSourceImpl db;
  private WriteOptionsWrapper optionsWrapper = WriteOptionsWrapper.getInstance()
      .sync(Args.getInstance().getStorage().isDbSync());

  public RocksDB(String parentName, String name) {
    db = new RocksDbDataSourceImpl(
        Paths.get(parentName, Args.getInstance().getStorage().getDbDirectory()).toString(), name);
    db.initDB();
  }

  @Override
  public byte[] get(byte[] key) {
    return db.getData(key);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    db.putData(key, value);
  }

  @Override
  public long size() {
    return db.getTotal();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public void remove(byte[] key) {
    db.deleteData(key);
  }

  @Override
  public DBIterator iterator() {
    return db.iterator();
  }

  @Override
  public void flush(Map<WrappedByteArray, WrappedByteArray> batch) {
    Map<byte[], byte[]> rows = batch.entrySet().stream()
        .map(e -> Maps.immutableEntry(e.getKey().getBytes(), e.getValue().getBytes()))
        .collect(HashMap::new, (m, k) -> m.put(k.getKey(), k.getValue()), HashMap::putAll);
    db.updateByBatch(rows, optionsWrapper);
  }

  @Override
  public void close() {
    db.closeDB();
  }

  @Override
  public void reset() {
    db.resetDb();
  }
}