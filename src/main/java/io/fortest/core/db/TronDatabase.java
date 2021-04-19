package io.fortest.core.db;

import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map.Entry;

import io.fortest.common.storage.DbSourceInter;
import io.fortest.common.storage.leveldb.LevelDbDataSourceImpl;
import io.fortest.common.storage.leveldb.RocksDbDataSourceImpl;
import io.fortest.core.config.args.Args;
import io.fortest.core.exception.BadItemException;
import io.fortest.core.exception.ItemNotFoundException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import io.fortest.core.db.api.IndexHelper;
import io.fortest.core.db2.core.IkhcChainBase;

@Slf4j(topic = "DB")
public abstract class khcDatabase<T> implements IkhcChainBase<T> {

  protected DbSourceInter<byte[]> dbSource;
  @Getter
  private String dbName;

  @Autowired(required = false)
  protected IndexHelper indexHelper;

  protected khcDatabase(String dbName) {
    this.dbName = dbName;

    if ("LEVELDB".equals(Args.getInstance().getStorage().getDbEngine().toUpperCase())) {
      dbSource =
          new LevelDbDataSourceImpl(Args.getInstance().getOutputDirectoryByDbName(dbName), dbName);
    } else if ("ROCKSDB".equals(Args.getInstance().getStorage().getDbEngine().toUpperCase())) {
      String parentName = Paths.get(Args.getInstance().getOutputDirectoryByDbName(dbName),
          Args.getInstance().getStorage().getDbDirectory()).toString();
      dbSource =
          new RocksDbDataSourceImpl(parentName, dbName);
    }

    dbSource.initDB();
  }

  protected khcDatabase() {
  }

  public DbSourceInter<byte[]> getDbSource() {
    return dbSource;
  }

  /**
   * reset the database.
   */
  public void reset() {
    dbSource.resetDb();
  }

  /**
   * close the database.
   */
  @Override
  public void close() {
    dbSource.closeDB();
  }

  public abstract void put(byte[] key, T item);

  public abstract void delete(byte[] key);

  public abstract T get(byte[] key)
      throws InvalidProtocolBufferException, ItemNotFoundException, BadItemException;

  public T getUnchecked(byte[] key) {
    return null;
  }

  public abstract boolean has(byte[] key);

  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public Iterator<Entry<byte[], T>> iterator() {
    throw new UnsupportedOperationException();
  }
}
