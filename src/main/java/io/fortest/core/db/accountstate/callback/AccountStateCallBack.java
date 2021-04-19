package io.fortest.core.db.accountstate.callback;

import com.google.protobuf.ByteString;
import com.google.protobuf.Internal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.fortest.common.crypto.Hash;
import io.fortest.common.utils.ByteUtil;
import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.utils.RLP;
import io.fortest.core.db.Manager;
import io.fortest.core.db.accountstate.AccountStateEntity;
import io.fortest.core.db.accountstate.storetrie.AccountStateStoreTrie;
import io.fortest.core.exception.BadBlockException;
import io.fortest.core.trie.TrieImpl;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j(topic = "AccountState")
@Component
public class AccountStateCallBack {

  private BlockCapsule blockCapsule;
  private volatile boolean execute = false;
  private volatile boolean allowGenerateRoot = false;
  private TrieImpl trie;

  @Setter
  private Manager manager;

  @Autowired
  private AccountStateStoreTrie db;

  private List<TrieEntry> trieEntryList = new ArrayList<>();

  private static class TrieEntry {

    private byte[] key;
    private byte[] data;

    public byte[] getKey() {
      return key;
    }

    public TrieEntry setKey(byte[] key) {
      this.key = key;
      return this;
    }

    public byte[] getData() {
      return data;
    }

    public TrieEntry setData(byte[] data) {
      this.data = data;
      return this;
    }

    public static TrieEntry build(byte[] key, byte[] data) {
      TrieEntry trieEntry = new TrieEntry();
      return trieEntry.setKey(key).setData(data);
    }
  }

  public void accountCallBack(byte[] key, AccountCapsule item) {
    if (!exe()) {
      return;
    }
    if (item == null) {
      return;
    }
    trieEntryList
        .add(TrieEntry.build(key, new AccountStateEntity(item.getInstance()).toByteArrays()));
  }

  public void preExeTrans() {
    trieEntryList.clear();
  }

  public void exeTransFinish() {
    for (TrieEntry trieEntry : trieEntryList) {
      trie.put(RLP.encodeElement(trieEntry.getKey()), trieEntry.getData());
    }
    trieEntryList.clear();
  }

  public void deleteAccount(byte[] key) {
    if (!exe()) {
      return;
    }
    trie.delete(RLP.encodeElement(key));
  }

  public void preExecute(BlockCapsule blockCapsule) {
    this.blockCapsule = blockCapsule;
    this.execute = true;
    this.allowGenerateRoot = manager.getDynamicPropertiesStore().allowAccountStateRoot();
    if (!exe()) {
      return;
    }
    byte[] rootHash = null;
    try {
      BlockCapsule parentBlockCapsule = manager.getBlockById(blockCapsule.getParentBlockId());
      rootHash = parentBlockCapsule.getInstance().getBlockHeader().getRawData()
          .getAccountStateRoot().toByteArray();
    } catch (Exception e) {
      logger.error("", e);
    }
    if (Arrays.equals(Internal.EMPTY_BYTE_ARRAY, rootHash)) {
      rootHash = Hash.EMPTY_TRIE_HASH;
    }
    trie = new TrieImpl(db, rootHash);
  }

  public void executePushFinish() throws BadBlockException {
    if (!exe()) {
      return;
    }
    ByteString oldRoot = blockCapsule.getInstance().getBlockHeader().getRawData()
        .getAccountStateRoot();
    execute = false;
    //
    byte[] newRoot = trie.getRootHash();
    if (ArrayUtils.isEmpty(newRoot)) {
      newRoot = Hash.EMPTY_TRIE_HASH;
    }
    if (!oldRoot.isEmpty() && !Arrays.equals(oldRoot.toByteArray(), newRoot)) {
      logger.error("the accountStateRoot hash is error. {}, oldRoot: {}, newRoot: {}",
          blockCapsule.getBlockId().getString(), ByteUtil.toHexString(oldRoot.toByteArray()),
          ByteUtil.toHexString(newRoot));
      printErrorLog(trie);
      throw new BadBlockException("the accountStateRoot hash is error");
    }
  }

  public void executeGenerateFinish() {
    if (!exe()) {
      return;
    }
    //
    byte[] newRoot = trie.getRootHash();
    if (ArrayUtils.isEmpty(newRoot)) {
      newRoot = Hash.EMPTY_TRIE_HASH;
    }
    blockCapsule.setAccountStateRoot(newRoot);
    execute = false;
  }

  public void exceptionFinish() {
    execute = false;
  }

  private boolean exe() {
    if (!execute || !allowGenerateRoot) {
      //Agreement same block high to generate account state root
      execute = false;
      return false;
    }
    return true;
  }

  private void printErrorLog(TrieImpl trie) {
    trie.scanTree(new TrieImpl.ScanAction() {
      @Override
      public void doOnNode(byte[] hash, TrieImpl.Node node) {

      }

      @Override
      public void doOnValue(byte[] nodeHash, TrieImpl.Node node, byte[] key, byte[] value) {
        try {
          logger.info("account info : {}", AccountStateEntity.parse(value));
        } catch (Exception e) {
          logger.error("", e);
        }
      }
    });
  }

}
