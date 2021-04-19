package io.fortest.core.db.api;

import javax.annotation.Resource;

import io.fortest.protos.Contract;
import io.fortest.protos.Protocol;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.AssetIssueCapsule;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.core.capsule.WitnessCapsule;
import io.fortest.core.db.api.index.Index;

@Slf4j(topic = "DB")
public class IndexHelper {

  @Getter
  @Resource
  private Index.Iface<Protocol.Transaction> transactionIndex;
  @Getter
  @Resource
  private Index.Iface<Protocol.Block> blockIndex;
  @Getter
  @Resource
  private Index.Iface<Protocol.Witness> witnessIndex;
  @Getter
  @Resource
  private Index.Iface<Protocol.Account> accountIndex;
  @Getter
  @Resource
  private Index.Iface<Contract.AssetIssueContract> assetIssueIndex;

  //@PostConstruct
  public void init() {
    transactionIndex.fill();
    //blockIndex.fill();
    //witnessIndex.fill();
    //accountIndex.fill();
    //assetIssueIndex.fill();
  }

  private <T> void add(Index.Iface<T> index, byte[] bytes) {
    index.add(bytes);
  }

  public void add(Protocol.Transaction t) {
    add(transactionIndex, getKey(t));
  }

  public void add(Protocol.Block b) {
    //add(blockIndex, getKey(b));
  }

  public void add(Protocol.Witness w) {
    //add(witnessIndex, getKey(w));
  }

  public void add(Protocol.Account a) {
    //add(accountIndex, getKey(a));
  }

  public void add(Contract.AssetIssueContract a) {
    //add(assetIssueIndex, getKey(a));
  }

  private <T> void update(Index.Iface<T> index, byte[] bytes) {
    index.update(bytes);
  }

  public void update(Protocol.Transaction t) {
    update(transactionIndex, getKey(t));
  }

  public void update(Protocol.Block b) {
    // update(blockIndex, getKey(b));
  }

  public void update(Protocol.Witness w) {
    //update(witnessIndex, getKey(w));
  }

  public void update(Protocol.Account a) {
    //update(accountIndex, getKey(a));
  }

  public void update(Contract.AssetIssueContract a) {
    //update(assetIssueIndex, getKey(a));
  }

  private <T> void remove(Index.Iface<T> index, byte[] bytes) {
    index.remove(bytes);
  }

  public void remove(Protocol.Transaction t) {
    remove(transactionIndex, getKey(t));
  }

  public void remove(Protocol.Block b) {
    //remove(blockIndex, getKey(b));
  }

  public void remove(Protocol.Witness w) {
    //remove(witnessIndex, getKey(w));
  }

  public void remove(Protocol.Account a) {
    //remove(accountIndex, getKey(a));
  }

  public void remove(Contract.AssetIssueContract a) {
    //remove(assetIssueIndex, getKey(a));
  }

  private byte[] getKey(Protocol.Transaction t) {
    return new TransactionCapsule(t).getTransactionId().getBytes();
  }

  private byte[] getKey(Protocol.Block b) {
    return new BlockCapsule(b).getBlockId().getBytes();
  }

  private byte[] getKey(Protocol.Witness w) {
    return new WitnessCapsule(w).createDbKey();
  }

  private byte[] getKey(Protocol.Account a) {
    return new AccountCapsule(a).createDbKey();
  }

  private byte[] getKey(Contract.AssetIssueContract a) {
    return new AssetIssueCapsule(a).createDbKey();
  }
}
