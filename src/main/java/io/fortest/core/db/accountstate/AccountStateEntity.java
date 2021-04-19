package io.fortest.core.db.accountstate;

import io.fortest.core.Wallet;
import io.fortest.protos.Protocol;
import lombok.extern.slf4j.Slf4j;

@Slf4j(topic = "AccountState")
public class AccountStateEntity {

  private Protocol.Account account;

  public AccountStateEntity() {
  }

  public AccountStateEntity(Protocol.Account account) {
    Protocol.Account.Builder builder = Protocol.Account.newBuilder();
    builder.setAddress(account.getAddress());
    builder.setBalance(account.getBalance());
    //builder.putAllAssetV2(account.getAssetV2Map());
    builder.setAllowance(account.getAllowance());
    this.account = builder.build();
  }

  public Protocol.Account getAccount() {
    return account;
  }

  public AccountStateEntity setAccount(Protocol.Account account) {
    this.account = account;
    return this;
  }

  public byte[] toByteArrays() {
    return account.toByteArray();
  }

  public static AccountStateEntity parse(byte[] data) {
    try {
      return new AccountStateEntity().setAccount(Protocol.Account.parseFrom(data));
    } catch (Exception e) {
      logger.error("parse to AccountStateEntity error! reason: {}", e.getMessage());
    }
    return null;
  }

  @Override
  public String toString() {
    return "address:" + Wallet.encode58Check(account.getAddress().toByteArray()) + "; " + account
        .toString();
  }
}
