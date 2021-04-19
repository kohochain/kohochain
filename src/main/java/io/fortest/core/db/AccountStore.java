package io.fortest.core.db;

import com.typesafe.config.ConfigObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.fortest.core.Wallet;
import io.fortest.core.capsule.AccountCapsule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import io.fortest.core.db.accountstate.callback.AccountStateCallBack;
import io.fortest.core.db.accountstate.storetrie.AccountStateStoreTrie;

@Slf4j(topic = "DB")
@Component
public class AccountStore extends khcStoreWithRevoking<AccountCapsule> {

  private static Map<String, byte[]> assertsAddress = new HashMap<>(); // key = name , value = address

  @Autowired
  private AccountStateCallBack accountStateCallBack;

  @Autowired
  private AccountStateStoreTrie accountStateStoreTrie;

  @Autowired
  private AccountStore(@Value("account") String dbName) {
    super(dbName);
  }

  @Override
  public AccountCapsule get(byte[] key) {
    byte[] value = revokingDB.getUnchecked(key);
    return ArrayUtils.isEmpty(value) ? null : new AccountCapsule(value);
  }


  @Override
  public void put(byte[] key, AccountCapsule item) {
    super.put(key, item);
    accountStateCallBack.accountCallBack(key, item);
  }

  /**
   * Max Ma account.
   */
  public AccountCapsule getTzl() {
    return getUnchecked(assertsAddress.get("Tzl"));
  }

  /**
   * Min Ma account.
   */
  public AccountCapsule getBlackhole() {
    return getUnchecked(assertsAddress.get("Blackhole"));
  }

  /**
   * Get foundation account info.
   */
  public AccountCapsule getForTest() {
    return getUnchecked(assertsAddress.get("ForTest"));
  }

  public static void setAccount(com.typesafe.config.Config config) {
    List list = config.getObjectList("genesis.block.assets");
    for (int i = 0; i < list.size(); i++) {
      ConfigObject obj = (ConfigObject) list.get(i);
      String accountName = obj.get("accountName").unwrapped().toString();
      byte[] address = Wallet.decodeFromBase58Check(obj.get("address").unwrapped().toString());
      assertsAddress.put(accountName, address);
    }
  }

  @Override
  public void close() {
    super.close();
    accountStateStoreTrie.close();
  }
}
