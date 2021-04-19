package io.fortest.core.actuator;

import com.google.common.collect.Lists;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.fortest.common.utils.StringUtil;
import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.TransactionResultCapsule;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import lombok.extern.slf4j.Slf4j;
import io.fortest.core.Wallet;
import io.fortest.core.db.Manager;
import io.fortest.protos.Contract;
import io.fortest.protos.Contract.UnfreezeBalanceContract;
import io.fortest.protos.Protocol;
import io.fortest.protos.Protocol.Transaction.Result.code;

import java.util.Iterator;
import java.util.List;

@Slf4j(topic = "actuator")
public class UnlockedBalanceActuator extends AbstractActuator {

  UnlockedBalanceActuator(Any contract, Manager dbManager) {
    super(contract, dbManager);
  }

  @Override
  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
    long fee = calcFee();
    final Contract.UnlockedBalanceContract unlockedBalanceContract;
    try {
      unlockedBalanceContract = contract.unpack(Contract.UnlockedBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    byte[] ownerAddress = unlockedBalanceContract.getOwnerAddress().toByteArray();

    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    long oldBalance = accountCapsule.getBalance();

    long unlockedBalance = 0L;

    long now = dbManager.getHeadBlockTimeStamp();


    List<Protocol.Account.Locked> lockedList = Lists.newArrayList();
    lockedList.addAll(accountCapsule.getLockedList());
    Iterator<Protocol.Account.Locked> iterator = lockedList.iterator();
    while (iterator.hasNext()) {
      Protocol.Account.Locked next = iterator.next();
      if (next.getExpireTime() <= now) {
        unlockedBalance += next.getLockedBalance();
        iterator.remove();
      }
    }

    accountCapsule.setInstance(accountCapsule.getInstance().toBuilder()
            .setBalance(oldBalance + unlockedBalance)
            .clearLocked().addAllLocked(lockedList).build());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    return true;
  }

  @Override
  public boolean validate() throws ContractValidateException {
    if (this.contract == null) {
      throw new ContractValidateException("No contract!");
    }
    if (this.dbManager == null) {
      throw new ContractValidateException("No dbManager!");
    }
    if (!this.contract.is(Contract.UnlockedBalanceContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [UnlockedBalanceContract],real type[" + contract
              .getClass() + "]");
    }
    final Contract.UnlockedBalanceContract unlockedBalanceContract;
    try {
      unlockedBalanceContract = this.contract.unpack(Contract.UnlockedBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    byte[] ownerAddress = unlockedBalanceContract.getOwnerAddress().toByteArray();
    if (!Wallet.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }


    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    if (accountCapsule == null) {
      String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
      throw new ContractValidateException(
          "Account[" + readableOwnerAddress + "] not exists");
    }



    long now = dbManager.getHeadBlockTimeStamp();
    //If the receiver is not included in the contract, unfreeze frozen balance for this account.
    //otherwise,unfreeze delegated frozen balance provided this account.
    if (accountCapsule.getLockedBalance() <= 0) {
      throw new ContractValidateException("no lockedBalance");
    }

    long allowedUnfreezeCount = accountCapsule.getLockedList().stream()
            .filter(locked -> locked.getExpireTime() <= now).count();
    if (allowedUnfreezeCount <= 0) {
      throw new ContractValidateException("It's not time to unlock.");
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return contract.unpack(UnfreezeBalanceContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

}
