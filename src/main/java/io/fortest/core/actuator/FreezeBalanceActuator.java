package io.fortest.core.actuator;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.DelegatedResourceAccountIndexCapsule;
import io.fortest.core.capsule.DelegatedResourceCapsule;
import io.fortest.core.capsule.TransactionResultCapsule;
import io.fortest.core.config.args.Args;
import io.fortest.protos.Contract;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import io.fortest.common.utils.StringUtil;
import io.fortest.core.Wallet;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.protos.Contract.FreezeBalanceContract;
import io.fortest.protos.Protocol;
import io.fortest.protos.Protocol.AccountType;
import io.fortest.protos.Protocol.Transaction.Result.code;

import java.util.Arrays;
import java.util.List;

@Slf4j(topic = "actuator")
public class FreezeBalanceActuator extends AbstractActuator {

  FreezeBalanceActuator(Any contract, Manager dbManager) {
    super(contract, dbManager);
  }

  @Override
  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
    long fee = calcFee();
    final FreezeBalanceContract freezeBalanceContract;
    try {
      freezeBalanceContract = contract.unpack(FreezeBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
//      ret.setStatus(fee, code.FAILED);
      ret.setStatus(fee, Protocol.Transaction.Result.code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    AccountCapsule accountCapsule = dbManager.getAccountStore()
        .get(freezeBalanceContract.getOwnerAddress().toByteArray());

    long now = dbManager.getHeadBlockTimeStamp();
    long duration = freezeBalanceContract.getFrozenDuration() * 86_400_000;

    long newBalance = accountCapsule.getBalance() - freezeBalanceContract.getFrozenBalance();

    long frozenBalance = freezeBalanceContract.getFrozenBalance();
    long expireTime = now + duration;
    byte[] ownerAddress = freezeBalanceContract.getOwnerAddress().toByteArray();
//    byte[] receiverAddress = freezeBalanceContract.getReceiverAddress().toByteArray();
//
//    if (accountCapsule != null && accountCapsule.getAccountStatus().equals(Protocol.AccountStatus.Account_Locked)){
//      throw new ContractExeException("account is locked");
//    }

    byte[] receiverAddress = null;
    switch (freezeBalanceContract.getResource()) {
      case BANDWIDTH:
        if (!ArrayUtils.isEmpty(receiverAddress)
            && dbManager.getDynamicPropertiesStore().supportDR()) {
          delegateResource(ownerAddress, receiverAddress, true,
              frozenBalance, expireTime);
          accountCapsule.addDelegatedFrozenBalanceForBandwidth(frozenBalance);
        } else {
          long newFrozenBalanceForBandwidth =
              frozenBalance + accountCapsule.getFrozenBalance();
          accountCapsule.setFrozenForBandwidth(newFrozenBalanceForBandwidth, expireTime);
        }
        dbManager.getDynamicPropertiesStore()
            .addTotalNetWeight(frozenBalance / 1000_000_00L);
        break;
      case ENERGY:
        if (!ArrayUtils.isEmpty(receiverAddress)
            && dbManager.getDynamicPropertiesStore().supportDR()) {
          delegateResource(ownerAddress, receiverAddress, false,
              frozenBalance, expireTime);
          accountCapsule.addDelegatedFrozenBalanceForEnergy(frozenBalance);
        } else {
          long newFrozenBalanceForEnergy =
              frozenBalance + accountCapsule.getAccountResource()
                  .getFrozenBalanceForEnergy()
                  .getFrozenBalance();
          accountCapsule.setFrozenForEnergy(newFrozenBalanceForEnergy, expireTime);
        }
        dbManager.getDynamicPropertiesStore()
            .addTotalEnergyWeight(frozenBalance / 1000_000_00L);
        break;
    }

    accountCapsule.setBalance(newBalance);
    dbManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);

//    ret.setStatus(fee, code.SUCESS);
    ret.setStatus(fee, Protocol.Transaction.Result.code.SUCESS);

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
    if (!contract.is(FreezeBalanceContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [FreezeBalanceContract],real type[" + contract
              .getClass() + "]");
    }

    final FreezeBalanceContract freezeBalanceContract;
    try {
      freezeBalanceContract = this.contract.unpack(FreezeBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    byte[] ownerAddress = freezeBalanceContract.getOwnerAddress().toByteArray();
    if (!Wallet.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }

    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    if (accountCapsule == null) {
      String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
      throw new ContractValidateException(
          "Account[" + readableOwnerAddress + "] not exists");
    }

//    Protocol.AccountStatus accountStatus = accountCapsule.getAccountStatus();
//    if (accountStatus != null && accountStatus.equals(Protocol.AccountStatus.Account_Locked)){
//      throw new ContractValidateException("Validate TransferContract error, account is locked.");
//    }


    long frozenBalance = freezeBalanceContract.getFrozenBalance();
    if (frozenBalance <= 0) {
      throw new ContractValidateException("frozenBalance must be positive");
    }
    if (frozenBalance < 1L) {
      throw new ContractValidateException("frozenBalance must be more than 1DATA");
    }

    int frozenCount = accountCapsule.getFrozenCount();
    if (!(frozenCount == 0 || frozenCount == 1)) {
      throw new ContractValidateException("frozenCount must be 0 or 1");
    }
    if (frozenBalance > accountCapsule.getBalance()) {
      throw new ContractValidateException("frozenBalance must be less than accountBalance");
    }
    long frozenDuration = freezeBalanceContract.getFrozenDuration();
    long minFrozenTime = dbManager.getDynamicPropertiesStore().getMinFrozenTime();
    long maxFrozenTime = dbManager.getDynamicPropertiesStore().getMaxFrozenTime();

    boolean needCheckFrozeTime = Args.getInstance().getCheckFrozenTime() == 1;//for test
    if (needCheckFrozeTime && !(frozenDuration >= minFrozenTime
        && frozenDuration <= maxFrozenTime)) {
      throw new ContractValidateException(
          "frozenDuration must be less than " + maxFrozenTime + " days "
              + "and more than " + minFrozenTime + " days");
    }

    switch (freezeBalanceContract.getResource()) {
      case BANDWIDTH:
        break;
      case ENERGY:
        break;
      default:
        throw new ContractValidateException(
            "ResourceCode error,valid ResourceCode[BANDWIDTH、ENERGY]");
    }

    //todo：need version control and config for delegating resource
    byte[] receiverAddress = null;
    //If the receiver is included in the contract, the receiver will receive the resource.
    if (!ArrayUtils.isEmpty(receiverAddress) && dbManager.getDynamicPropertiesStore().supportDR()) {
      if (Arrays.equals(receiverAddress, ownerAddress)) {
        throw new ContractValidateException(
            "receiverAddress must not be the same as ownerAddress");
      }

      if (!Wallet.addressValid(receiverAddress)) {
        throw new ContractValidateException("Invalid receiverAddress");
      }

      AccountCapsule receiverCapsule = dbManager.getAccountStore().get(receiverAddress);
      if (receiverCapsule == null) {
        String readableOwnerAddress = StringUtil.createReadableString(receiverAddress);
        throw new ContractValidateException(
            "Account[" + readableOwnerAddress + "] not exists");
      }

      if (dbManager.getDynamicPropertiesStore().getAllowTvmConstantinople() == 1
          && receiverCapsule.getType() == AccountType.Contract) {
        throw new ContractValidateException(
            "Do not allow delegate resources to contract addresses");

      }

    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return contract.unpack(FreezeBalanceContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

  private void delegateResource(byte[] ownerAddress, byte[] receiverAddress, boolean isBandwidth,
      long balance, long expireTime) {
    byte[] key = DelegatedResourceCapsule.createDbKey(ownerAddress, receiverAddress);
    //modify DelegatedResourceStore
    DelegatedResourceCapsule delegatedResourceCapsule = dbManager.getDelegatedResourceStore()
        .get(key);
    if (delegatedResourceCapsule != null) {
      if (isBandwidth) {
        delegatedResourceCapsule.addFrozenBalanceForBandwidth(balance, expireTime);
      } else {
        delegatedResourceCapsule.addFrozenBalanceForEnergy(balance, expireTime);
      }
    } else {
      delegatedResourceCapsule = new DelegatedResourceCapsule(
          ByteString.copyFrom(ownerAddress),
          ByteString.copyFrom(receiverAddress));
      if (isBandwidth) {
        delegatedResourceCapsule.setFrozenBalanceForBandwidth(balance, expireTime);
      } else {
        delegatedResourceCapsule.setFrozenBalanceForEnergy(balance, expireTime);
      }

    }
    dbManager.getDelegatedResourceStore().put(key, delegatedResourceCapsule);

    //modify DelegatedResourceAccountIndexStore
    {
      DelegatedResourceAccountIndexCapsule delegatedResourceAccountIndexCapsule = dbManager
          .getDelegatedResourceAccountIndexStore()
          .get(ownerAddress);
      if (delegatedResourceAccountIndexCapsule == null) {
        delegatedResourceAccountIndexCapsule = new DelegatedResourceAccountIndexCapsule(
            ByteString.copyFrom(ownerAddress));
      }
      List<ByteString> toAccountsList = delegatedResourceAccountIndexCapsule.getToAccountsList();
      if (!toAccountsList.contains(ByteString.copyFrom(receiverAddress))) {
        delegatedResourceAccountIndexCapsule.addToAccount(ByteString.copyFrom(receiverAddress));
      }
      dbManager.getDelegatedResourceAccountIndexStore()
          .put(ownerAddress, delegatedResourceAccountIndexCapsule);
    }

    {
      DelegatedResourceAccountIndexCapsule delegatedResourceAccountIndexCapsule = dbManager
          .getDelegatedResourceAccountIndexStore()
          .get(receiverAddress);
      if (delegatedResourceAccountIndexCapsule == null) {
        delegatedResourceAccountIndexCapsule = new DelegatedResourceAccountIndexCapsule(
            ByteString.copyFrom(receiverAddress));
      }
      List<ByteString> fromAccountsList = delegatedResourceAccountIndexCapsule
          .getFromAccountsList();
      if (!fromAccountsList.contains(ByteString.copyFrom(ownerAddress))) {
        delegatedResourceAccountIndexCapsule.addFromAccount(ByteString.copyFrom(ownerAddress));
      }
      dbManager.getDelegatedResourceAccountIndexStore()
          .put(receiverAddress, delegatedResourceAccountIndexCapsule);
    }

    //modify AccountStore
    AccountCapsule receiverCapsule = dbManager.getAccountStore().get(receiverAddress);
    if (isBandwidth) {
      receiverCapsule.addAcquiredDelegatedFrozenBalanceForBandwidth(balance);
    } else {
      receiverCapsule.addAcquiredDelegatedFrozenBalanceForEnergy(balance);
    }

    dbManager.getAccountStore().put(receiverCapsule.createDbKey(), receiverCapsule);
  }

}
