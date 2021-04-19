package io.fortest.core.actuator;

import com.google.common.collect.Lists;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.fortest.core.capsule.*;
import io.fortest.core.config.args.Args;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import io.fortest.common.utils.StringUtil;
import io.fortest.core.Wallet;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.protos.Contract.UnfreezeBalanceContract;
import io.fortest.protos.Protocol.Account.AccountResource;
import io.fortest.protos.Protocol.Account.Frozen;
import io.fortest.protos.Protocol.Transaction.Result.code;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Slf4j(topic = "actuator")
public class UnfreezeBalanceActuator extends AbstractActuator {

  UnfreezeBalanceActuator(Any contract, Manager dbManager) {
    super(contract, dbManager);
  }

  @Override
  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
    long fee = calcFee();
    final UnfreezeBalanceContract unfreezeBalanceContract;
    try {
      unfreezeBalanceContract = contract.unpack(UnfreezeBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    byte[] ownerAddress = unfreezeBalanceContract.getOwnerAddress().toByteArray();

    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);

    long oldBalance = accountCapsule.getBalance();

    long unfreezeBalance = 0L;

    long now = dbManager.getHeadBlockTimeStamp();
    long lockedDuration = unfreezeBalanceContract.getLockedDuration() * 86_400_000;

    long lockedExpireTime = now + lockedDuration;

    long actualUnfreezeBalance = unfreezeBalanceContract.getUnfrozenBalance();

    byte[] receiverAddress = unfreezeBalanceContract.getReceiverAddress().toByteArray();
    AccountCapsule receiverCapsule = dbManager.getAccountStore().get(receiverAddress);
    //If the receiver is not included in the contract, unfreeze frozen balance for this account.
    //otherwise,unfreeze delegated frozen balance provided this account.
    if (!ArrayUtils.isEmpty(receiverAddress) && dbManager.getDynamicPropertiesStore().supportDR()) {
      byte[] key = DelegatedResourceCapsule
          .createDbKey(unfreezeBalanceContract.getOwnerAddress().toByteArray(),
              unfreezeBalanceContract.getReceiverAddress().toByteArray());
      DelegatedResourceCapsule delegatedResourceCapsule = dbManager.getDelegatedResourceStore()
          .get(key);

//      AccountCapsule receiverCapsule = dbManager.getAccountStore().get(receiverAddress);

      switch (unfreezeBalanceContract.getResource()) {
        case BANDWIDTH:
//          unfreezeBalance = delegatedResourceCapsule.getFrozenBalanceForBandwidth();
          long frozenBalanceForBandwidth  = delegatedResourceCapsule.getFrozenBalanceForBandwidth();
          long expireTimeForBandwidth = delegatedResourceCapsule.getExpireTimeForBandwidth();
          delegatedResourceCapsule.setFrozenBalanceForBandwidth(frozenBalanceForBandwidth - actualUnfreezeBalance, expireTimeForBandwidth);
          receiverCapsule.addAcquiredDelegatedFrozenBalanceForBandwidth(-actualUnfreezeBalance);
          accountCapsule.addDelegatedFrozenBalanceForBandwidth(-actualUnfreezeBalance);
          break;
        case ENERGY:
//          unfreezeBalance = delegatedResourceCapsule.getFrozenBalanceForEnergy();
          long frozenBalanceForEnergy = delegatedResourceCapsule.getFrozenBalanceForEnergy();
          long expireTimeForEnergy = delegatedResourceCapsule.getExpireTimeForEnergy(dbManager);
          delegatedResourceCapsule.setFrozenBalanceForEnergy(frozenBalanceForEnergy - actualUnfreezeBalance, expireTimeForEnergy);
          receiverCapsule.addAcquiredDelegatedFrozenBalanceForEnergy(-actualUnfreezeBalance);
          accountCapsule.addDelegatedFrozenBalanceForEnergy(-actualUnfreezeBalance);
          break;
        default:
          //this should never happen
          break;
      }

      //这个要改成锁定
      //accountCapsule.setBalance(oldBalance + unfreezeBalance);
      Long newLockedBalance = actualUnfreezeBalance + accountCapsule.getLockedBalance();
      accountCapsule.setLocked(newLockedBalance, lockedExpireTime);

      dbManager.getAccountStore().put(receiverCapsule.createDbKey(), receiverCapsule);

      if (delegatedResourceCapsule.getFrozenBalanceForBandwidth() == 0
          && delegatedResourceCapsule.getFrozenBalanceForEnergy() == 0) {
        dbManager.getDelegatedResourceStore().delete(key);

        //modify DelegatedResourceAccountIndexStore
        {
          DelegatedResourceAccountIndexCapsule delegatedResourceAccountIndexCapsule = dbManager
              .getDelegatedResourceAccountIndexStore()
              .get(ownerAddress);
          if (delegatedResourceAccountIndexCapsule != null) {
            List<ByteString> toAccountsList = new ArrayList<>(delegatedResourceAccountIndexCapsule
                .getToAccountsList());
            toAccountsList.remove(ByteString.copyFrom(receiverAddress));
            delegatedResourceAccountIndexCapsule.setAllToAccounts(toAccountsList);
            dbManager.getDelegatedResourceAccountIndexStore()
                .put(ownerAddress, delegatedResourceAccountIndexCapsule);
          }
        }

        {
          DelegatedResourceAccountIndexCapsule delegatedResourceAccountIndexCapsule = dbManager
              .getDelegatedResourceAccountIndexStore()
              .get(receiverAddress);
          if (delegatedResourceAccountIndexCapsule != null) {
            List<ByteString> fromAccountsList = new ArrayList<>(delegatedResourceAccountIndexCapsule
                .getFromAccountsList());
            fromAccountsList.remove(ByteString.copyFrom(ownerAddress));
            delegatedResourceAccountIndexCapsule.setAllFromAccounts(fromAccountsList);
            dbManager.getDelegatedResourceAccountIndexStore()
                .put(receiverAddress, delegatedResourceAccountIndexCapsule);
          }
        }

      } else {
        dbManager.getDelegatedResourceStore().put(key, delegatedResourceCapsule);
      }
    } else {
      long newLockedBalance;
      switch (unfreezeBalanceContract.getResource()) {
        case BANDWIDTH:
          List<Frozen> frozenList = Lists.newArrayList();
          frozenList.addAll(accountCapsule.getFrozenList());
          Iterator<Frozen> iterator = frozenList.iterator();
          while (iterator.hasNext()) {
            Frozen next = iterator.next();
            if (next.getExpireTime() <= now) {
              long frozenBalance = next.getFrozenBalance();
              long expireTime = next.getExpireTime();
              long minBalance = Math.min(actualUnfreezeBalance, frozenBalance);
              actualUnfreezeBalance -= minBalance;
              unfreezeBalance += minBalance;
              if (frozenBalance - minBalance > 0){
                accountCapsule.setInstance(accountCapsule.getInstance().toBuilder().clearFrozen().addAllFrozen(new ArrayList<>()).build());
                accountCapsule.setFrozenForBandwidth(frozenBalance-minBalance, expireTime);
              }else {
                accountCapsule.setInstance(accountCapsule.getInstance().toBuilder().clearFrozen().addAllFrozen(new ArrayList<>()).build());
              }
              if (actualUnfreezeBalance == 0){
                break;
              }
            }
          }
          newLockedBalance =
                  unfreezeBalance + accountCapsule.getLockedBalance();
          accountCapsule.setLocked(newLockedBalance, lockedExpireTime);
          break;
        case ENERGY:
          unfreezeBalance = accountCapsule.getAccountResource().getFrozenBalanceForEnergy()
                  .getFrozenBalance();
          long minBalance = Math.min(unfreezeBalance, actualUnfreezeBalance);


          if (unfreezeBalance-minBalance > 0){
            long expireTime =  accountCapsule.getAccountResource()
                    .getFrozenBalanceForEnergy().getExpireTime();
            long newFrozenBalanceForEnergy =
                    accountCapsule.getAccountResource()
                            .getFrozenBalanceForEnergy()
                            .getFrozenBalance() - minBalance;
            accountCapsule.setFrozenForEnergy(newFrozenBalanceForEnergy, expireTime);
            newLockedBalance = minBalance + accountCapsule.getLockedBalance();
            accountCapsule.setLocked(newLockedBalance, lockedExpireTime);

          }else {
            AccountResource newAccountResource = accountCapsule.getAccountResource().toBuilder()
                    .clearFrozenBalanceForEnergy().build();
            accountCapsule.setInstance(accountCapsule.getInstance().toBuilder()
                    .setAccountResource(newAccountResource).build());
            newLockedBalance = minBalance + accountCapsule.getLockedBalance();
            accountCapsule.setLocked(newLockedBalance, lockedExpireTime);
          }

          break;
        default:
          //this should never happen
          break;
      }

    }

    switch (unfreezeBalanceContract.getResource()) {
      case BANDWIDTH:
        dbManager.getDynamicPropertiesStore()
            .addTotalNetWeight(-unfreezeBalance / 1000_000_00L);
        break;
      case ENERGY:
        dbManager.getDynamicPropertiesStore()
            .addTotalEnergyWeight(-unfreezeBalance / 1000_000_00L);
        break;
        default:
        //this should never happen
        break;
    }

    VotesCapsule votesCapsule;
    if (!ArrayUtils.isEmpty(receiverAddress) && dbManager.getDynamicPropertiesStore().supportDR()) {
      if (!dbManager.getVotesStore().has(receiverAddress)) {
        votesCapsule = new VotesCapsule(unfreezeBalanceContract.getReceiverAddress(),
                receiverCapsule.getVotesList());
      } else {
        votesCapsule = dbManager.getVotesStore().get(receiverAddress);
      }

    }else {
      if (!dbManager.getVotesStore().has(ownerAddress)) {
        votesCapsule = new VotesCapsule(unfreezeBalanceContract.getOwnerAddress(),
                accountCapsule.getVotesList());
      } else {
        votesCapsule = dbManager.getVotesStore().get(ownerAddress);
      }
    }


    if (!ArrayUtils.isEmpty(receiverAddress) && dbManager.getDynamicPropertiesStore().supportDR()) {
      receiverCapsule.clearVotes();
      votesCapsule.clearNewVotes();
      dbManager.getAccountStore().put(receiverAddress, receiverCapsule);

      dbManager.getVotesStore().put(receiverAddress, votesCapsule);

      dbManager.getAccountStore().put(ownerAddress, accountCapsule);

    }else {
      accountCapsule.clearVotes();
      votesCapsule.clearNewVotes();
      dbManager.getAccountStore().put(ownerAddress, accountCapsule);

      dbManager.getVotesStore().put(ownerAddress, votesCapsule);
    }

    ret.setUnfreezeAmount(unfreezeBalance);
    ret.setStatus(fee, code.SUCESS);

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
    if (!this.contract.is(UnfreezeBalanceContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [UnfreezeBalanceContract],real type[" + contract
              .getClass() + "]");
    }
    final UnfreezeBalanceContract unfreezeBalanceContract;
    try {
      unfreezeBalanceContract = this.contract.unpack(UnfreezeBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    byte[] ownerAddress = unfreezeBalanceContract.getOwnerAddress().toByteArray();
    if (!Wallet.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }

    long unfreezeBalance = unfreezeBalanceContract.getUnfrozenBalance();
    if (unfreezeBalance <= 0){
      throw new ContractValidateException("Invalid unfreezeBalance");
    }

    long lockedDuration = unfreezeBalanceContract.getLockedDuration();
    long minLockedTime = dbManager.getDynamicPropertiesStore().getMinLockedTime();
    long maxLockedTime = dbManager.getDynamicPropertiesStore().getMaxLockedTime();

    boolean needCheckLockedTime = Args.getInstance().getCheckLockedTime() == 1;//for test
    if (needCheckLockedTime && !(lockedDuration >= minLockedTime
            && lockedDuration <= maxLockedTime)) {
      throw new ContractValidateException(
              "lockedDuration must be less than " + maxLockedTime + " days "
                      + "and more than " + minLockedTime + " days");
    }

    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    if (accountCapsule == null) {
      String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
      throw new ContractValidateException(
          "Account[" + readableOwnerAddress + "] not exists");
    }
    long now = dbManager.getHeadBlockTimeStamp();
    byte[] receiverAddress = unfreezeBalanceContract.getReceiverAddress().toByteArray();
    //If the receiver is not included in the contract, unfreeze frozen balance for this account.
    //otherwise,unfreeze delegated frozen balance provided this account.
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

      byte[] key = DelegatedResourceCapsule
          .createDbKey(unfreezeBalanceContract.getOwnerAddress().toByteArray(),
              unfreezeBalanceContract.getReceiverAddress().toByteArray());
      DelegatedResourceCapsule delegatedResourceCapsule = dbManager.getDelegatedResourceStore()
          .get(key);
      if (delegatedResourceCapsule == null) {
        throw new ContractValidateException(
            "delegated Resource not exists");
      }

      switch (unfreezeBalanceContract.getResource()) {
        case BANDWIDTH:
          if (delegatedResourceCapsule.getFrozenBalanceForBandwidth() <= 0) {
            throw new ContractValidateException("no delegatedFrozenBalance(BANDWIDTH)");
          }

          if (delegatedResourceCapsule.getFrozenBalanceForBandwidth() < unfreezeBalance){
            throw new ContractValidateException("unfreezeBalance greater than allowedUnfreezeCount");
          }
          if (receiverCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth()
              < delegatedResourceCapsule.getFrozenBalanceForBandwidth()) {
            throw new ContractValidateException(
                "AcquiredDelegatedFrozenBalanceForBandwidth[" + receiverCapsule
                    .getAcquiredDelegatedFrozenBalanceForBandwidth() + "] < delegatedBandwidth["
                    + delegatedResourceCapsule.getFrozenBalanceForBandwidth()
                    + "],this should never happen");
          }
          if (delegatedResourceCapsule.getExpireTimeForBandwidth() > now) {
            throw new ContractValidateException("It's not time to unfreeze.");
          }
          break;
        case ENERGY:
          if (delegatedResourceCapsule.getFrozenBalanceForEnergy() <= 0) {
            throw new ContractValidateException("no delegateFrozenBalance(Energy)");
          }
          if (delegatedResourceCapsule.getFrozenBalanceForEnergy() < unfreezeBalance){
            throw new ContractValidateException("unfreezeBalance greater than allowedUnfreezeCount");
          }
          if (receiverCapsule.getAcquiredDelegatedFrozenBalanceForEnergy()
              < delegatedResourceCapsule.getFrozenBalanceForEnergy()) {
            throw new ContractValidateException(
                "AcquiredDelegatedFrozenBalanceForEnergy[" + receiverCapsule
                    .getAcquiredDelegatedFrozenBalanceForEnergy() + "] < delegatedEnergy["
                    + delegatedResourceCapsule.getFrozenBalanceForEnergy() +
                    "],this should never happen");
          }
          if (delegatedResourceCapsule.getExpireTimeForEnergy(dbManager) > now) {
            throw new ContractValidateException("It's not time to unfreeze.");
          }
          break;
        default:
          throw new ContractValidateException(
              "ResourceCode error.valid ResourceCode[BANDWIDTH、Energy]");
      }

    } else {
      switch (unfreezeBalanceContract.getResource()) {
        case BANDWIDTH:
          if (accountCapsule.getFrozenCount() <= 0) {
            throw new ContractValidateException("no frozenBalance(BANDWIDTH)");
          }

          long allowedUnfreezeBalance = accountCapsule.getFrozenBalance();
          if (allowedUnfreezeBalance < unfreezeBalance){
            throw new ContractValidateException("unfreezeBalance greater than allowedUnfreezeCount");
          }

          long allowedUnfreezeCount = accountCapsule.getFrozenList().stream()
              .filter(frozen -> frozen.getExpireTime() <= now).count();
          if (allowedUnfreezeCount <= 0) {
            throw new ContractValidateException("It's not time to unfreeze(BANDWIDTH).");
          }
          break;
        case ENERGY:
          Frozen frozenBalanceForEnergy = accountCapsule.getAccountResource()
              .getFrozenBalanceForEnergy();
          if (frozenBalanceForEnergy.getFrozenBalance() <= 0) {
            throw new ContractValidateException("no frozenBalance(Energy)");
          }

          if (frozenBalanceForEnergy.getExpireTime() > now) {
            throw new ContractValidateException("It's not time to unfreeze(Energy).");
          }

          if (frozenBalanceForEnergy.getFrozenBalance() < unfreezeBalance){
            throw new ContractValidateException("unfreezeBalance greater than allowedUnfreezeCount");
          }

          break;
        default:
          throw new ContractValidateException(
              "ResourceCode error.valid ResourceCode[BANDWIDTH、Energy]");
      }

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
