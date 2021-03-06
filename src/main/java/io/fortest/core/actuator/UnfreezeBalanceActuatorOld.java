//package io.fortest.core.actuator;
//
//import com.google.common.collect.Lists;
//import com.google.protobuf.Any;
//import com.google.protobuf.ByteString;
//import com.google.protobuf.InvalidProtocolBufferException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Iterator;
//import java.util.List;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.ArrayUtils;
//import StringUtil;
//import Wallet;
//import AccountCapsule;
//import DelegatedResourceAccountIndexCapsule;
//import DelegatedResourceCapsule;
//import TransactionResultCapsule;
//import VotesCapsule;
//import Args;
//import Manager;
//import ContractExeException;
//import ContractValidateException;
//import Contract.UnfreezeBalanceContract;
//import Protocol.Account.AccountResource;
//import Protocol.Account.Frozen;
//import Protocol.AccountType;
//import Protocol.Transaction.Result.code;
//
//@Slf4j(topic = "actuator")
//public class UnfreezeBalanceActuatorOld extends AbstractActuator {
//
//  UnfreezeBalanceActuatorOld(Any contract, Manager dbManager) {
//    super(contract, dbManager);
//  }
//
//  @Override
//  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
//    long fee = calcFee();
//    final UnfreezeBalanceContract unfreezeBalanceContract;
//    try {
//      unfreezeBalanceContract = contract.unpack(UnfreezeBalanceContract.class);
//    } catch (InvalidProtocolBufferException e) {
//      logger.debug(e.getMessage(), e);
//      ret.setStatus(fee, code.FAILED);
//      throw new ContractExeException(e.getMessage());
//    }
//    byte[] ownerAddress = unfreezeBalanceContract.getOwnerAddress().toByteArray();
//
//    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
//    long oldBalance = accountCapsule.getBalance();
//
//    long unfreezeBalance = 0L;
//
//    long now = dbManager.getHeadBlockTimeStamp();
//    long lockedDuration = unfreezeBalanceContract.getLockedDuration() * 86_400_000;
//
//
//
//    long lockedExpireTime = now + lockedDuration;
//
//
//    long actualUnfreezeBalance = unfreezeBalanceContract.getUnfrozenBalance();
//    ;
////    unfreezeBalanceContract.getReceiverAddress().toByteArray()
//    byte[] receiverAddress = null;
//    //If the receiver is not included in the contract, unfreeze frozen balance for this account.
//    //otherwise,unfreeze delegated frozen balance provided this account. unfreezeBalanceContract.getReceiverAddress().toByteArray()
//    if (!ArrayUtils.isEmpty(receiverAddress) && dbManager.getDynamicPropertiesStore().supportDR()) {
//      byte[] key = DelegatedResourceCapsule
//          .createDbKey(unfreezeBalanceContract.getOwnerAddress().toByteArray(), null);
//      DelegatedResourceCapsule delegatedResourceCapsule = dbManager.getDelegatedResourceStore()
//          .get(key);
//
//      switch (unfreezeBalanceContract.getResource()) {
//        case BANDWIDTH:
//          unfreezeBalance = delegatedResourceCapsule.getFrozenBalanceForBandwidth();
//          delegatedResourceCapsule.setFrozenBalanceForBandwidth(0, 0);
//          accountCapsule.addDelegatedFrozenBalanceForBandwidth(-unfreezeBalance);
//          break;
//        case ENERGY:
//          unfreezeBalance = delegatedResourceCapsule.getFrozenBalanceForEnergy();
//          delegatedResourceCapsule.setFrozenBalanceForEnergy(0, 0);
//          accountCapsule.addDelegatedFrozenBalanceForEnergy(-unfreezeBalance);
//          break;
//        default:
//          //this should never happen
//          break;
//      }
//
//      AccountCapsule receiverCapsule = dbManager.getAccountStore().get(receiverAddress);
//      if (dbManager.getDynamicPropertiesStore().getAllowTvmConstantinople() == 0 ||
//          (receiverCapsule != null && receiverCapsule.getType() != AccountType.Contract)) {
//        switch (unfreezeBalanceContract.getResource()) {
//          case BANDWIDTH:
//            receiverCapsule.addAcquiredDelegatedFrozenBalanceForBandwidth(-unfreezeBalance);
//            break;
//          case ENERGY:
//            receiverCapsule.addAcquiredDelegatedFrozenBalanceForEnergy(-unfreezeBalance);
//            break;
//          default:
//            //this should never happen
//            break;
//        }
//        dbManager.getAccountStore().put(receiverCapsule.createDbKey(), receiverCapsule);
//      }
//
//      accountCapsule.setBalance(oldBalance + unfreezeBalance);
//
//      if (delegatedResourceCapsule.getFrozenBalanceForBandwidth() == 0
//          && delegatedResourceCapsule.getFrozenBalanceForEnergy() == 0) {
//        dbManager.getDelegatedResourceStore().delete(key);
//
//        //modify DelegatedResourceAccountIndexStore
//        {
//          DelegatedResourceAccountIndexCapsule delegatedResourceAccountIndexCapsule = dbManager
//              .getDelegatedResourceAccountIndexStore()
//              .get(ownerAddress);
//          if (delegatedResourceAccountIndexCapsule != null) {
//            List<ByteString> toAccountsList = new ArrayList<>(delegatedResourceAccountIndexCapsule
//                .getToAccountsList());
//            toAccountsList.remove(ByteString.copyFrom(receiverAddress));
//            delegatedResourceAccountIndexCapsule.setAllToAccounts(toAccountsList);
//            dbManager.getDelegatedResourceAccountIndexStore()
//                .put(ownerAddress, delegatedResourceAccountIndexCapsule);
//          }
//        }
//
//        {
//          DelegatedResourceAccountIndexCapsule delegatedResourceAccountIndexCapsule = dbManager
//              .getDelegatedResourceAccountIndexStore()
//              .get(receiverAddress);
//          if (delegatedResourceAccountIndexCapsule != null) {
//            List<ByteString> fromAccountsList = new ArrayList<>(delegatedResourceAccountIndexCapsule
//                .getFromAccountsList());
//            fromAccountsList.remove(ByteString.copyFrom(ownerAddress));
//            delegatedResourceAccountIndexCapsule.setAllFromAccounts(fromAccountsList);
//            dbManager.getDelegatedResourceAccountIndexStore()
//                .put(receiverAddress, delegatedResourceAccountIndexCapsule);
//          }
//        }
//
//      } else {
//        dbManager.getDelegatedResourceStore().put(key, delegatedResourceCapsule);
//      }
//    } else {
//      long newLockedBalance;
//      switch (unfreezeBalanceContract.getResource()) {
//        case BANDWIDTH:
//          List<Frozen> frozenList = Lists.newArrayList();
//          frozenList.addAll(accountCapsule.getFrozenList());
//          Iterator<Frozen> iterator = frozenList.iterator();
//          while (iterator.hasNext()) {
//            Frozen next = iterator.next();
//            if (next.getExpireTime() <= now) {
//              long frozenBalance = next.getFrozenBalance();
//              long expireTime = next.getExpireTime();
//              long minBalance = Math.min(actualUnfreezeBalance, frozenBalance);
//              actualUnfreezeBalance -= minBalance;
//              unfreezeBalance += minBalance;
//              if (frozenBalance - minBalance > 0){
//                accountCapsule.setInstance(accountCapsule.getInstance().toBuilder().clearFrozen().addAllFrozen(new ArrayList<>()).build());
//                accountCapsule.setFrozenForBandwidth(frozenBalance-minBalance, expireTime);
//              }else {
//                accountCapsule.setInstance(accountCapsule.getInstance().toBuilder().clearFrozen().addAllFrozen(new ArrayList<>()).build());
//              }
//              if (actualUnfreezeBalance == 0){
//                break;
//              }
//            }
//          }
//          newLockedBalance =
//                  unfreezeBalance + accountCapsule.getLockedBalance();
//          accountCapsule.setLocked(newLockedBalance, lockedExpireTime);
//          break;
//        case ENERGY:
//          unfreezeBalance = accountCapsule.getAccountResource().getFrozenBalanceForEnergy()
//              .getFrozenBalance();
//          long minBalance = Math.min(unfreezeBalance, actualUnfreezeBalance);
//
//
//          if (unfreezeBalance-minBalance > 0){
//            long expireTime =  accountCapsule.getAccountResource()
//                    .getFrozenBalanceForEnergy().getExpireTime();
//            long newFrozenBalanceForEnergy =
//                    accountCapsule.getAccountResource()
//                            .getFrozenBalanceForEnergy()
//                            .getFrozenBalance() - minBalance;
//            accountCapsule.setFrozenForEnergy(newFrozenBalanceForEnergy, expireTime);
//            newLockedBalance = minBalance + accountCapsule.getLockedBalance();
//            accountCapsule.setLocked(newLockedBalance, lockedExpireTime);
//
//          }else {
//            AccountResource newAccountResource = accountCapsule.getAccountResource().toBuilder()
//                    .clearFrozenBalanceForEnergy().build();
//            accountCapsule.setInstance(accountCapsule.getInstance().toBuilder()
//                    .setAccountResource(newAccountResource).build());
//            newLockedBalance = minBalance + accountCapsule.getLockedBalance();
//            accountCapsule.setLocked(newLockedBalance, lockedExpireTime);
//          }
//
//          break;
//        default:
//          //this should never happen
//          break;
//      }
//
//    }
//
//    switch (unfreezeBalanceContract.getResource()) {
//      case BANDWIDTH:
//        dbManager.getDynamicPropertiesStore()
//            .addTotalNetWeight(- unfreezeBalanceContract.getUnfrozenBalance() / 1000_000_00L);
//        break;
//      case ENERGY:
//        dbManager.getDynamicPropertiesStore()
//            .addTotalEnergyWeight(- unfreezeBalanceContract.getUnfrozenBalance() / 1000_000_00L);
//        break;
//      default:
//        //this should never happen
//        break;
//    }
//
//    VotesCapsule votesCapsule;
//    if (!dbManager.getVotesStore().has(ownerAddress)) {
//      votesCapsule = new VotesCapsule(unfreezeBalanceContract.getOwnerAddress(),
//          accountCapsule.getVotesList());
//    } else {
//      votesCapsule = dbManager.getVotesStore().get(ownerAddress);
//    }
//    accountCapsule.clearVotes();
//    votesCapsule.clearNewVotes();
//
//    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
//
//    dbManager.getVotesStore().put(ownerAddress, votesCapsule);
//
//    ret.setUnfreezeAmount(unfreezeBalance);
//    ret.setStatus(fee, code.SUCESS);
//
//    return true;
//  }
//
//  @Override
//  public boolean validate() throws ContractValidateException {
//    if (this.contract == null) {
//      throw new ContractValidateException("No contract!");
//    }
//    if (this.dbManager == null) {
//      throw new ContractValidateException("No dbManager!");
//    }
//    if (!this.contract.is(UnfreezeBalanceContract.class)) {
//      throw new ContractValidateException(
//          "contract type error,expected type [UnfreezeBalanceContract],real type[" + contract
//              .getClass() + "]");
//    }
//    final UnfreezeBalanceContract unfreezeBalanceContract;
//    try {
//      unfreezeBalanceContract = this.contract.unpack(UnfreezeBalanceContract.class);
//    } catch (InvalidProtocolBufferException e) {
//      logger.debug(e.getMessage(), e);
//      throw new ContractValidateException(e.getMessage());
//    }
//    byte[] ownerAddress = unfreezeBalanceContract.getOwnerAddress().toByteArray();
//    if (!Wallet.addressValid(ownerAddress)) {
//      throw new ContractValidateException("Invalid address");
//    }
//
//
//    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
//    if (accountCapsule == null) {
//      String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
//      throw new ContractValidateException(
//          "Account[" + readableOwnerAddress + "] not exists");
//    }
//
//
//    long unfreezeBalance = unfreezeBalanceContract.getUnfrozenBalance();
//    if (unfreezeBalance <= 0){
//      throw new ContractValidateException("Invalid unfreezeBalance");
//    }
//
//    long lockedDuration = unfreezeBalanceContract.getLockedDuration();
//    long minLockedTime = dbManager.getDynamicPropertiesStore().getMinLockedTime();
//    long maxLockedTime = dbManager.getDynamicPropertiesStore().getMaxLockedTime();
//
//    boolean needCheckLockedTime = Args.getInstance().getCheckLockedTime() == 1;//for test
//    if (needCheckLockedTime && !(lockedDuration >= minLockedTime
//            && lockedDuration <= maxLockedTime)) {
//      throw new ContractValidateException(
//              "frozenDuration must be less than " + maxLockedTime + " days "
//                      + "and more than " + minLockedTime + " days");
//    }
//
//    long now = dbManager.getHeadBlockTimeStamp();
//    byte[] receiverAddress = null;
//    //If the receiver is not included in the contract, unfreeze frozen balance for this account.
//    //otherwise,unfreeze delegated frozen balance provided this account.
//    if (!ArrayUtils.isEmpty(receiverAddress) && dbManager.getDynamicPropertiesStore().supportDR()) {
//      if (Arrays.equals(receiverAddress, ownerAddress)) {
//        throw new ContractValidateException(
//            "receiverAddress must not be the same as ownerAddress");
//      }
//
//      if (!Wallet.addressValid(receiverAddress)) {
//        throw new ContractValidateException("Invalid receiverAddress");
//      }
//
//      AccountCapsule receiverCapsule = dbManager.getAccountStore().get(receiverAddress);
//      if (dbManager.getDynamicPropertiesStore().getAllowTvmConstantinople() == 0
//          && receiverCapsule == null) {
//        String readableReceiverAddress = StringUtil.createReadableString(receiverAddress);
//        throw new ContractValidateException(
//            "Receiver Account[" + readableReceiverAddress + "] not exists");
//      }
//
//      byte[] key = DelegatedResourceCapsule
//          .createDbKey(unfreezeBalanceContract.getOwnerAddress().toByteArray(), null);
//      DelegatedResourceCapsule delegatedResourceCapsule = dbManager.getDelegatedResourceStore()
//          .get(key);
//      if (delegatedResourceCapsule == null) {
//        throw new ContractValidateException(
//            "delegated Resource not exists");
//      }
//
//      switch (unfreezeBalanceContract.getResource()) {
//        case BANDWIDTH:
//          if (delegatedResourceCapsule.getFrozenBalanceForBandwidth() <= 0) {
//            throw new ContractValidateException("no delegatedFrozenBalance(BANDWIDTH)");
//          }
//
//          if (dbManager.getDynamicPropertiesStore().getAllowTvmConstantinople() == 0) {
//            if (receiverCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth()
//                < delegatedResourceCapsule.getFrozenBalanceForBandwidth()) {
//              throw new ContractValidateException(
//                  "AcquiredDelegatedFrozenBalanceForBandwidth[" + receiverCapsule
//                      .getAcquiredDelegatedFrozenBalanceForBandwidth() + "] < delegatedBandwidth["
//                      + delegatedResourceCapsule.getFrozenBalanceForBandwidth()
//                      + "]");
//            }
//          } else {
//            if (receiverCapsule != null && receiverCapsule.getType() != AccountType.Contract
//                && receiverCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth()
//                < delegatedResourceCapsule.getFrozenBalanceForBandwidth()) {
//              throw new ContractValidateException(
//                  "AcquiredDelegatedFrozenBalanceForBandwidth[" + receiverCapsule
//                      .getAcquiredDelegatedFrozenBalanceForBandwidth() + "] < delegatedBandwidth["
//                      + delegatedResourceCapsule.getFrozenBalanceForBandwidth()
//                      + "]");
//            }
//          }
//
//          if (delegatedResourceCapsule.getExpireTimeForBandwidth() > now) {
//            throw new ContractValidateException("It's not time to unfreeze.");
//          }
//          break;
//        case ENERGY:
//          if (delegatedResourceCapsule.getFrozenBalanceForEnergy() <= 0) {
//            throw new ContractValidateException("no delegateFrozenBalance(Energy)");
//          }
//          if (dbManager.getDynamicPropertiesStore().getAllowTvmConstantinople() == 0) {
//            if (receiverCapsule.getAcquiredDelegatedFrozenBalanceForEnergy()
//                < delegatedResourceCapsule.getFrozenBalanceForEnergy()) {
//              throw new ContractValidateException(
//                  "AcquiredDelegatedFrozenBalanceForEnergy[" + receiverCapsule
//                      .getAcquiredDelegatedFrozenBalanceForEnergy() + "] < delegatedEnergy["
//                      + delegatedResourceCapsule.getFrozenBalanceForEnergy() +
//                      "]");
//            }
//          } else {
//            if (receiverCapsule != null && receiverCapsule.getType() != AccountType.Contract
//                && receiverCapsule.getAcquiredDelegatedFrozenBalanceForEnergy()
//                < delegatedResourceCapsule.getFrozenBalanceForEnergy()) {
//              throw new ContractValidateException(
//                  "AcquiredDelegatedFrozenBalanceForEnergy[" + receiverCapsule
//                      .getAcquiredDelegatedFrozenBalanceForEnergy() + "] < delegatedEnergy["
//                      + delegatedResourceCapsule.getFrozenBalanceForEnergy() +
//                      "]");
//            }
//          }
//
//          if (delegatedResourceCapsule.getExpireTimeForEnergy(dbManager) > now) {
//            throw new ContractValidateException("It's not time to unfreeze.");
//          }
//          break;
//        default:
//          throw new ContractValidateException(
//              "ResourceCode error.valid ResourceCode[BANDWIDTH???Energy]");
//      }
//
//    } else {
//      switch (unfreezeBalanceContract.getResource()) {
//        case BANDWIDTH:
//          if (accountCapsule.getFrozenCount() <= 0) {
//            throw new ContractValidateException("no frozenBalance(BANDWIDTH)");
//          }
//
//          long allowedUnfreezeCount = accountCapsule.getFrozenList().stream()
//              .filter(frozen -> frozen.getExpireTime() <= now).count();
//          if (allowedUnfreezeCount <= 0) {
//            throw new ContractValidateException("It's not time to unfreeze(BANDWIDTH).");
//          }
//          long allowedUnfreezeBalance = accountCapsule.getFrozenBalance();
//          if (allowedUnfreezeBalance < unfreezeBalance){
//            throw new ContractValidateException("unfreezeBalance greater than allowedUnfreezeCount");
//          }
//          break;
//        case ENERGY:
//          Frozen frozenBalanceForEnergy = accountCapsule.getAccountResource()
//              .getFrozenBalanceForEnergy();
//          if (frozenBalanceForEnergy.getFrozenBalance() <= 0) {
//            throw new ContractValidateException("no frozenBalance(Energy)");
//          }
//          if (frozenBalanceForEnergy.getExpireTime() > now) {
//            throw new ContractValidateException("It's not time to unfreeze(Energy).");
//          }
//          if (frozenBalanceForEnergy.getFrozenBalance() < unfreezeBalance){
//            throw new ContractValidateException("unfreezeBalance greater than allowedUnfreezeCount");
//          }
//
//          break;
//        default:
//          throw new ContractValidateException(
//              "ResourceCode error.valid ResourceCode[BANDWIDTH???Energy]");
//      }
//
//    }
//
//    return true;
//  }
//
//  @Override
//  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
//    return contract.unpack(UnfreezeBalanceContract.class).getOwnerAddress();
//  }
//
//  @Override
//  public long calcFee() {
//    return 0;
//  }
//
//}
