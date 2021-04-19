package io.fortest.core.actuator;

import static io.fortest.core.actuator.ActuatorConstant.ACCOUNT_EXCEPTION_STR;
import static io.fortest.core.actuator.ActuatorConstant.NOT_EXIST_STR;
import static io.fortest.core.actuator.ActuatorConstant.WITNESS_EXCEPTION_STR;

import com.google.common.math.LongMath;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Iterator;
import java.util.Objects;

import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.TransactionResultCapsule;
import io.fortest.core.capsule.VotesCapsule;
import io.fortest.core.config.Parameter;
import lombok.extern.slf4j.Slf4j;
import io.fortest.common.storage.Deposit;
import io.fortest.common.utils.ByteArray;
import io.fortest.common.utils.StringUtil;
import io.fortest.core.Wallet;
import io.fortest.core.db.AccountStore;
import io.fortest.core.db.Manager;
import io.fortest.core.db.VotesStore;
import io.fortest.core.db.WitnessStore;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.protos.Contract.VoteWitnessContract;
import io.fortest.protos.Contract.VoteWitnessContract.Vote;
import io.fortest.protos.Protocol.Transaction.Result.code;

@Slf4j(topic = "actuator")
public class VoteWitnessActuator extends AbstractActuator {


  VoteWitnessActuator(Any contract, Manager dbManager) {
    super(contract, dbManager);
  }

  @Override
  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
    long fee = calcFee();
    try {
      VoteWitnessContract voteContract = contract.unpack(VoteWitnessContract.class);
      countVoteAccount(voteContract, getDeposit());
      ret.setStatus(fee, code.SUCESS);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }
    return true;
  }

  @Override
  public boolean validate() throws ContractValidateException {
    if (this.contract == null) {
      throw new ContractValidateException("No contract!");
    }
    if (dbManager == null && (getDeposit() == null || getDeposit().getDbManager() == null)) {
      throw new ContractValidateException("No dbManager!");
    }
    if (!this.contract.is(VoteWitnessContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [VoteWitnessContract],real type[" + contract
              .getClass() + "]");
    }
    final VoteWitnessContract contract;
    try {
      contract = this.contract.unpack(VoteWitnessContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    if (!Wallet.addressValid(contract.getOwnerAddress().toByteArray())) {
      throw new ContractValidateException("Invalid address");
    }
    byte[] ownerAddress = contract.getOwnerAddress().toByteArray();
    String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);

    AccountStore accountStore = dbManager.getAccountStore();
    WitnessStore witnessStore = dbManager.getWitnessStore();

    if (contract.getVotesCount() == 0) {
      throw new ContractValidateException(
          "VoteNumber must more than 0");
    }
    int maxVoteNumber = Parameter.ChainConstant.MAX_VOTE_NUMBER;
    if (contract.getVotesCount() > maxVoteNumber) {
      throw new ContractValidateException(
          "VoteNumber more than maxVoteNumber " + maxVoteNumber);
    }
    try {
      Iterator<Vote> iterator = contract.getVotesList().iterator();
      Long sum = 0L;
      while (iterator.hasNext()) {
        Vote vote = iterator.next();
        byte[] witnessCandidate = vote.getVoteAddress().toByteArray();
        if (!Wallet.addressValid(witnessCandidate)) {
          throw new ContractValidateException("Invalid vote address!");
        }
        long voteCount = vote.getVoteCount();
        if (voteCount <= 0) {
          throw new ContractValidateException("vote count must be greater than 0");
        }
        String readableWitnessAddress = StringUtil.createReadableString(vote.getVoteAddress());
        if (!Objects.isNull(getDeposit())) {
          if (Objects.isNull(getDeposit().getAccount(witnessCandidate))) {
            throw new ContractValidateException(
                ACCOUNT_EXCEPTION_STR + readableWitnessAddress + NOT_EXIST_STR);
          }
        } else if (!accountStore.has(witnessCandidate)) {
          throw new ContractValidateException(
              ACCOUNT_EXCEPTION_STR + readableWitnessAddress + NOT_EXIST_STR);
        }
        if (!Objects.isNull(getDeposit())) {
          if (Objects.isNull(getDeposit().getWitness(witnessCandidate))) {
            throw new ContractValidateException(
                WITNESS_EXCEPTION_STR + readableWitnessAddress + NOT_EXIST_STR);
          }
        } else if (!witnessStore.has(witnessCandidate)) {
          throw new ContractValidateException(
              WITNESS_EXCEPTION_STR + readableWitnessAddress + NOT_EXIST_STR);
        }
        sum = LongMath.checkedAdd(sum, vote.getVoteCount());
      }

      AccountCapsule accountCapsule =
          (Objects.isNull(getDeposit())) ? accountStore.get(ownerAddress)
              : getDeposit().getAccount(ownerAddress);
      if (accountCapsule == null) {
        throw new ContractValidateException(
            ACCOUNT_EXCEPTION_STR + readableOwnerAddress + NOT_EXIST_STR);
      }

      long mPower = accountCapsule.getMPower(dbManager);

      sum = LongMath.checkedMultiply(sum, 100000000L); //fortest -> drop. The vote count is based on fortest
      if (sum > mPower) {
        throw new ContractValidateException(
            "The total number of votes[" + sum + "] is greater than the fortestPower[" + mPower
                + "]");
      }
    } catch (ArithmeticException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    return true;
  }

  private void countVoteAccount(VoteWitnessContract voteContract, Deposit deposit) {
    byte[] ownerAddress = voteContract.getOwnerAddress().toByteArray();

    VotesCapsule votesCapsule;
    VotesStore votesStore = dbManager.getVotesStore();
    AccountStore accountStore = dbManager.getAccountStore();

    AccountCapsule accountCapsule = (Objects.isNull(getDeposit())) ? accountStore.get(ownerAddress)
        : getDeposit().getAccount(ownerAddress);

    if (!Objects.isNull(getDeposit())) {
      VotesCapsule vCapsule = getDeposit().getVotesCapsule(ownerAddress);
      if (Objects.isNull(vCapsule)) {
        votesCapsule = new VotesCapsule(voteContract.getOwnerAddress(),
            accountCapsule.getVotesList());
      } else {
        votesCapsule = vCapsule;
      }
    } else if (!votesStore.has(ownerAddress)) {
      votesCapsule = new VotesCapsule(voteContract.getOwnerAddress(),
          accountCapsule.getVotesList());
    } else {
      votesCapsule = votesStore.get(ownerAddress);
    }

    accountCapsule.clearVotes();
    votesCapsule.clearNewVotes();

    voteContract.getVotesList().forEach(vote -> {
      logger.debug("countVoteAccount,address[{}]",
          ByteArray.toHexString(vote.getVoteAddress().toByteArray()));

      votesCapsule.addNewVotes(vote.getVoteAddress(), vote.getVoteCount());
      accountCapsule.addVotes(vote.getVoteAddress(), vote.getVoteCount());
    });

    if (Objects.isNull(deposit)) {
      accountStore.put(accountCapsule.createDbKey(), accountCapsule);
      votesStore.put(ownerAddress, votesCapsule);
    } else {
      // cache
      deposit.putAccountValue(accountCapsule.createDbKey(), accountCapsule);
      deposit.putVoteValue(ownerAddress, votesCapsule);
    }

  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return contract.unpack(VoteWitnessContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

}
