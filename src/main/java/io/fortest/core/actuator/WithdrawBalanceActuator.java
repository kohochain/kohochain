package io.fortest.core.actuator;

import static io.fortest.core.actuator.ActuatorConstant.ACCOUNT_EXCEPTION_STR;

import com.google.common.math.LongMath;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Arrays;
import java.util.Objects;

import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.TransactionResultCapsule;
import io.fortest.core.config.args.Args;
import lombok.extern.slf4j.Slf4j;
import io.fortest.common.utils.StringUtil;
import io.fortest.core.Wallet;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.protos.Contract.WithdrawBalanceContract;
import io.fortest.protos.Protocol.Transaction.Result.code;

@Slf4j(topic = "actuator")
public class WithdrawBalanceActuator extends AbstractActuator {

  WithdrawBalanceActuator(Any contract, Manager dbManager) {
    super(contract, dbManager);
  }


  @Override
  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
    long fee = calcFee();
    final WithdrawBalanceContract withdrawBalanceContract;
    try {
      withdrawBalanceContract = contract.unpack(WithdrawBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    }

    AccountCapsule accountCapsule = (Objects.isNull(getDeposit())) ? dbManager.getAccountStore().
        get(withdrawBalanceContract.getOwnerAddress().toByteArray())
        : getDeposit().getAccount(withdrawBalanceContract.getOwnerAddress().toByteArray());
    long oldBalance = accountCapsule.getBalance();
    long allowance = accountCapsule.getAllowance();

    long now = dbManager.getHeadBlockTimeStamp();
    accountCapsule.setInstance(accountCapsule.getInstance().toBuilder()
        .setBalance(oldBalance + allowance)
        .setAllowance(0L)
        .setLatestWithdrawTime(now)
        .build());
    if (Objects.isNull(getDeposit())) {
      dbManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);
    } else {
      // cache
      deposit.putAccountValue(accountCapsule.createDbKey(), accountCapsule);
    }

    ret.setWithdrawAmount(allowance);
    ret.setStatus(fee, code.SUCESS);

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
    if (!this.contract.is(WithdrawBalanceContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [WithdrawBalanceContract],real type[" + contract
              .getClass() + "]");
    }
    final WithdrawBalanceContract withdrawBalanceContract;
    try {
      withdrawBalanceContract = this.contract.unpack(WithdrawBalanceContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }
    byte[] ownerAddress = withdrawBalanceContract.getOwnerAddress().toByteArray();
    if (!Wallet.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }

    AccountCapsule accountCapsule =
        Objects.isNull(getDeposit()) ? dbManager.getAccountStore().get(ownerAddress)
            : getDeposit().getAccount(ownerAddress);
    if (accountCapsule == null) {
      String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
      throw new ContractValidateException(
          ACCOUNT_EXCEPTION_STR + readableOwnerAddress + "] not exists");
    }

    String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);
    if (!dbManager.getWitnessStore().has(ownerAddress)) {
      throw new ContractValidateException(
          ACCOUNT_EXCEPTION_STR + readableOwnerAddress + "] is not a witnessAccount");
    }

    boolean isGP = Args.getInstance().getGenesisBlock().getWitnesses().stream().anyMatch(witness ->
        Arrays.equals(ownerAddress, witness.getAddress()));
    if (isGP) {
      throw new ContractValidateException(
          ACCOUNT_EXCEPTION_STR + readableOwnerAddress
              + "] is a guard representative and is not allowed to withdraw Balance");
    }

    long latestWithdrawTime = accountCapsule.getLatestWithdrawTime();
    long now = dbManager.getHeadBlockTimeStamp();
    long witnessAllowanceFrozenTime = Objects.isNull(getDeposit()) ?
        dbManager.getDynamicPropertiesStore().getWitnessAllowanceFrozenTime() * 86_400_000L :
        getDeposit().getWitnessAllowanceFrozenTime() * 86_400_000L;

    if (now - latestWithdrawTime < witnessAllowanceFrozenTime) {
      throw new ContractValidateException("The last withdraw time is "
          + latestWithdrawTime + ",less than 24 hours");
    }

    if (accountCapsule.getAllowance() <= 0) {
      throw new ContractValidateException("witnessAccount does not have any allowance");
    }
    try {
      LongMath.checkedAdd(accountCapsule.getBalance(), accountCapsule.getAllowance());
    } catch (ArithmeticException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return contract.unpack(WithdrawBalanceContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

}
