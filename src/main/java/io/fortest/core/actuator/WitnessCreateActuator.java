package io.fortest.core.actuator;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.TransactionResultCapsule;
import io.fortest.core.capsule.WitnessCapsule;
import io.fortest.core.capsule.utils.TransactionUtil;
import lombok.extern.slf4j.Slf4j;
import io.fortest.common.utils.StringUtil;
import io.fortest.core.Wallet;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.BalanceInsufficientException;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.protos.Contract.WitnessCreateContract;
import io.fortest.protos.Protocol.Transaction.Result.code;

@Slf4j(topic = "actuator")
public class WitnessCreateActuator extends AbstractActuator {

  WitnessCreateActuator(final Any contract, final Manager dbManager) {
    super(contract, dbManager);
  }

  @Override
  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
    long fee = calcFee();
    try {
      final WitnessCreateContract witnessCreateContract = this.contract
          .unpack(WitnessCreateContract.class);
      this.createWitness(witnessCreateContract);
      ret.setStatus(fee, code.SUCESS);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    } catch (BalanceInsufficientException e) {
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
    if (this.dbManager == null) {
      throw new ContractValidateException("No dbManager!");
    }
    if (!this.contract.is(WitnessCreateContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [WitnessCreateContract],real type[" + contract
              .getClass() + "]");
    }
    final WitnessCreateContract contract;
    try {
      contract = this.contract.unpack(WitnessCreateContract.class);
    } catch (InvalidProtocolBufferException e) {
      throw new ContractValidateException(e.getMessage());
    }

    byte[] ownerAddress = contract.getOwnerAddress().toByteArray();
    String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);

    if (!Wallet.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }

    if (!TransactionUtil.validUrl(contract.getUrl().toByteArray())) {
      throw new ContractValidateException("Invalid url");
    }

    AccountCapsule accountCapsule = this.dbManager.getAccountStore().get(ownerAddress);

    if (accountCapsule == null) {
      throw new ContractValidateException("account[" + readableOwnerAddress + "] not exists");
    }
    /* todo later
    if (ArrayUtils.isEmpty(accountCapsule.getAccountName().toByteArray())) {
      throw new ContractValidateException("account name not set");
    } */

    if (this.dbManager.getWitnessStore().has(ownerAddress)) {
      throw new ContractValidateException("Witness[" + readableOwnerAddress + "] has existed");
    }

    if (accountCapsule.getBalance() < dbManager.getDynamicPropertiesStore()
        .getAccountUpgradeCost()) {
      throw new ContractValidateException("balance < AccountUpgradeCost");
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return contract.unpack(WitnessCreateContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return dbManager.getDynamicPropertiesStore().getAccountUpgradeCost();
  }

  private void createWitness(final WitnessCreateContract witnessCreateContract)
      throws BalanceInsufficientException {
    //Create Witness by witnessCreateContract
    final WitnessCapsule witnessCapsule = new WitnessCapsule(
        witnessCreateContract.getOwnerAddress(),
        0,
        witnessCreateContract.getUrl().toStringUtf8());

    logger.debug("createWitness,address[{}]", witnessCapsule.createReadableString());
    this.dbManager.getWitnessStore().put(witnessCapsule.createDbKey(), witnessCapsule);
    AccountCapsule accountCapsule = this.dbManager.getAccountStore()
        .get(witnessCapsule.createDbKey());
    accountCapsule.setIsWitness(true);
    if (dbManager.getDynamicPropertiesStore().getAllowMultiSign() == 1) {
      accountCapsule.setDefaultWitnessPermission(dbManager);
    }
    this.dbManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);
    long cost = dbManager.getDynamicPropertiesStore().getAccountUpgradeCost();
    dbManager.adjustBalance(witnessCreateContract.getOwnerAddress().toByteArray(), -cost);

    dbManager.adjustBalance(this.dbManager.getAccountStore().getBlackhole().createDbKey(), +cost);

    dbManager.getDynamicPropertiesStore().addTotalCreateWitnessCost(cost);
  }
}
