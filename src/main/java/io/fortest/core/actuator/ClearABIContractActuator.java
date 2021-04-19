package io.fortest.core.actuator;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Arrays;

import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.ContractCapsule;
import io.fortest.core.capsule.TransactionResultCapsule;
import lombok.extern.slf4j.Slf4j;
import io.fortest.common.runtime.config.VMConfig;
import io.fortest.common.utils.StringUtil;
import io.fortest.core.Wallet;
import io.fortest.core.db.AccountStore;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.protos.Contract.ClearABIContract;
import io.fortest.protos.Protocol.Transaction.Result.code;

@Slf4j(topic = "actuator")
public class ClearABIContractActuator extends AbstractActuator {

  ClearABIContractActuator(Any contract, Manager dbManager) {
    super(contract, dbManager);
  }

  @Override
  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
    long fee = calcFee();
    try {
      ClearABIContract usContract = contract.unpack(ClearABIContract.class);

      byte[] contractAddress = usContract.getContractAddress().toByteArray();
      ContractCapsule deployedContract = dbManager.getContractStore().get(contractAddress);

      deployedContract.clearABI();
      dbManager.getContractStore().put(contractAddress, deployedContract);

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
    if (!VMConfig.allowTvmConstantinople()) {
      throw new ContractValidateException(
          "contract type error,unexpected type [ClearABIContract]");
    }

    if (this.contract == null) {
      throw new ContractValidateException("No contract!");
    }
    if (this.dbManager == null) {
      throw new ContractValidateException("No dbManager!");
    }
    if (!this.contract.is(ClearABIContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [ClearABIContract],real type["
              + contract
              .getClass() + "]");
    }
    final ClearABIContract contract;
    try {
      contract = this.contract.unpack(ClearABIContract.class);
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

    AccountCapsule accountCapsule = accountStore.get(ownerAddress);
    if (accountCapsule == null) {
      throw new ContractValidateException(
          "Account[" + readableOwnerAddress + "] not exists");
    }

    byte[] contractAddress = contract.getContractAddress().toByteArray();
    ContractCapsule deployedContract = dbManager.getContractStore().get(contractAddress);

    if (deployedContract == null) {
      throw new ContractValidateException(
          "Contract not exists");
    }

    byte[] deployedContractOwnerAddress = deployedContract.getInstance().getOriginAddress()
        .toByteArray();

    if (!Arrays.equals(ownerAddress, deployedContractOwnerAddress)) {
      throw new ContractValidateException(
          "Account[" + readableOwnerAddress + "] is not the owner of the contract");
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return contract.unpack(ClearABIContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

}
