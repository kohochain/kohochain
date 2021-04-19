package io.fortest.core.actuator;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Arrays;

import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.TransactionResultCapsule;
import io.fortest.core.config.Parameter;
import lombok.extern.slf4j.Slf4j;
import io.fortest.common.storage.Deposit;
import io.fortest.core.Wallet;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.BalanceInsufficientException;
import io.fortest.core.exception.BlackListException;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.protos.Contract.TransferContract;
import io.fortest.protos.Protocol;
import io.fortest.protos.Protocol.AccountType;
import io.fortest.protos.Protocol.Transaction.Result.code;

@Slf4j(topic = "actuator")
public class TransferActuator extends AbstractActuator {

  TransferActuator(Any contract, Manager dbManager) {
    super(contract, dbManager);
  }

  @Override
  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
    long fee = calcFee();
    try {
      TransferContract transferContract = contract.unpack(TransferContract.class);
      long amount = transferContract.getAmount();
      byte[] toAddress = transferContract.getToAddress().toByteArray();
      byte[] ownerAddress = transferContract.getOwnerAddress().toByteArray();
      AccountCapsule ownerAccount = dbManager.getAccountStore().get(toAddress);
      if (ownerAccount != null && ownerAccount.getAccountStatus().equals(Protocol.AccountStatus.Account_Locked)){
        throw new BlackListException("account is locked");
      }
      // if account with to_address does not exist, create it first.
      AccountCapsule toAccount = dbManager.getAccountStore().get(toAddress);
      if (toAccount == null) {
        boolean withDefaultPermission =
            dbManager.getDynamicPropertiesStore().getAllowMultiSign() == 1;
        toAccount = new AccountCapsule(ByteString.copyFrom(toAddress), AccountType.Normal,
            dbManager.getHeadBlockTimeStamp(), withDefaultPermission, dbManager);
        dbManager.getAccountStore().put(toAddress, toAccount);

        fee = fee + dbManager.getDynamicPropertiesStore().getCreateNewAccountFeeInSystemContract();
      }

      dbManager.adjustBalance(ownerAddress, -fee);
      dbManager.adjustBalance(dbManager.getAccountStore().getBlackhole().createDbKey(), fee);
      ret.setStatus(fee, code.SUCESS);
      dbManager.adjustBalance(ownerAddress, -amount);
      dbManager.adjustBalance(toAddress, amount);
    } catch (BalanceInsufficientException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    } catch (ArithmeticException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    } catch (BlackListException e) {
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
    if (!this.contract.is(TransferContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [TransferContract],real type[" + contract
              .getClass() + "]");
    }
    long fee = calcFee();
    final TransferContract transferContract;
    try {
      transferContract = contract.unpack(TransferContract.class);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    byte[] toAddress = transferContract.getToAddress().toByteArray();
    byte[] ownerAddress = transferContract.getOwnerAddress().toByteArray();
    long amount = transferContract.getAmount();

    if (!Wallet.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid ownerAddress");
    }
    if (!Wallet.addressValid(toAddress)) {
      throw new ContractValidateException("Invalid toAddress");
    }

    if (Arrays.equals(toAddress, ownerAddress)) {
      throw new ContractValidateException("Cannot transfer fortest to yourself.");
    }

    AccountCapsule ownerAccount = dbManager.getAccountStore().get(ownerAddress);
    if (ownerAccount == null) {
      throw new ContractValidateException("Validate TransferContract error, no OwnerAccount.");
    }
    Protocol.AccountStatus accountStatus = ownerAccount.getAccountStatus();
    if (accountStatus != null && accountStatus.equals(Protocol.AccountStatus.Account_Locked)){
      throw new ContractValidateException("Validate TransferContract error, account is locked.");
    }

    long balance = ownerAccount.getBalance();

    if (amount <= 0) {
      throw new ContractValidateException("Amount must greater than 0.");
    }

    try {
      AccountCapsule toAccount = dbManager.getAccountStore().get(toAddress);
      if (toAccount == null) {
        fee = fee + dbManager.getDynamicPropertiesStore().getCreateNewAccountFeeInSystemContract();
      }

      if (balance < Math.addExact(amount, fee)) {
        throw new ContractValidateException(
            "Validate TransferContract error, balance is not sufficient.");
      }

      if (toAccount != null) {
        long toAddressBalance = Math.addExact(toAccount.getBalance(), amount);
      }
    } catch (ArithmeticException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    return true;
  }


  public static boolean validateForSmartContract(Deposit deposit, byte[] ownerAddress,
      byte[] toAddress, long amount) throws ContractValidateException {
    if (!Wallet.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid ownerAddress");
    }
    if (!Wallet.addressValid(toAddress)) {
      throw new ContractValidateException("Invalid toAddress");
    }

    if (Arrays.equals(toAddress, ownerAddress)) {
      throw new ContractValidateException("Cannot transfer kht to yourself.");
    }

    AccountCapsule ownerAccount = deposit.getAccount(ownerAddress);
    if (ownerAccount == null) {
      throw new ContractValidateException("Validate InternalTransfer error, no OwnerAccount.");
    }

    Protocol.AccountStatus accountStatus = ownerAccount.getAccountStatus();
    if (accountStatus != null && accountStatus.equals(Protocol.AccountStatus.Account_Locked)){
      throw new ContractValidateException("Validate TransferContract error, account is locked.");
    }

    AccountCapsule toAccount = deposit.getAccount(toAddress);
    if (toAccount == null) {
      throw new ContractValidateException(
          "Validate InternalTransfer error, no ToAccount. And not allowed to create account in smart contract.");
    }

    long balance = ownerAccount.getBalance();

    if (amount < 0) {
      throw new ContractValidateException("Amount must greater than or equals 0.");
    }

    try {
      if (balance < amount) {
        throw new ContractValidateException(
            "Validate InternalTransfer error, balance is not sufficient.");
      }

      if (toAccount != null) {
        long toAddressBalance = Math.addExact(toAccount.getBalance(), amount);
      }
    } catch (ArithmeticException e) {
      logger.debug(e.getMessage(), e);
      throw new ContractValidateException(e.getMessage());
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return contract.unpack(TransferContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return Parameter.ChainConstant.TRANSFER_FEE;
  }

}