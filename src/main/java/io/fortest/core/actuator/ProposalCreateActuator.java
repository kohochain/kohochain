package io.fortest.core.actuator;

import static io.fortest.core.actuator.ActuatorConstant.ACCOUNT_EXCEPTION_STR;
import static io.fortest.core.actuator.ActuatorConstant.NOT_EXIST_STR;
import static io.fortest.core.actuator.ActuatorConstant.WITNESS_EXCEPTION_STR;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.ProposalCapsule;
import io.fortest.core.capsule.TransactionResultCapsule;
import io.fortest.core.config.Parameter;
import io.fortest.core.config.args.Args;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import io.fortest.common.utils.StringUtil;
import io.fortest.core.Wallet;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.protos.Contract.ProposalCreateContract;
import io.fortest.protos.Protocol;
import io.fortest.protos.Protocol.Transaction.Result.code;

@Slf4j(topic = "actuator")
public class ProposalCreateActuator extends AbstractActuator {

  ProposalCreateActuator(final Any contract, final Manager dbManager) {
    super(contract, dbManager);
  }

  @Override
  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
    long fee = calcFee();
    try {
      final ProposalCreateContract proposalCreateContract = this.contract
          .unpack(ProposalCreateContract.class);
      long id = (Objects.isNull(getDeposit())) ?
          dbManager.getDynamicPropertiesStore().getLatestProposalNum() + 1 :
          getDeposit().getLatestProposalNum() + 1;
      ProposalCapsule proposalCapsule =
          new ProposalCapsule(proposalCreateContract.getOwnerAddress(), id);
      proposalCapsule.setParameters(proposalCreateContract.getParametersMap());
      long now = dbManager.getHeadBlockTimeStamp();
      long maintenanceTimeInterval = (Objects.isNull(getDeposit())) ?
          dbManager.getDynamicPropertiesStore().getMaintenanceTimeInterval() :
          getDeposit().getMaintenanceTimeInterval();
      proposalCapsule.setCreateTime(now);

      long currentMaintenanceTime =
          (Objects.isNull(getDeposit())) ? dbManager.getDynamicPropertiesStore()
              .getNextMaintenanceTime() :
              getDeposit().getNextMaintenanceTime();
      long now3 = now + Args.getInstance().getProposalExpireTime();
      long round = (now3 - currentMaintenanceTime) / maintenanceTimeInterval;
      long expirationTime =
          currentMaintenanceTime + (round + 1) * maintenanceTimeInterval;
      proposalCapsule.setExpirationTime(expirationTime);

      if (Objects.isNull(deposit)) {
        dbManager.getProposalStore().put(proposalCapsule.createDbKey(), proposalCapsule);
        dbManager.getDynamicPropertiesStore().saveLatestProposalNum(id);
      } else {
        deposit.putProposalValue(proposalCapsule.createDbKey(), proposalCapsule);
        deposit.putDynamicPropertiesWithLatestProposalNum(id);
      }
      for (Map.Entry<Long, String> entry : proposalCapsule.getParameters().entrySet()) {
          this.preExecute(entry);
      }
      ret.setStatus(fee, code.SUCESS);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    } catch (ContractValidateException e) {
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
    if (dbManager == null && (deposit == null || deposit.getDbManager() == null)) {
      throw new ContractValidateException("No dbManager!");
    }
    if (!this.contract.is(ProposalCreateContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [ProposalCreateContract],real type[" + contract
              .getClass() + "]");
    }
    final ProposalCreateContract contract;
    try {
      contract = this.contract.unpack(ProposalCreateContract.class);
    } catch (InvalidProtocolBufferException e) {
      throw new ContractValidateException(e.getMessage());
    }

    byte[] ownerAddress = contract.getOwnerAddress().toByteArray();
    String readableOwnerAddress = StringUtil.createReadableString(ownerAddress);

    if (!Wallet.addressValid(ownerAddress)) {
      throw new ContractValidateException("Invalid address");
    }

    if (!Objects.isNull(deposit)) {
      if (Objects.isNull(deposit.getAccount(ownerAddress))) {
        throw new ContractValidateException(
            ACCOUNT_EXCEPTION_STR + readableOwnerAddress + NOT_EXIST_STR);
      }
    } else if (!dbManager.getAccountStore().has(ownerAddress)) {
      throw new ContractValidateException(
          ACCOUNT_EXCEPTION_STR + readableOwnerAddress + NOT_EXIST_STR);
    }

    if (!Objects.isNull(getDeposit())) {
      if (Objects.isNull(getDeposit().getWitness(ownerAddress))) {
        throw new ContractValidateException(
            WITNESS_EXCEPTION_STR + readableOwnerAddress + NOT_EXIST_STR);
      }
    } else if (!dbManager.getWitnessStore().has(ownerAddress)) {
      throw new ContractValidateException(
          WITNESS_EXCEPTION_STR + readableOwnerAddress + NOT_EXIST_STR);
    }

    if (contract.getParametersMap().size() == 0) {
      throw new ContractValidateException("This proposal has no parameter.");
    }

    for (Map.Entry<Long, String> entry : contract.getParametersMap().entrySet()) {
      if (!validKey(entry.getKey())) {
        throw new ContractValidateException("Bad chain parameter id");
      }
      validateValue(entry, ownerAddress);
    }

    return true;
  }

  private void preExecute(Map.Entry<Long, String> entry) throws ContractValidateException {
    switch (entry.getKey().intValue()) {
      case (0):
      case (1):
      case (2):
      case (3):
      case (4):
      case (5):
      case (6):
      case (7):
      case (8):
      case (9):
      case (10):
      case (11):
      case (12):
      case (13):
      case (14):
      case (15):
      case (16):
      case (17):
      case (18):
      case (19):
      case (20):
      case (21):
      case (22):
      case (23):
      case (24):
      case (25):
      case (26):
        break;
      case (27): {
        String readableOwnerAddress = entry.getValue();
        byte[] address = new byte[0];
        try {
          address = Hex.decodeHex(readableOwnerAddress);
        } catch (DecoderException e) {
          throw new ContractValidateException("Invalid address");
        }
        dbManager.setAccountStatus(address, Protocol.AccountStatus.Account_Locked);
        break;
      }
      case (28): {
        break;
      }
      default:
        break;
    }
  }


  private void validateValue(Map.Entry<Long, String> entry, byte[] ownerAddress) throws ContractValidateException {
    Long value = null;
    switch (entry.getKey().intValue()) {
      case (0): {
        value  = Long.valueOf(entry.getValue());
        if ( value < 3 * 27 * 1000 || value > 24 * 3600 * 1000) {
          throw new ContractValidateException(
              "Bad chain parameter value,valid range is [3 * 27 * 1000,24 * 3600 * 1000]");
        }
        return;
      }
      case (1):
      case (2):
      case (3):
      case (4):
      case (5):
      case (6):
      case (7):
      case (8): {
        value  = Long.valueOf(entry.getValue());
        if (value < 0 || value > 100_000_000_000_000_000L) {
          throw new ContractValidateException(
              "Bad chain parameter value,valid range is [0,100_000_000_000_000_000L]");
        }
        break;
      }
      case (9): {
        value  = Long.valueOf(entry.getValue());
        if (value != 1) {
          throw new ContractValidateException(
              "This value[ALLOW_CREATION_OF_CONTRACTS] is only allowed to be 1");
        }
        break;
      }
      case (10): {
        value  = Long.valueOf(entry.getValue());
        if (dbManager.getDynamicPropertiesStore().getRemoveThePowerOfTheGr() == -1) {
          throw new ContractValidateException(
              "This proposal has been executed before and is only allowed to be executed once");
        }

        if (value != 1) {
          throw new ContractValidateException(
              "This value[REMOVE_THE_POWER_OF_THE_GR] is only allowed to be 1");
        }
        break;
      }
      case (11):
        break;
      case (12):
        break;
      case (13):
        value  = Long.valueOf(entry.getValue());
        if (value < 10 || value > 100) {
          throw new ContractValidateException(
              "Bad chain parameter value,valid range is [10,100]");
        }
        break;
      case (14): {
        value  = Long.valueOf(entry.getValue());
        if (value != 1) {
          throw new ContractValidateException(
              "This value[ALLOW_UPDATE_ACCOUNT_NAME] is only allowed to be 1");
        }
        break;
      }
      case (15): {
        value  = Long.valueOf(entry.getValue());
        if (value != 1) {
          throw new ContractValidateException(
              "This value[ALLOW_SAME_TOKEN_NAME] is only allowed to be 1");
        }
        break;
      }
      case (16): {
        if (value != 1) {
          throw new ContractValidateException(
              "This value[ALLOW_DELEGATE_RESOURCE] is only allowed to be 1");
        }
        break;
      }
      case (17): { // deprecated
        if (!dbManager.getForkController().pass(Parameter.ForkBlockVersionConsts.ENERGY_LIMIT)) {
          throw new ContractValidateException("Bad chain parameter id");
        }
        if (dbManager.getForkController().pass(Parameter.ForkBlockVersionEnum.VERSION_3_2_2)) {
          throw new ContractValidateException("Bad chain parameter id");
        }
        if (value < 0 || value > 100_000_000_000_000_000L) {
          throw new ContractValidateException(
              "Bad chain parameter value,valid range is [0,100_000_000_000_000_000L]");
        }
        break;
      }
      case (18): {
        if (value != 1) {
          throw new ContractValidateException(
              "This value[ALLOW_TVM_TRANSFER_TRC10] is only allowed to be 1");
        }
        if (dbManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {
          throw new ContractValidateException("[ALLOW_SAME_TOKEN_NAME] proposal must be approved "
              + "before [ALLOW_TVM_TRANSFER_TRC10] can be proposed");
        }
        break;
      }
      case (19): {
        if (!dbManager.getForkController().pass(Parameter.ForkBlockVersionEnum.VERSION_3_2_2)) {
          throw new ContractValidateException("Bad chain parameter id");
        }
        if (value < 0 || value > 100_000_000_000_000_000L) {
          throw new ContractValidateException(
              "Bad chain parameter value,valid range is [0,100_000_000_000_000_000L]");
        }
        break;
      }
      case (20): {
        if (!dbManager.getForkController().pass(Parameter.ForkBlockVersionEnum.VERSION_3_5)) {
          throw new ContractValidateException("Bad chain parameter id: ALLOW_MULTI_SIGN");
        }
        if (value != 1) {
          throw new ContractValidateException(
              "This value[ALLOW_MULTI_SIGN] is only allowed to be 1");
        }
        break;
      }
      case (21): {
        if (!dbManager.getForkController().pass(Parameter.ForkBlockVersionEnum.VERSION_3_5)) {
          throw new ContractValidateException("Bad chain parameter id: ALLOW_ADAPTIVE_ENERGY");
        }
        if (value != 1) {
          throw new ContractValidateException(
              "This value[ALLOW_ADAPTIVE_ENERGY] is only allowed to be 1");
        }
        break;
      }
      case (22): {
        if (!dbManager.getForkController().pass(Parameter.ForkBlockVersionEnum.VERSION_3_5)) {
          throw new ContractValidateException(
              "Bad chain parameter id: UPDATE_ACCOUNT_PERMISSION_FEE");
        }
        if (value < 0 || value > 100_000_000_000L) {
          throw new ContractValidateException(
              "Bad chain parameter value,valid range is [0,100_000_000_000L]");
        }
        break;
      }
      case (23): {
        if (!dbManager.getForkController().pass(Parameter.ForkBlockVersionEnum.VERSION_3_5)) {
          throw new ContractValidateException("Bad chain parameter id: MULTI_SIGN_FEE");
        }
        if (value < 0 || value > 100_000_000_000L) {
          throw new ContractValidateException(
              "Bad chain parameter value,valid range is [0,100_000_000_000L]");
        }
        break;
      }
      case (24): {
        if (!dbManager.getForkController().pass(Parameter.ForkBlockVersionEnum.VERSION_3_6)) {
          throw new ContractValidateException("Bad chain parameter id");
        }
        if (value != 1 && value != 0) {
          throw new ContractValidateException(
              "This value[ALLOW_PROTO_FILTER_NUM] is only allowed to be 1 or 0");
        }
        break;
      }
      case (25): {
        if (!dbManager.getForkController().pass(Parameter.ForkBlockVersionEnum.VERSION_3_6)) {
          throw new ContractValidateException("Bad chain parameter id");
        }
        if (value != 1 && value != 0) {
          throw new ContractValidateException(
              "This value[ALLOW_ACCOUNT_STATE_ROOT] is only allowed to be 1 or 0");
        }
        break;
      }
      case (26): {
        if (!dbManager.getForkController().pass(Parameter.ForkBlockVersionEnum.VERSION_3_6)) {
          throw new ContractValidateException("Bad chain parameter id");
        }
        if (value != 1) {
          throw new ContractValidateException(
              "This value[ALLOW_TVM_CONSTANTINOPLE] is only allowed to be 1");
        }
        if (dbManager.getDynamicPropertiesStore().getAllowTvmTransferTrc10() == 0) {
          throw new ContractValidateException(
              "[ALLOW_TVM_TRANSFER_TRC10] proposal must be approved "
                  + "before [ALLOW_TVM_CONSTANTINOPLE] can be proposed");
        }
        break;
      }
      case (27): {
        String readableOwnerAddress = entry.getValue();
        byte[] address = new byte[0];
        try {
          address = Hex.decodeHex(readableOwnerAddress);
        } catch (DecoderException e) {
          throw new ContractValidateException("Invalid address");
        }
        AccountCapsule ownerAccount = dbManager.getAccountStore().get(address);
        if (ownerAccount == null) {
          throw new ContractValidateException("Validate TransferContract error, no OwnerAccount.");
        }
        if (!Wallet.addressValid(address)) {
          throw new ContractValidateException("Invalid address");
        }

        if (Arrays.equals(address, ownerAddress)) {
          throw new ContractValidateException("Cannot locked yourself.");
        }

        break;
      }
      case (28): {
        String readableOwnerAddress = entry.getValue();
        byte[] address = new byte[0];
        try {
          address = Hex.decodeHex(readableOwnerAddress);
        } catch (DecoderException e) {
          throw new ContractValidateException("Invalid address");
        }
        ;
        AccountCapsule account = dbManager.getAccountStore().get(address);
        if (account == null) {
          throw new ContractValidateException("Validate TransferContract error, no OwnerAccount.");
        }
        if (!Wallet.addressValid(address)) {
          throw new ContractValidateException("Invalid address");
        }
        Protocol.AccountStatus status = account.getAccountStatus();
        if (status == null){
          throw new ContractValidateException("account status is normal");
        }else if (status.equals(Protocol.AccountStatus.Account_Normal)){
          throw new ContractValidateException("account status is normal");
        }
        break;
      }
      default:
        break;
    }
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return contract.unpack(ProposalCreateContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }

  private boolean validKey(long idx) {
    return idx >= 0 && idx < Parameter.ChainParameters.values().length;
  }

}
