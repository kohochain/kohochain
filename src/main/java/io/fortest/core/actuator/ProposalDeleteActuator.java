package io.fortest.core.actuator;

import static io.fortest.core.actuator.ActuatorConstant.ACCOUNT_EXCEPTION_STR;
import static io.fortest.core.actuator.ActuatorConstant.NOT_EXIST_STR;
import static io.fortest.core.actuator.ActuatorConstant.PROPOSAL_EXCEPTION_STR;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Map;
import java.util.Objects;

import io.fortest.core.capsule.ProposalCapsule;
import io.fortest.core.capsule.TransactionResultCapsule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import io.fortest.common.utils.ByteArray;
import io.fortest.common.utils.StringUtil;
import io.fortest.core.Wallet;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.core.exception.ItemNotFoundException;
import io.fortest.protos.Contract.ProposalDeleteContract;
import io.fortest.protos.Protocol;
import io.fortest.protos.Protocol.Proposal.State;
import io.fortest.protos.Protocol.Transaction.Result.code;

@Slf4j(topic = "actuator")
public class ProposalDeleteActuator extends AbstractActuator {

  ProposalDeleteActuator(final Any contract, final Manager dbManager) {
    super(contract, dbManager);
  }

  @Override
  public boolean execute(TransactionResultCapsule ret) throws ContractExeException {
    long fee = calcFee();
    try {
      final ProposalDeleteContract proposalDeleteContract = this.contract
          .unpack(ProposalDeleteContract.class);
      ProposalCapsule proposalCapsule = (Objects.isNull(deposit)) ? dbManager.getProposalStore().
          get(ByteArray.fromLong(proposalDeleteContract.getProposalId())) :
          deposit.getProposalCapsule(ByteArray.fromLong(proposalDeleteContract.getProposalId()));
      proposalCapsule.setState(State.CANCELED);
      if (Objects.isNull(deposit)) {
        dbManager.getProposalStore().put(proposalCapsule.createDbKey(), proposalCapsule);
      } else {
        deposit.putProposalValue(proposalCapsule.createDbKey(), proposalCapsule);
      }
      for (Map.Entry<Long, String> entry : proposalCapsule.getParameters().entrySet()) {
        this.preExecute(entry);
      }
      ret.setStatus(fee, code.SUCESS);
    } catch (InvalidProtocolBufferException e) {
      logger.debug(e.getMessage(), e);
      ret.setStatus(fee, code.FAILED);
      throw new ContractExeException(e.getMessage());
    } catch (ItemNotFoundException e) {
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
        dbManager.setAccountStatus(address, Protocol.AccountStatus.Account_Normal);
        break;
      }
      case (28): {
        break;
      }
      default:
        break;
    }
  }

  @Override
  public boolean validate() throws ContractValidateException {
    if (this.contract == null) {
      throw new ContractValidateException("No contract!");
    }
    if (dbManager == null && (deposit == null || deposit.getDbManager() == null)) {
      throw new ContractValidateException("No dbManager!");
    }
    if (!this.contract.is(ProposalDeleteContract.class)) {
      throw new ContractValidateException(
          "contract type error,expected type [ProposalDeleteContract],real type[" + contract
              .getClass() + "]");
    }
    final ProposalDeleteContract contract;
    try {
      contract = this.contract.unpack(ProposalDeleteContract.class);
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
      throw new ContractValidateException(ACCOUNT_EXCEPTION_STR + readableOwnerAddress
          + NOT_EXIST_STR);
    }

    long latestProposalNum = Objects.isNull(deposit) ? dbManager.getDynamicPropertiesStore()
        .getLatestProposalNum() : deposit.getLatestProposalNum();
    if (contract.getProposalId() > latestProposalNum) {
      throw new ContractValidateException(PROPOSAL_EXCEPTION_STR + contract.getProposalId()
          + NOT_EXIST_STR);
    }

    ProposalCapsule proposalCapsule;
    try {
      proposalCapsule = Objects.isNull(getDeposit()) ? dbManager.getProposalStore().
          get(ByteArray.fromLong(contract.getProposalId())) :
          deposit.getProposalCapsule(ByteArray.fromLong(contract.getProposalId()));
    } catch (ItemNotFoundException ex) {
      throw new ContractValidateException(PROPOSAL_EXCEPTION_STR + contract.getProposalId()
          + NOT_EXIST_STR);
    }

    long now = dbManager.getHeadBlockTimeStamp();
    if (!proposalCapsule.getProposalAddress().equals(contract.getOwnerAddress())) {
      throw new ContractValidateException(PROPOSAL_EXCEPTION_STR + contract.getProposalId() + "] "
          + "is not proposed by " + readableOwnerAddress);
    }
    if (now >= proposalCapsule.getExpirationTime()) {
      throw new ContractValidateException(PROPOSAL_EXCEPTION_STR + contract.getProposalId()
          + "] expired");
    }
    if (proposalCapsule.getState() == State.CANCELED) {
      throw new ContractValidateException(PROPOSAL_EXCEPTION_STR + contract.getProposalId()
          + "] canceled");
    }

    return true;
  }

  @Override
  public ByteString getOwnerAddress() throws InvalidProtocolBufferException {
    return contract.unpack(ProposalDeleteContract.class).getOwnerAddress();
  }

  @Override
  public long calcFee() {
    return 0;
  }
}
