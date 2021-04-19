package io.fortest.core.witness;

import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Map;

import io.fortest.core.capsule.ProposalCapsule;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.protos.Protocol;
import io.fortest.protos.Protocol.Proposal.State;

@Slf4j(topic = "witness")
public class ProposalController {

  @Setter
  @Getter
  private Manager manager;

  public static ProposalController createInstance(Manager manager) {
    ProposalController instance = new ProposalController();
    instance.setManager(manager);
    return instance;
  }


  public void processProposals() throws ContractValidateException {
    long latestProposalNum = manager.getDynamicPropertiesStore().getLatestProposalNum();
    if (latestProposalNum == 0) {
      logger.info("latestProposalNum is 0,return");
      return;
    }

    long proposalNum = latestProposalNum;

    ProposalCapsule proposalCapsule = null;

    while (proposalNum > 0) {
      try {
        proposalCapsule = manager.getProposalStore()
            .get(ProposalCapsule.calculateDbKey(proposalNum));
      } catch (Exception ex) {
        logger.error("", ex);
        continue;
      }

      if (proposalCapsule.hasProcessed()) {
        logger
            .info("Proposal has processed，id:[{}],skip it and before it",
                proposalCapsule.getID());
        //proposals with number less than this one, have been processed before
        break;
      }

      if (proposalCapsule.hasCanceled()) {
        logger.info("Proposal has canceled，id:[{}],skip it", proposalCapsule.getID());
        proposalNum--;
        continue;
      }

      long currentTime = manager.getDynamicPropertiesStore().getNextMaintenanceTime();
      if (proposalCapsule.hasExpired(currentTime)) {
         processProposal(proposalCapsule);
        proposalNum--;
        continue;
      }

      proposalNum--;
      logger.info("Proposal has not expired，id:[{}],skip it", proposalCapsule.getID());
    }
    logger.info("Processing proposals done, oldest proposal[{}]", proposalNum);
  }

  public void processProposal(ProposalCapsule proposalCapsule) throws ContractValidateException {

    List<ByteString> activeWitnesses = this.manager.getWitnessScheduleStore().getActiveWitnesses();
    if (proposalCapsule.hasMostApprovals(activeWitnesses)) {
      logger.info(
          "Processing proposal,id:{},it has received most approvals, "
              + "begin to set dynamic parameter:{}, "
              + "and set proposal state as APPROVED",
          proposalCapsule.getID(), proposalCapsule.getParameters());
      setDynamicParameters(proposalCapsule);
      proposalCapsule.setState(State.APPROVED);
      manager.getProposalStore().put(proposalCapsule.createDbKey(), proposalCapsule);
    } else {
      logger.info(
          "Processing proposal,id:{}, "
              + "it has not received enough approvals, set proposal state as DISAPPROVED",
          proposalCapsule.getID());
      proposalCapsule.setState(State.DISAPPROVED);
      this.ifDisApproved(proposalCapsule);
      manager.getProposalStore().put(proposalCapsule.createDbKey(), proposalCapsule);
    }

  }

  public void ifDisApproved(ProposalCapsule proposalCapsule) throws ContractValidateException {
    Map<Long, String> map = proposalCapsule.getInstance().getParametersMap();
    for (Map.Entry<Long, String> entry : map.entrySet()) {
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
          manager.setAccountStatus(address, Protocol.AccountStatus.Account_Normal);
          break;
        }
        case (28): {
          break;
        }
        default:
          break;
      }
    }
  }

  public void setDynamicParameters(ProposalCapsule proposalCapsule) throws ContractValidateException {
    Map<Long, String> map = proposalCapsule.getInstance().getParametersMap();
    for (Map.Entry<Long, String> entry : map.entrySet()) {

      switch (entry.getKey().intValue()) {
        case (0): {
          manager.getDynamicPropertiesStore().saveMaintenanceTimeInterval(Long.valueOf(entry.getValue()));
          break;
        }
        case (1): {
          manager.getDynamicPropertiesStore().saveAccountUpgradeCost(Long.valueOf(entry.getValue()));
          break;
        }
        case (2): {
          manager.getDynamicPropertiesStore().saveCreateAccountFee(Long.valueOf(entry.getValue()));
          break;
        }
        case (3): {
          manager.getDynamicPropertiesStore().saveTransactionFee(Long.valueOf(entry.getValue()));
          break;
        }
        case (4): {
          manager.getDynamicPropertiesStore().saveAssetIssueFee(Long.valueOf(entry.getValue()));
          break;
        }
        case (5): {
          manager.getDynamicPropertiesStore().saveWitnessPayPerBlock(Long.valueOf(entry.getValue()));
          break;
        }
        case (6): {
          manager.getDynamicPropertiesStore().saveWitnessStandbyAllowance(Long.valueOf(entry.getValue()));
          break;
        }
        case (7): {
          manager.getDynamicPropertiesStore()
              .saveCreateNewAccountFeeInSystemContract(Long.valueOf(entry.getValue()));
          break;
        }
        case (8): {
          manager.getDynamicPropertiesStore().saveCreateNewAccountBandwidthRate(Long.valueOf(entry.getValue()));
          break;
        }
        case (9): {
          manager.getDynamicPropertiesStore().saveAllowCreationOfContracts(Long.valueOf(entry.getValue()));
          break;
        }
        case (10): {
          if (manager.getDynamicPropertiesStore().getRemoveThePowerOfTheGr() == 0) {
            manager.getDynamicPropertiesStore().saveRemoveThePowerOfTheGr(Long.valueOf(entry.getValue()));
          }
          break;
        }
        case (11): {
          manager.getDynamicPropertiesStore().saveEnergyFee(Long.valueOf(entry.getValue()));
          break;
        }
        case (12): {
          manager.getDynamicPropertiesStore().saveExchangeCreateFee(Long.valueOf(entry.getValue()));
          break;
        }
        case (13): {
          manager.getDynamicPropertiesStore().saveMaxCpuTimeOfOneTx(Long.valueOf(entry.getValue()));
          break;
        }
        case (14): {
          manager.getDynamicPropertiesStore().saveAllowUpdateAccountName(Long.valueOf(entry.getValue()));
          break;
        }
        case (15): {
          manager.getDynamicPropertiesStore().saveAllowSameTokenName(Long.valueOf(entry.getValue()));
          break;
        }
        case (16): {
          manager.getDynamicPropertiesStore().saveAllowDelegateResource(Long.valueOf(entry.getValue()));
          break;
        }
        case (17): {
          manager.getDynamicPropertiesStore().saveTotalEnergyLimit(Long.valueOf(entry.getValue()));
          break;
        }
        case (18): {
          manager.getDynamicPropertiesStore().saveAllowTvmTransferTrc10(Long.valueOf(entry.getValue()));
          break;
        }
        case (19): {
          manager.getDynamicPropertiesStore().saveTotalEnergyLimit2(Long.valueOf(entry.getValue()));
          break;
        }
        case (20): {
          if (manager.getDynamicPropertiesStore().getAllowMultiSign() == 0) {
            manager.getDynamicPropertiesStore().saveAllowMultiSign(Long.valueOf(entry.getValue()));
          }
          break;
        }
        case (21): {
          if (manager.getDynamicPropertiesStore().getAllowAdaptiveEnergy() == 0) {
            manager.getDynamicPropertiesStore().saveAllowAdaptiveEnergy(Long.valueOf(entry.getValue()));
          }
          break;
        }
        case (22): {
          manager.getDynamicPropertiesStore().saveUpdateAccountPermissionFee(Long.valueOf(entry.getValue()));
          break;
        }
        case (23): {
          manager.getDynamicPropertiesStore().saveMultiSignFee(Long.valueOf(entry.getValue()));
          break;
        }
        case (24): {
          manager.getDynamicPropertiesStore().saveAllowProtoFilterNum(Long.valueOf(entry.getValue()));
          break;
        }
        case (25): {
          manager.getDynamicPropertiesStore().saveAllowAccountStateRoot(Long.valueOf(entry.getValue()));
          break;
        }
        case (26): {
          manager.getDynamicPropertiesStore().saveAllowTvmConstantinople(Long.valueOf(entry.getValue()));
          manager.getDynamicPropertiesStore().addSystemContractAndSetPermission(48);
          break;
        }
        case (27): {
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
          manager.setAccountStatus(address, Protocol.AccountStatus.Account_Normal);
          break;
        }
        default:
          break;
      }
    }
  }

}
