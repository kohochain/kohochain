/*
 * Copyright (c) [2016] [ <ether.camp> ]
 * This file is part of the ethereumJ library.
 *
 * The ethereumJ library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The ethereumJ library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with the ethereumJ library. If not, see <http://www.gnu.org/licenses/>.
 */

package io.fortest.core;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.fortest.api.GrpcAPI;
import io.fortest.common.crypto.ECKey;
import io.fortest.common.crypto.Hash;
import io.fortest.common.overlay.discover.node.Node;
import io.fortest.common.overlay.discover.node.NodeHandler;
import io.fortest.common.overlay.discover.node.NodeManager;
import io.fortest.common.overlay.message.Message;
import io.fortest.common.runtime.Runtime;
import io.fortest.common.runtime.RuntimeImpl;
import io.fortest.common.runtime.config.VMConfig;
import io.fortest.common.runtime.vm.program.ProgramResult;
import io.fortest.common.runtime.vm.program.invoke.ProgramInvokeFactoryImpl;
import io.fortest.common.storage.DepositImpl;
import io.fortest.core.actuator.Actuator;
import io.fortest.core.actuator.ActuatorFactory;
import io.fortest.core.config.Parameter;
import io.fortest.core.config.args.Args;
import io.fortest.protos.Contract;
import io.fortest.protos.Protocol;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.spongycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import io.fortest.common.utils.Base58;
import io.fortest.common.utils.ByteArray;
import io.fortest.common.utils.ByteUtil;
import io.fortest.common.utils.Sha256Hash;
import io.fortest.common.utils.Utils;
import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.AssetIssueCapsule;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.ContractCapsule;
import io.fortest.core.capsule.DelegatedResourceAccountIndexCapsule;
import io.fortest.core.capsule.DelegatedResourceCapsule;
import io.fortest.core.capsule.ExchangeCapsule;
import io.fortest.core.capsule.ProposalCapsule;
import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.core.capsule.TransactionInfoCapsule;
import io.fortest.core.capsule.TransactionResultCapsule;
import io.fortest.core.capsule.WitnessCapsule;
import io.fortest.core.db.AccountIdIndexStore;
import io.fortest.core.db.AccountStore;
import io.fortest.core.db.BandwidthProcessor;
import io.fortest.core.db.ContractStore;
import io.fortest.core.db.EnergyProcessor;
import io.fortest.core.db.Manager;
import io.fortest.core.exception.AccountResourceInsufficientException;
import io.fortest.core.exception.BadItemException;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.core.exception.DupTransactionException;
import io.fortest.core.exception.HeaderNotFound;
import io.fortest.core.exception.NonUniqueObjectException;
import io.fortest.core.exception.PermissionException;
import io.fortest.core.exception.SignatureFormatException;
import io.fortest.core.exception.StoreException;
import io.fortest.core.exception.TaposException;
import io.fortest.core.exception.TooBigTransactionException;
import io.fortest.core.exception.TransactionExpirationException;
import io.fortest.core.exception.VMIllegalException;
import io.fortest.core.exception.ValidateSignatureException;
import io.fortest.core.net.khcNetDelegate;
import io.fortest.core.net.khcNetService;
import io.fortest.core.net.message.TransactionMessage;

@Slf4j
@Component
public class Wallet {

  @Getter
  private final ECKey ecKey;
  @Autowired
  private khcNetService khcNetService;
  @Autowired
  private khcNetDelegate khcNetDelegate;
  @Autowired
  @Getter
  private Manager dbManager;
  @Autowired
  private NodeManager nodeManager;
  private static String addressPreFixString = Constant.ADD_PRE_FIX_STRING_MAINNET;//default testnet
  private static byte addressPreFixByte = Constant.ADD_PRE_FIX_BYTE_MAINNET;

  private int minEffectiveConnection = Args.getInstance().getMinEffectiveConnection();

  /**
   * Creates a new Wallet with a random ECKey.
   */
  public Wallet() {
    this.ecKey = new ECKey(Utils.getRandom());
  }

  /**
   * Creates a Wallet with an existing ECKey.
   */
  public Wallet(final ECKey ecKey) {
    this.ecKey = ecKey;
    logger.info("wallet address: {}", ByteArray.toHexString(this.ecKey.getAddress()));
  }

  public static boolean isConstant(Protocol.SmartContract.ABI abi, Contract.TriggerSmartContract triggerSmartContract)
      throws ContractValidateException {
    try {
      boolean constant = isConstant(abi, getSelector(triggerSmartContract.getData().toByteArray()));
      if (constant) {
        if (!Args.getInstance().isSupportConstant()) {
          throw new ContractValidateException("this node don't support constant");
        }
      }
      return constant;
    } catch (ContractValidateException e) {
      throw e;
    } catch (Exception e) {
      return false;
    }
  }

  public byte[] getAddress() {
    return ecKey.getAddress();
  }

  public static String getAddressPreFixString() {
    return addressPreFixString;
  }

  public static void setAddressPreFixString(String addressPreFixString) {
    Wallet.addressPreFixString = addressPreFixString;
  }

  public static byte getAddressPreFixByte() {
    return addressPreFixByte;
  }

  public static void setAddressPreFixByte(byte addressPreFixByte) {
    Wallet.addressPreFixByte = addressPreFixByte;
  }

  public static boolean addressValid(byte[] address) {
    if (ArrayUtils.isEmpty(address)) {
      logger.warn("Warning: Address is empty !!");
      return false;
    }
    if (address.length != Constant.ADDRESS_SIZE / 2) {
      logger.warn(
          "Warning: Address length need " + Constant.ADDRESS_SIZE + " but " + address.length
              + " !!");
      return false;
    }
    if (address[0] != addressPreFixByte) {
      logger.warn("Warning: Address need prefix with " + addressPreFixByte + " but "
          + address[0] + " !!");
      return false;
    }
    //Other rule;
    return true;
  }

  public static String encode58Check(byte[] input) {
    byte[] hash0 = Sha256Hash.hash(input);
    byte[] hash1 = Sha256Hash.hash(hash0);
    byte[] inputCheck = new byte[input.length + 4];
    System.arraycopy(input, 0, inputCheck, 0, input.length);
    System.arraycopy(hash1, 0, inputCheck, input.length, 4);
    return Base58.encode(inputCheck);
  }

  private static byte[] decode58Check(String input) {
    byte[] decodeCheck = Base58.decode(input);
    if (decodeCheck.length <= 4) {
      return null;
    }
    byte[] decodeData = new byte[decodeCheck.length - 4];
    System.arraycopy(decodeCheck, 0, decodeData, 0, decodeData.length);
    byte[] hash0 = Sha256Hash.hash(decodeData);
    byte[] hash1 = Sha256Hash.hash(hash0);
    if (hash1[0] == decodeCheck[decodeData.length] &&
        hash1[1] == decodeCheck[decodeData.length + 1] &&
        hash1[2] == decodeCheck[decodeData.length + 2] &&
        hash1[3] == decodeCheck[decodeData.length + 3]) {
      return decodeData;
    }
    return null;
  }

  public static byte[] generateContractAddress(Protocol.Transaction kht) {

    Contract.CreateSmartContract contract = ContractCapsule.getSmartContractFromTransaction(kht);
    byte[] ownerAddress = contract.getOwnerAddress().toByteArray();
    TransactionCapsule khtCap = new TransactionCapsule(kht);
    byte[] txRawDataHash = khtCap.getTransactionId().getBytes();

    byte[] combined = new byte[txRawDataHash.length + ownerAddress.length];
    System.arraycopy(txRawDataHash, 0, combined, 0, txRawDataHash.length);
    System.arraycopy(ownerAddress, 0, combined, txRawDataHash.length, ownerAddress.length);

    return Hash.sha3omit12(combined);

  }

  public static byte[] generateContractAddress(byte[] ownerAddress, byte[] txRawDataHash) {

    byte[] combined = new byte[txRawDataHash.length + ownerAddress.length];
    System.arraycopy(txRawDataHash, 0, combined, 0, txRawDataHash.length);
    System.arraycopy(ownerAddress, 0, combined, txRawDataHash.length, ownerAddress.length);

    return Hash.sha3omit12(combined);

  }

  // for `CREATE2`
  public static byte[] generateContractAddress2(byte[] address, byte[] salt, byte[] code) {
    byte[] mergedData = ByteUtil.merge(address, salt, Hash.sha3(code));
    return Hash.sha3omit12(mergedData);
  }

  // for `CREATE`
  public static byte[] generateContractAddress(byte[] transactionRootId, long nonce) {
    byte[] nonceBytes = Longs.toByteArray(nonce);
    byte[] combined = new byte[transactionRootId.length + nonceBytes.length];
    System.arraycopy(transactionRootId, 0, combined, 0, transactionRootId.length);
    System.arraycopy(nonceBytes, 0, combined, transactionRootId.length, nonceBytes.length);

    return Hash.sha3omit12(combined);
  }

  public static byte[] decodeFromBase58Check(String addressBase58) {
    if (StringUtils.isEmpty(addressBase58)) {
      logger.warn("Warning: Address is empty !!");
      return null;
    }
    byte[] address = decode58Check(addressBase58);
    if (address == null) {
      return null;
    }

    if (!addressValid(address)) {
      return null;
    }

    return address;
  }


  public Protocol.Account getAccount(Protocol.Account account) {
    AccountStore accountStore = dbManager.getAccountStore();
    AccountCapsule accountCapsule = accountStore.get(account.getAddress().toByteArray());
    if (accountCapsule == null) {
      return null;
    }
    BandwidthProcessor processor = new BandwidthProcessor(dbManager);
    processor.updateUsage(accountCapsule);

    EnergyProcessor energyProcessor = new EnergyProcessor(dbManager);
    energyProcessor.updateUsage(accountCapsule);

    long genesisTimeStamp = dbManager.getGenesisBlock().getTimeStamp();
    accountCapsule.setLatestConsumeTime(genesisTimeStamp
        + Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL * accountCapsule.getLatestConsumeTime());
    accountCapsule.setLatestConsumeFreeTime(genesisTimeStamp
        + Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL * accountCapsule.getLatestConsumeFreeTime());
    accountCapsule.setLatestConsumeTimeForEnergy(genesisTimeStamp
        + Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL * accountCapsule.getLatestConsumeTimeForEnergy());

    return accountCapsule.getInstance();
  }


  public Protocol.Account getAccountById(Protocol.Account account) {
    AccountStore accountStore = dbManager.getAccountStore();
    AccountIdIndexStore accountIdIndexStore = dbManager.getAccountIdIndexStore();
    byte[] address = accountIdIndexStore.get(account.getAccountId());
    if (address == null) {
      return null;
    }
    AccountCapsule accountCapsule = accountStore.get(address);
    if (accountCapsule == null) {
      return null;
    }
    BandwidthProcessor processor = new BandwidthProcessor(dbManager);
    processor.updateUsage(accountCapsule);

    EnergyProcessor energyProcessor = new EnergyProcessor(dbManager);
    energyProcessor.updateUsage(accountCapsule);

    return accountCapsule.getInstance();
  }

  /**
   * Create a transaction.
   */
  /*public Transaction createTransaction(byte[] address, String to, long amount) {
    long balance = getBalance(address);
    return new TransactionCapsule(address, to, amount, balance, utxoStore).getInstance();
  } */

  /**
   * Create a transaction by contract.
   */
  @Deprecated
  public Protocol.Transaction createTransaction(Contract.TransferContract contract) {
    AccountStore accountStore = dbManager.getAccountStore();
    return new TransactionCapsule(contract, accountStore).getInstance();
  }


  public TransactionCapsule createTransactionCapsule(com.google.protobuf.Message message,
      Protocol.Transaction.Contract.ContractType contractType) throws ContractValidateException {
    TransactionCapsule kht = new TransactionCapsule(message, contractType);
    if (contractType != Protocol.Transaction.Contract.ContractType.CreateSmartContract
        && contractType != Protocol.Transaction.Contract.ContractType.TriggerSmartContract) {
      List<Actuator> actList = ActuatorFactory.createActuator(kht, dbManager);
      for (Actuator act : actList) {
        act.validate();
      }
    }

    if (contractType == Protocol.Transaction.Contract.ContractType.CreateSmartContract) {

      Contract.CreateSmartContract contract = ContractCapsule
          .getSmartContractFromTransaction(kht.getInstance());
      long percent = contract.getNewContract().getConsumeUserResourcePercent();
      if (percent < 0 || percent > 100) {
        throw new ContractValidateException("percent must be >= 0 and <= 100");
      }
    }

    try {
      BlockCapsule.BlockId blockId = dbManager.getHeadBlockId();
      if (Args.getInstance().getkhtReferenceBlock().equals("solid")) {
        blockId = dbManager.getSolidBlockId();
      }
      kht.setReference(blockId.getNum(), blockId.getBytes());
      long expiration =
          dbManager.getHeadBlockTimeStamp() + Args.getInstance()
              .getkhtExpirationTimeInMilliseconds();
      kht.setExpiration(expiration);
      kht.setTimestamp();
    } catch (Exception e) {
      logger.error("Create transaction capsule failed.", e);
    }
    return kht;
  }

  /**
   * Broadcast a transaction.
   */
  public GrpcAPI.Return broadcastTransaction(Protocol.Transaction signaturedTransaction) {
    GrpcAPI.Return.Builder builder = GrpcAPI.Return.newBuilder();
    TransactionCapsule kht = new TransactionCapsule(signaturedTransaction);
    try {
      Message message = new TransactionMessage(signaturedTransaction.toByteArray());
      if (minEffectiveConnection != 0) {
        if (khcNetDelegate.getActivePeer().isEmpty()) {
          logger.warn("Broadcast transaction {} failed, no connection.", kht.getTransactionId());
          return builder.setResult(false).setCode(GrpcAPI.Return.response_code.NO_CONNECTION)
              .setMessage(ByteString.copyFromUtf8("no connection"))
              .build();
        }

        int count = (int) khcNetDelegate.getActivePeer().stream()
            .filter(p -> !p.isNeedSyncFromUs() && !p.isNeedSyncFromPeer())
            .count();

        if (count < minEffectiveConnection) {
          String info = "effective connection:" + count + " lt minEffectiveConnection:"
              + minEffectiveConnection;
          logger.warn("Broadcast transaction {} failed, {}.", kht.getTransactionId(), info);
          return builder.setResult(false).setCode(GrpcAPI.Return.response_code.NOT_ENOUGH_EFFECTIVE_CONNECTION)
              .setMessage(ByteString.copyFromUtf8(info))
              .build();
        }
      }

      if (dbManager.isTooManyPending()) {
        logger.warn("Broadcast transaction {} failed, too many pending.", kht.getTransactionId());
        return builder.setResult(false).setCode(GrpcAPI.Return.response_code.SERVER_BUSY).build();
      }

      if (dbManager.isGeneratingBlock()) {
        logger
            .warn("Broadcast transaction {} failed, is generating block.", kht.getTransactionId());
        return builder.setResult(false).setCode(GrpcAPI.Return.response_code.SERVER_BUSY).build();
      }

      if (dbManager.getTransactionIdCache().getIfPresent(kht.getTransactionId()) != null) {
        logger.warn("Broadcast transaction {} failed, is already exist.", kht.getTransactionId());
        return builder.setResult(false).setCode(GrpcAPI.Return.response_code.DUP_TRANSACTION_ERROR).build();
      } else {
        dbManager.getTransactionIdCache().put(kht.getTransactionId(), true);
      }
      if (dbManager.getDynamicPropertiesStore().supportVM()) {
        kht.resetResult();
      }
      dbManager.pushTransaction(kht);
      khcNetService.broadcast(message);
      logger.info("Broadcast transaction {} successfully.", kht.getTransactionId());
      return builder.setResult(true).setCode(GrpcAPI.Return.response_code.SUCCESS).build();
    } catch (ValidateSignatureException e) {
      logger.error("Broadcast transaction {} failed, {}.", kht.getTransactionId(), e.getMessage());
      return builder.setResult(false).setCode(GrpcAPI.Return.response_code.SIGERROR)
          .setMessage(ByteString.copyFromUtf8("validate signature error " + e.getMessage()))
          .build();
    } catch (ContractValidateException e) {
      logger.error("Broadcast transaction {} failed, {}.", kht.getTransactionId(), e.getMessage());
      return builder.setResult(false).setCode(GrpcAPI.Return.response_code.CONTRACT_VALIDATE_ERROR)
          .setMessage(ByteString.copyFromUtf8("contract validate error : " + e.getMessage()))
          .build();
    } catch (ContractExeException e) {
      logger.error("Broadcast transaction {} failed, {}.", kht.getTransactionId(), e.getMessage());
      return builder.setResult(false).setCode(GrpcAPI.Return.response_code.CONTRACT_EXE_ERROR)
          .setMessage(ByteString.copyFromUtf8("contract execute error : " + e.getMessage()))
          .build();
    } catch (AccountResourceInsufficientException e) {
      logger.error("Broadcast transaction {} failed, {}.", kht.getTransactionId(), e.getMessage());
      return builder.setResult(false).setCode(GrpcAPI.Return.response_code.BANDWITH_ERROR)
          .setMessage(ByteString.copyFromUtf8("AccountResourceInsufficient error"))
          .build();
    } catch (DupTransactionException e) {
      logger.error("Broadcast transaction {} failed, {}.", kht.getTransactionId(), e.getMessage());
      return builder.setResult(false).setCode(GrpcAPI.Return.response_code.DUP_TRANSACTION_ERROR)
          .setMessage(ByteString.copyFromUtf8("dup transaction"))
          .build();
    } catch (TaposException e) {
      logger.error("Broadcast transaction {} failed, {}.", kht.getTransactionId(), e.getMessage());
      return builder.setResult(false).setCode(GrpcAPI.Return.response_code.TAPOS_ERROR)
          .setMessage(ByteString.copyFromUtf8("Tapos check error"))
          .build();
    } catch (TooBigTransactionException e) {
      logger.error("Broadcast transaction {} failed, {}.", kht.getTransactionId(), e.getMessage());
      return builder.setResult(false).setCode(GrpcAPI.Return.response_code.TOO_BIG_TRANSACTION_ERROR)
          .setMessage(ByteString.copyFromUtf8("transaction size is too big"))
          .build();
    } catch (TransactionExpirationException e) {
      logger.error("Broadcast transaction {} failed, {}.", kht.getTransactionId(), e.getMessage());
      return builder.setResult(false).setCode(GrpcAPI.Return.response_code.TRANSACTION_EXPIRATION_ERROR)
          .setMessage(ByteString.copyFromUtf8("transaction expired"))
          .build();
    } catch (Exception e) {
      logger.error("Broadcast transaction {} failed, {}.", kht.getTransactionId(), e.getMessage());
      return builder.setResult(false).setCode(GrpcAPI.Return.response_code.OTHER_ERROR)
          .setMessage(ByteString.copyFromUtf8("other error : " + e.getMessage()))
          .build();
    }
  }

  public TransactionCapsule getTransactionSign(Protocol.TransactionSign transactionSign) {
    byte[] privateKey = transactionSign.getPrivateKey().toByteArray();
    TransactionCapsule kht = new TransactionCapsule(transactionSign.getTransaction());
    kht.sign(privateKey);
    return kht;
  }

  public TransactionCapsule addSign(Protocol.TransactionSign transactionSign)
      throws PermissionException, SignatureException, SignatureFormatException {
    byte[] privateKey = transactionSign.getPrivateKey().toByteArray();
    TransactionCapsule kht = new TransactionCapsule(transactionSign.getTransaction());
    kht.addSign(privateKey, dbManager.getAccountStore());
    return kht;
  }

  public static boolean checkPermissionOprations(Protocol.Permission permission, Protocol.Transaction.Contract contract)
      throws PermissionException {
    ByteString operations = permission.getOperations();
    if (operations.size() != 32) {
      throw new PermissionException("operations size must 32");
    }
    int contractType = contract.getTypeValue();
    boolean b = (operations.byteAt(contractType / 8) & (1 << (contractType % 8))) != 0;
    return b;
  }

  public GrpcAPI.TransactionSignWeight getTransactionSignWeight(Protocol.Transaction kht) {
    GrpcAPI.TransactionSignWeight.Builder tswBuilder = GrpcAPI.TransactionSignWeight.newBuilder();
    GrpcAPI.TransactionExtention.Builder khtExBuilder = GrpcAPI.TransactionExtention.newBuilder();
    khtExBuilder.setTransaction(kht);
    khtExBuilder.setTxid(ByteString.copyFrom(Sha256Hash.hash(kht.getRawData().toByteArray())));
    GrpcAPI.Return.Builder retBuilder = GrpcAPI.Return.newBuilder();
    retBuilder.setResult(true).setCode(GrpcAPI.Return.response_code.SUCCESS);
    khtExBuilder.setResult(retBuilder);
    tswBuilder.setTransaction(khtExBuilder);
    GrpcAPI.TransactionSignWeight.Result.Builder resultBuilder = GrpcAPI.TransactionSignWeight.Result.newBuilder();
    try {
      Protocol.Transaction.Contract contract = kht.getRawData().getContract(0);
      byte[] owner = TransactionCapsule.getOwner(contract);
      AccountCapsule account = dbManager.getAccountStore().get(owner);
      if (account == null) {
        throw new PermissionException("Account is not exist!");
      }
      int permissionId = contract.getPermissionId();
      Protocol.Permission permission = account.getPermissionById(permissionId);
      if (permission == null) {
        throw new PermissionException("permission isn't exit");
      }
      if (permissionId != 0) {
        if (permission.getType() != Protocol.Permission.PermissionType.Active) {
          throw new PermissionException("Permission type is error");
        }
        //check oprations
        if (!checkPermissionOprations(permission, contract)) {
          throw new PermissionException("Permission denied");
        }
      }
      tswBuilder.setPermission(permission);
      if (kht.getSignatureCount() > 0) {
        List<ByteString> approveList = new ArrayList<ByteString>();
        long currentWeight = TransactionCapsule.checkWeight(permission, kht.getSignatureList(),
            Sha256Hash.hash(kht.getRawData().toByteArray()), approveList);
        tswBuilder.addAllApprovedList(approveList);
        tswBuilder.setCurrentWeight(currentWeight);
      }
      if (tswBuilder.getCurrentWeight() >= permission.getThreshold()) {
        resultBuilder.setCode(GrpcAPI.TransactionSignWeight.Result.response_code.ENOUGH_PERMISSION);
      } else {
        resultBuilder.setCode(GrpcAPI.TransactionSignWeight.Result.response_code.NOT_ENOUGH_PERMISSION);
      }
    } catch (SignatureFormatException signEx) {
      resultBuilder.setCode(GrpcAPI.TransactionSignWeight.Result.response_code.SIGNATURE_FORMAT_ERROR);
      resultBuilder.setMessage(signEx.getMessage());
    } catch (SignatureException signEx) {
      resultBuilder.setCode(GrpcAPI.TransactionSignWeight.Result.response_code.COMPUTE_ADDRESS_ERROR);
      resultBuilder.setMessage(signEx.getMessage());
    } catch (PermissionException permEx) {
      resultBuilder.setCode(GrpcAPI.TransactionSignWeight.Result.response_code.PERMISSION_ERROR);
      resultBuilder.setMessage(permEx.getMessage());
    } catch (Exception ex) {
      resultBuilder.setCode(GrpcAPI.TransactionSignWeight.Result.response_code.OTHER_ERROR);
      resultBuilder.setMessage(ex.getClass() + " : " + ex.getMessage());
    }
    tswBuilder.setResult(resultBuilder);
    return tswBuilder.build();
  }

  public GrpcAPI.TransactionApprovedList getTransactionApprovedList(Protocol.Transaction kht) {
    GrpcAPI.TransactionApprovedList.Builder tswBuilder = GrpcAPI.TransactionApprovedList.newBuilder();
    GrpcAPI.TransactionExtention.Builder khtExBuilder = GrpcAPI.TransactionExtention.newBuilder();
    khtExBuilder.setTransaction(kht);
    khtExBuilder.setTxid(ByteString.copyFrom(Sha256Hash.hash(kht.getRawData().toByteArray())));
    GrpcAPI.Return.Builder retBuilder = GrpcAPI.Return.newBuilder();
    retBuilder.setResult(true).setCode(GrpcAPI.Return.response_code.SUCCESS);
    khtExBuilder.setResult(retBuilder);
    tswBuilder.setTransaction(khtExBuilder);
    GrpcAPI.TransactionApprovedList.Result.Builder resultBuilder = GrpcAPI.TransactionApprovedList.Result
        .newBuilder();
    try {
      Protocol.Transaction.Contract contract = kht.getRawData().getContract(0);
      byte[] owner = TransactionCapsule.getOwner(contract);
      AccountCapsule account = dbManager.getAccountStore().get(owner);
      if (account == null) {
        throw new PermissionException("Account is not exist!");
      }

      if (kht.getSignatureCount() > 0) {
        List<ByteString> approveList = new ArrayList<ByteString>();
        byte[] hash = Sha256Hash.hash(kht.getRawData().toByteArray());
        for (ByteString sig : kht.getSignatureList()) {
          if (sig.size() < 65) {
            throw new SignatureFormatException(
                "Signature size is " + sig.size());
          }
          String base64 = TransactionCapsule.getBase64FromByteString(sig);
          byte[] address = ECKey.signatureToAddress(hash, base64);
          approveList.add(ByteString.copyFrom(address)); //out put approve list.
        }
        tswBuilder.addAllApprovedList(approveList);
      }
      resultBuilder.setCode(GrpcAPI.TransactionApprovedList.Result.response_code.SUCCESS);
    } catch (SignatureFormatException signEx) {
      resultBuilder.setCode(GrpcAPI.TransactionApprovedList.Result.response_code.SIGNATURE_FORMAT_ERROR);
      resultBuilder.setMessage(signEx.getMessage());
    } catch (SignatureException signEx) {
      resultBuilder.setCode(GrpcAPI.TransactionApprovedList.Result.response_code.COMPUTE_ADDRESS_ERROR);
      resultBuilder.setMessage(signEx.getMessage());
    } catch (Exception ex) {
      resultBuilder.setCode(GrpcAPI.TransactionApprovedList.Result.response_code.OTHER_ERROR);
      resultBuilder.setMessage(ex.getClass() + " : " + ex.getMessage());
    }
    tswBuilder.setResult(resultBuilder);
    return tswBuilder.build();
  }

  public byte[] pass2Key(byte[] passPhrase) {
    return Sha256Hash.hash(passPhrase);
  }

  public byte[] createAdresss(byte[] passPhrase) {
    byte[] privateKey = pass2Key(passPhrase);
    ECKey ecKey = ECKey.fromPrivate(privateKey);
    return ecKey.getAddress();
  }

  public Protocol.Block getNowBlock() {
    List<BlockCapsule> blockList = dbManager.getBlockStore().getBlockByLatestNum(1);
    if (CollectionUtils.isEmpty(blockList)) {
      return null;
    } else {
      return blockList.get(0).getInstance();
    }
  }

  public Protocol.Block getBlockByNum(long blockNum) {
    try {
      return dbManager.getBlockByNum(blockNum).getInstance();
    } catch (StoreException e) {
      logger.info(e.getMessage());
      return null;
    }
  }

  public long getTransactionCountByBlockNum(long blockNum) {
    long count = 0;

    try {
      Protocol.Block block = dbManager.getBlockByNum(blockNum).getInstance();
      count = block.getTransactionsCount();
    } catch (StoreException e) {
      logger.error(e.getMessage());
    }

    return count;
  }

  public GrpcAPI.WitnessList getWitnessList() {
    GrpcAPI.WitnessList.Builder builder = GrpcAPI.WitnessList.newBuilder();
    List<WitnessCapsule> witnessCapsuleList = dbManager.getWitnessStore().getAllWitnesses();
    witnessCapsuleList
        .forEach(witnessCapsule -> builder.addWitnesses(witnessCapsule.getInstance()));
    return builder.build();
  }

  public GrpcAPI.ProposalList getProposalList() {
    GrpcAPI.ProposalList.Builder builder = GrpcAPI.ProposalList.newBuilder();
    List<ProposalCapsule> proposalCapsuleList = dbManager.getProposalStore().getAllProposals();
    proposalCapsuleList
        .forEach(proposalCapsule -> builder.addProposals(proposalCapsule.getInstance()));
    return builder.build();
  }

  public GrpcAPI.DelegatedResourceList getDelegatedResource(ByteString fromAddress, ByteString toAddress) {
    GrpcAPI.DelegatedResourceList.Builder builder = GrpcAPI.DelegatedResourceList.newBuilder();
    byte[] dbKey = DelegatedResourceCapsule
        .createDbKey(fromAddress.toByteArray(), toAddress.toByteArray());
    DelegatedResourceCapsule delegatedResourceCapsule = dbManager.getDelegatedResourceStore()
        .get(dbKey);
    if (delegatedResourceCapsule != null) {
      builder.addDelegatedResource(delegatedResourceCapsule.getInstance());
    }
    return builder.build();
  }

  public Protocol.DelegatedResourceAccountIndex getDelegatedResourceAccountIndex(ByteString address) {
    DelegatedResourceAccountIndexCapsule accountIndexCapsule =
        dbManager.getDelegatedResourceAccountIndexStore().get(address.toByteArray());
    if (accountIndexCapsule != null) {
      return accountIndexCapsule.getInstance();
    } else {
      return null;
    }
  }

  public GrpcAPI.ExchangeList getExchangeList() {
    GrpcAPI.ExchangeList.Builder builder = GrpcAPI.ExchangeList.newBuilder();
    List<ExchangeCapsule> exchangeCapsuleList = dbManager.getExchangeStoreFinal().getAllExchanges();

    exchangeCapsuleList
        .forEach(exchangeCapsule -> builder.addExchanges(exchangeCapsule.getInstance()));
    return builder.build();
  }

  public Protocol.ChainParameters getChainParameters() {
    Protocol.ChainParameters.Builder builder = Protocol.ChainParameters.newBuilder();

    // MAINTENANCE_TIME_INTERVAL, //ms  ,0
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getMaintenanceTimeInterval")
            .setValue(dbManager.getDynamicPropertiesStore().getMaintenanceTimeInterval())
            .build());
    //    ACCOUNT_UPGRADE_COST, //drop ,1
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getAccountUpgradeCost")
            .setValue(dbManager.getDynamicPropertiesStore().getAccountUpgradeCost())
            .build());
    //    CREATE_ACCOUNT_FEE, //drop ,2
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getCreateAccountFee")
            .setValue(dbManager.getDynamicPropertiesStore().getCreateAccountFee())
            .build());
    //    TRANSACTION_FEE, //drop ,3
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getTransactionFee")
            .setValue(dbManager.getDynamicPropertiesStore().getTransactionFee())
            .build());
    //    ASSET_ISSUE_FEE, //drop ,4
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getAssetIssueFee")
            .setValue(dbManager.getDynamicPropertiesStore().getAssetIssueFee())
            .build());
    //    WITNESS_PAY_PER_BLOCK, //drop ,5
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getWitnessPayPerBlock")
            .setValue(dbManager.getDynamicPropertiesStore().getWitnessPayPerBlock())
            .build());
    //    WITNESS_STANDBY_ALLOWANCE, //drop ,6
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getWitnessStandbyAllowance")
            .setValue(dbManager.getDynamicPropertiesStore().getWitnessStandbyAllowance())
            .build());
    //    CREATE_NEW_ACCOUNT_FEE_IN_SYSTEM_CONTRACT, //drop ,7
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getCreateNewAccountFeeInSystemContract")
            .setValue(
                dbManager.getDynamicPropertiesStore().getCreateNewAccountFeeInSystemContract())
            .build());
    //    CREATE_NEW_ACCOUNT_BANDWIDTH_RATE, // 1 ~ ,8
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getCreateNewAccountBandwidthRate")
            .setValue(dbManager.getDynamicPropertiesStore().getCreateNewAccountBandwidthRate())
            .build());
    //    ALLOW_CREATION_OF_CONTRACTS, // 0 / >0 ,9
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getAllowCreationOfContracts")
            .setValue(dbManager.getDynamicPropertiesStore().getAllowCreationOfContracts())
            .build());
    //    REMOVE_THE_POWER_OF_THE_GR,  // 1 ,10
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getRemoveThePowerOfTheGr")
            .setValue(dbManager.getDynamicPropertiesStore().getRemoveThePowerOfTheGr())
            .build());
    //    ENERGY_FEE, // drop, 11
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getEnergyFee")
            .setValue(dbManager.getDynamicPropertiesStore().getEnergyFee())
            .build());
    //    EXCHANGE_CREATE_FEE, // drop, 12
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getExchangeCreateFee")
            .setValue(dbManager.getDynamicPropertiesStore().getExchangeCreateFee())
            .build());
    //    MAX_CPU_TIME_OF_ONE_TX, // ms, 13
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getMaxCpuTimeOfOneTx")
            .setValue(dbManager.getDynamicPropertiesStore().getMaxCpuTimeOfOneTx())
            .build());
    //    ALLOW_UPDATE_ACCOUNT_NAME, // 1, 14
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getAllowUpdateAccountName")
            .setValue(dbManager.getDynamicPropertiesStore().getAllowUpdateAccountName())
            .build());
    //    ALLOW_SAME_TOKEN_NAME, // 1, 15
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getAllowSameTokenName")
            .setValue(dbManager.getDynamicPropertiesStore().getAllowSameTokenName())
            .build());
    //    ALLOW_DELEGATE_RESOURCE, // 0, 16
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getAllowDelegateResource")
            .setValue(dbManager.getDynamicPropertiesStore().getAllowDelegateResource())
            .build());
    //    TOTAL_ENERGY_LIMIT, // 50,000,000,000, 17
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getTotalEnergyLimit")
            .setValue(dbManager.getDynamicPropertiesStore().getTotalEnergyLimit())
            .build());
    //    ALLOW_TVM_TRANSFER_TRC10, // 1, 18
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getAllowTvmTransferTrc10")
            .setValue(dbManager.getDynamicPropertiesStore().getAllowTvmTransferTrc10())
            .build());
    //    TOTAL_CURRENT_ENERGY_LIMIT, // 50,000,000,000, 19
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getTotalEnergyCurrentLimit")
            .setValue(dbManager.getDynamicPropertiesStore().getTotalEnergyCurrentLimit())
            .build());
    //    ALLOW_MULTI_SIGN, // 1, 20
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getAllowMultiSign")
            .setValue(dbManager.getDynamicPropertiesStore().getAllowMultiSign())
            .build());
    //    ALLOW_ADAPTIVE_ENERGY, // 1, 21
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getAllowAdaptiveEnergy")
            .setValue(dbManager.getDynamicPropertiesStore().getAllowAdaptiveEnergy())
            .build());
    //other chainParameters
    builder.addChainParameter(Protocol.ChainParameters.ChainParameter.newBuilder()
        .setKey("getTotalEnergyTargetLimit")
        .setValue(dbManager.getDynamicPropertiesStore().getTotalEnergyTargetLimit())
        .build());

    builder.addChainParameter(Protocol.ChainParameters.ChainParameter.newBuilder()
        .setKey("getTotalEnergyAverageUsage")
        .setValue(dbManager.getDynamicPropertiesStore().getTotalEnergyAverageUsage())
        .build());

    builder.addChainParameter(Protocol.ChainParameters.ChainParameter.newBuilder()
        .setKey("getUpdateAccountPermissionFee")
        .setValue(dbManager.getDynamicPropertiesStore().getUpdateAccountPermissionFee())
        .build());

    builder.addChainParameter(Protocol.ChainParameters.ChainParameter.newBuilder()
        .setKey("getMultiSignFee")
        .setValue(dbManager.getDynamicPropertiesStore().getMultiSignFee())
        .build());

    builder.addChainParameter(Protocol.ChainParameters.ChainParameter.newBuilder()
        .setKey("getUpdateAccountPermissionFee")
        .setValue(dbManager.getDynamicPropertiesStore().getUpdateAccountPermissionFee())
        .build());

    builder.addChainParameter(Protocol.ChainParameters.ChainParameter.newBuilder()
        .setKey("getAllowAccountStateRoot")
        .setValue(dbManager.getDynamicPropertiesStore().getAllowAccountStateRoot())
        .build());

    builder.addChainParameter(Protocol.ChainParameters.ChainParameter.newBuilder()
        .setKey("getAllowProtoFilterNum")
        .setValue(dbManager.getDynamicPropertiesStore().getAllowProtoFilterNum())
        .build());

    // ALLOW_TVM_CONSTANTINOPLE, // 1, 30
    builder.addChainParameter(
        Protocol.ChainParameters.ChainParameter.newBuilder()
            .setKey("getAllowTvmConstantinople")
            .setValue(dbManager.getDynamicPropertiesStore().getAllowTvmConstantinople())
            .build());

    return builder.build();
  }

  public static String makeUpperCamelMethod(String originName) {
    return "get" + CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, originName)
        .replace("_", "");
  }

  public GrpcAPI.AssetIssueList getAssetIssueList() {
    GrpcAPI.AssetIssueList.Builder builder = GrpcAPI.AssetIssueList.newBuilder();

    dbManager.getAssetIssueStoreFinal().getAllAssetIssues()
        .forEach(issueCapsule -> builder.addAssetIssue(issueCapsule.getInstance()));

    return builder.build();
  }


  public GrpcAPI.AssetIssueList getAssetIssueList(long offset, long limit) {
    GrpcAPI.AssetIssueList.Builder builder = GrpcAPI.AssetIssueList.newBuilder();

    List<AssetIssueCapsule> assetIssueList =
        dbManager.getAssetIssueStoreFinal().getAssetIssuesPaginated(offset, limit);

    if (CollectionUtils.isEmpty(assetIssueList)) {
      return null;
    }

    assetIssueList.forEach(issueCapsule -> builder.addAssetIssue(issueCapsule.getInstance()));
    return builder.build();
  }

  public GrpcAPI.AssetIssueList getAssetIssueByAccount(ByteString accountAddress) {
    if (accountAddress == null || accountAddress.isEmpty()) {
      return null;
    }

    List<AssetIssueCapsule> assetIssueCapsuleList =
        dbManager.getAssetIssueStoreFinal().getAllAssetIssues();

    GrpcAPI.AssetIssueList.Builder builder = GrpcAPI.AssetIssueList.newBuilder();
    assetIssueCapsuleList.stream()
        .filter(assetIssueCapsule -> assetIssueCapsule.getOwnerAddress().equals(accountAddress))
        .forEach(issueCapsule -> {
          builder.addAssetIssue(issueCapsule.getInstance());
        });

    return builder.build();
  }

  public GrpcAPI.AccountNetMessage getAccountNet(ByteString accountAddress) {
    if (accountAddress == null || accountAddress.isEmpty()) {
      return null;
    }
    GrpcAPI.AccountNetMessage.Builder builder = GrpcAPI.AccountNetMessage.newBuilder();
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(accountAddress.toByteArray());
    if (accountCapsule == null) {
      return null;
    }

    BandwidthProcessor processor = new BandwidthProcessor(dbManager);
    processor.updateUsage(accountCapsule);

    long netLimit = processor
        .calculateGlobalNetLimit(accountCapsule);
    long freeNetLimit = dbManager.getDynamicPropertiesStore().getFreeNetLimit();
    long totalNetLimit = dbManager.getDynamicPropertiesStore().getTotalNetLimit();
    long totalNetWeight = dbManager.getDynamicPropertiesStore().getTotalNetWeight();

    Map<String, Long> assetNetLimitMap = new HashMap<>();
    Map<String, Long> allFreeAssetNetUsage;
    if (dbManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {
      allFreeAssetNetUsage = accountCapsule.getAllFreeAssetNetUsage();
      allFreeAssetNetUsage.keySet().forEach(asset -> {
        byte[] key = ByteArray.fromString(asset);
        assetNetLimitMap
            .put(asset, dbManager.getAssetIssueStore().get(key).getFreeAssetNetLimit());
      });
    } else {
      allFreeAssetNetUsage = accountCapsule.getAllFreeAssetNetUsageV2();
      allFreeAssetNetUsage.keySet().forEach(asset -> {
        byte[] key = ByteArray.fromString(asset);
        assetNetLimitMap
            .put(asset, dbManager.getAssetIssueV2Store().get(key).getFreeAssetNetLimit());
      });
    }

    builder.setFreeNetUsed(accountCapsule.getFreeNetUsage())
        .setFreeNetLimit(freeNetLimit)
        .setNetUsed(accountCapsule.getNetUsage())
        .setNetLimit(netLimit)
        .setTotalNetLimit(totalNetLimit)
        .setTotalNetWeight(totalNetWeight)
        .putAllAssetNetUsed(allFreeAssetNetUsage)
        .putAllAssetNetLimit(assetNetLimitMap);
    return builder.build();
  }

  public GrpcAPI.AccountResourceMessage getAccountResource(ByteString accountAddress) {
    if (accountAddress == null || accountAddress.isEmpty()) {
      return null;
    }
    GrpcAPI.AccountResourceMessage.Builder builder = GrpcAPI.AccountResourceMessage.newBuilder();
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(accountAddress.toByteArray());
    if (accountCapsule == null) {
      return null;
    }

    BandwidthProcessor processor = new BandwidthProcessor(dbManager);
    processor.updateUsage(accountCapsule);

    EnergyProcessor energyProcessor = new EnergyProcessor(dbManager);
    energyProcessor.updateUsage(accountCapsule);

    long netLimit = processor
        .calculateGlobalNetLimit(accountCapsule);
    long freeNetLimit = dbManager.getDynamicPropertiesStore().getFreeNetLimit();
    long totalNetLimit = dbManager.getDynamicPropertiesStore().getTotalNetLimit();
    long totalNetWeight = dbManager.getDynamicPropertiesStore().getTotalNetWeight();
    long energyLimit = energyProcessor
        .calculateGlobalEnergyLimit(accountCapsule);
    long totalEnergyLimit = dbManager.getDynamicPropertiesStore().getTotalEnergyCurrentLimit();
    long totalEnergyWeight = dbManager.getDynamicPropertiesStore().getTotalEnergyWeight();

    long storageLimit = accountCapsule.getAccountResource().getStorageLimit();
    long storageUsage = accountCapsule.getAccountResource().getStorageUsage();

    Map<String, Long> assetNetLimitMap = new HashMap<>();
    Map<String, Long> allFreeAssetNetUsage;
    if (dbManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {
      allFreeAssetNetUsage = accountCapsule.getAllFreeAssetNetUsage();
      allFreeAssetNetUsage.keySet().forEach(asset -> {
        byte[] key = ByteArray.fromString(asset);
        assetNetLimitMap
            .put(asset, dbManager.getAssetIssueStore().get(key).getFreeAssetNetLimit());
      });
    } else {
      allFreeAssetNetUsage = accountCapsule.getAllFreeAssetNetUsageV2();
      allFreeAssetNetUsage.keySet().forEach(asset -> {
        byte[] key = ByteArray.fromString(asset);
        assetNetLimitMap
            .put(asset, dbManager.getAssetIssueV2Store().get(key).getFreeAssetNetLimit());
      });
    }

    builder.setFreeNetUsed(accountCapsule.getFreeNetUsage())
        .setFreeNetLimit(freeNetLimit)
        .setNetUsed(accountCapsule.getNetUsage())
        .setNetLimit(netLimit)
        .setTotalNetLimit(totalNetLimit)
        .setTotalNetWeight(totalNetWeight)
        .setEnergyLimit(energyLimit)
        .setEnergyUsed(accountCapsule.getAccountResource().getEnergyUsage())
        .setTotalEnergyLimit(totalEnergyLimit)
        .setTotalEnergyWeight(totalEnergyWeight)
        .setStorageLimit(storageLimit)
        .setStorageUsed(storageUsage)
        .putAllAssetNetUsed(allFreeAssetNetUsage)
        .putAllAssetNetLimit(assetNetLimitMap);
    return builder.build();
  }

  public Contract.AssetIssueContract getAssetIssueByName(ByteString assetName)
      throws NonUniqueObjectException {
    if (assetName == null || assetName.isEmpty()) {
      return null;
    }

    if (dbManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {
      // fetch from old DB, same as old logic ops
      AssetIssueCapsule assetIssueCapsule =
          dbManager.getAssetIssueStore().get(assetName.toByteArray());
      return assetIssueCapsule != null ? assetIssueCapsule.getInstance() : null;
    } else {
      // get asset issue by name from new DB
      List<AssetIssueCapsule> assetIssueCapsuleList =
          dbManager.getAssetIssueV2Store().getAllAssetIssues();
      GrpcAPI.AssetIssueList.Builder builder = GrpcAPI.AssetIssueList.newBuilder();
      assetIssueCapsuleList
          .stream()
          .filter(assetIssueCapsule -> assetIssueCapsule.getName().equals(assetName))
          .forEach(
              issueCapsule -> {
                builder.addAssetIssue(issueCapsule.getInstance());
              });

      // check count
      if (builder.getAssetIssueCount() > 1) {
        throw new NonUniqueObjectException("get more than one asset, please use getassetissuebyid");
      } else {
        // fetch from DB by assetName as id
        AssetIssueCapsule assetIssueCapsule =
            dbManager.getAssetIssueV2Store().get(assetName.toByteArray());

        if (assetIssueCapsule != null) {
          // check already fetch
          if (builder.getAssetIssueCount() > 0
              && builder.getAssetIssue(0).getId().equals(assetIssueCapsule.getInstance().getId())) {
            return assetIssueCapsule.getInstance();
          }

          builder.addAssetIssue(assetIssueCapsule.getInstance());
          // check count
          if (builder.getAssetIssueCount() > 1) {
            throw new NonUniqueObjectException(
                "get more than one asset, please use getassetissuebyid");
          }
        }
      }

      if (builder.getAssetIssueCount() > 0) {
        return builder.getAssetIssue(0);
      } else {
        return null;
      }
    }
  }

  public GrpcAPI.AssetIssueList getAssetIssueListByName(ByteString assetName) {
    if (assetName == null || assetName.isEmpty()) {
      return null;
    }

    List<AssetIssueCapsule> assetIssueCapsuleList =
        dbManager.getAssetIssueStoreFinal().getAllAssetIssues();

    GrpcAPI.AssetIssueList.Builder builder = GrpcAPI.AssetIssueList.newBuilder();
    assetIssueCapsuleList.stream()
        .filter(assetIssueCapsule -> assetIssueCapsule.getName().equals(assetName))
        .forEach(issueCapsule -> {
          builder.addAssetIssue(issueCapsule.getInstance());
        });

    return builder.build();
  }

  public Contract.AssetIssueContract getAssetIssueById(String assetId) {
    if (assetId == null || assetId.isEmpty()) {
      return null;
    }
    AssetIssueCapsule assetIssueCapsule = dbManager.getAssetIssueV2Store()
        .get(ByteArray.fromString(assetId));
    return assetIssueCapsule != null ? assetIssueCapsule.getInstance() : null;
  }

  public GrpcAPI.NumberMessage totalTransaction() {
    GrpcAPI.NumberMessage.Builder builder = GrpcAPI.NumberMessage.newBuilder()
        .setNum(dbManager.getTransactionStore().getTotalTransactions());
    return builder.build();
  }

  public GrpcAPI.NumberMessage getNextMaintenanceTime() {
    GrpcAPI.NumberMessage.Builder builder = GrpcAPI.NumberMessage.newBuilder()
        .setNum(dbManager.getDynamicPropertiesStore().getNextMaintenanceTime());
    return builder.build();
  }

  public Protocol.Block getBlockById(ByteString blockId) {
    if (Objects.isNull(blockId)) {
      return null;
    }
    Protocol.Block block = null;
    try {
      block = dbManager.getBlockStore().get(blockId.toByteArray()).getInstance();
    } catch (StoreException e) {
    }
    return block;
  }

  public GrpcAPI.BlockList getBlocksByLimitNext(long number, long limit) {
    if (limit <= 0) {
      return null;
    }
    GrpcAPI.BlockList.Builder blockListBuilder = GrpcAPI.BlockList.newBuilder();
    dbManager.getBlockStore().getLimitNumber(number, limit).forEach(
        blockCapsule -> blockListBuilder.addBlock(blockCapsule.getInstance()));
    return blockListBuilder.build();
  }

  public GrpcAPI.BlockList getBlockByLatestNum(long getNum) {
    GrpcAPI.BlockList.Builder blockListBuilder = GrpcAPI.BlockList.newBuilder();
    dbManager.getBlockStore().getBlockByLatestNum(getNum).forEach(
        blockCapsule -> blockListBuilder.addBlock(blockCapsule.getInstance()));
    return blockListBuilder.build();
  }

  public Protocol.Transaction getTransactionById(ByteString transactionId) {
    if (Objects.isNull(transactionId)) {
      return null;
    }
    TransactionCapsule transactionCapsule = null;
    try {
      transactionCapsule = dbManager.getTransactionStore()
          .get(transactionId.toByteArray());
    } catch (StoreException e) {
      return null;
    }
    if (transactionCapsule != null) {
      return transactionCapsule.getInstance();
    }
    return null;
  }

  public Protocol.TransactionInfo getTransactionInfoById(ByteString transactionId) {
    if (Objects.isNull(transactionId)) {
      return null;
    }
    TransactionInfoCapsule transactionInfoCapsule;
    try {
      transactionInfoCapsule = dbManager.getTransactionHistoryStore()
          .get(transactionId.toByteArray());
    } catch (StoreException e) {
      return null;
    }
    if (transactionInfoCapsule != null) {
      return transactionInfoCapsule.getInstance();
    }
    try {
      transactionInfoCapsule = dbManager.getTransactionRetStore()
          .getTransactionInfo(transactionId.toByteArray());
    } catch (BadItemException e) {
      return null;
    }

    return transactionInfoCapsule == null ? null : transactionInfoCapsule.getInstance();
  }

  public Protocol.Proposal getProposalById(ByteString proposalId) {
    if (Objects.isNull(proposalId)) {
      return null;
    }
    ProposalCapsule proposalCapsule = null;
    try {
      proposalCapsule = dbManager.getProposalStore()
          .get(proposalId.toByteArray());
    } catch (StoreException e) {
    }
    if (proposalCapsule != null) {
      return proposalCapsule.getInstance();
    }
    return null;
  }

  public Protocol.Exchange getExchangeById(ByteString exchangeId) {
    if (Objects.isNull(exchangeId)) {
      return null;
    }
    ExchangeCapsule exchangeCapsule = null;
    try {
      exchangeCapsule = dbManager.getExchangeStoreFinal().get(exchangeId.toByteArray());
    } catch (StoreException e) {
      return null;
    }
    if (exchangeCapsule != null) {
      return exchangeCapsule.getInstance();
    }
    return null;
  }


  public GrpcAPI.NodeList listNodes() {
    List<NodeHandler> handlerList = nodeManager.dumpActiveNodes();

    Map<String, NodeHandler> nodeHandlerMap = new HashMap<>();
    for (NodeHandler handler : handlerList) {
      String key = handler.getNode().getHexId() + handler.getNode().getHost();
      nodeHandlerMap.put(key, handler);
    }

    GrpcAPI.NodeList.Builder nodeListBuilder = GrpcAPI.NodeList.newBuilder();

    nodeHandlerMap.entrySet().stream()
        .forEach(v -> {
          Node node = v.getValue().getNode();
          nodeListBuilder.addNodes(GrpcAPI.Node.newBuilder().setAddress(
              GrpcAPI.Address.newBuilder()
                  .setHost(ByteString.copyFrom(ByteArray.fromString(node.getHost())))
                  .setPort(node.getPort())));
        });
    return nodeListBuilder.build();
  }

  public Protocol.Transaction deployContract(Contract.CreateSmartContract createSmartContract,
                                             TransactionCapsule khtCap) {

    // do nothing, so can add some useful function later
    // khtcap contract para cacheUnpackValue has value
    return khtCap.getInstance();
  }

  public Protocol.Transaction triggerContract(Contract.TriggerSmartContract triggerSmartContract,
                                              TransactionCapsule khtCap, GrpcAPI.TransactionExtention.Builder builder,
                                              GrpcAPI.Return.Builder retBuilder)
      throws ContractValidateException, ContractExeException, HeaderNotFound, VMIllegalException {

    ContractStore contractStore = dbManager.getContractStore();
    byte[] contractAddress = triggerSmartContract.getContractAddress().toByteArray();
    Protocol.SmartContract.ABI abi = contractStore.getABI(contractAddress);
    if (abi == null) {
      throw new ContractValidateException("No contract or not a smart contract");
    }
    System.out.println(Hex.toHexString(triggerSmartContract.getData().toByteArray()));
    byte[] selector = getSelector(triggerSmartContract.getData().toByteArray());

    if (isConstant(abi, selector)) {
      return callConstantContract(khtCap, builder, retBuilder);
    } else {
      return khtCap.getInstance();
    }
  }

  public Protocol.Transaction triggerConstantContract(Contract.TriggerSmartContract triggerSmartContract,
                                                      TransactionCapsule khtCap, GrpcAPI.TransactionExtention.Builder builder,
                                                      GrpcAPI.Return.Builder retBuilder)
      throws ContractValidateException, ContractExeException, HeaderNotFound, VMIllegalException {

    ContractStore contractStore = dbManager.getContractStore();
    byte[] contractAddress = triggerSmartContract.getContractAddress().toByteArray();
    byte[] isContractExiste = contractStore.findContractByHash(contractAddress);

    if (ArrayUtils.isEmpty(isContractExiste)) {
      throw new ContractValidateException("No contract or not a smart contract");
    }

    if (!Args.getInstance().isSupportConstant()) {
      throw new ContractValidateException("this node don't support constant");
    }

    return callConstantContract(khtCap, builder, retBuilder);
  }

  public Protocol.Transaction callConstantContract(TransactionCapsule khtCap, GrpcAPI.TransactionExtention.Builder builder,
                                                   GrpcAPI.Return.Builder retBuilder)
      throws ContractValidateException, ContractExeException, HeaderNotFound, VMIllegalException {

    if (!Args.getInstance().isSupportConstant()) {
      throw new ContractValidateException("this node don't support constant");
    }
    DepositImpl deposit = DepositImpl.createRoot(dbManager);

    Protocol.Block headBlock;
    List<BlockCapsule> blockCapsuleList = dbManager.getBlockStore().getBlockByLatestNum(1);
    if (CollectionUtils.isEmpty(blockCapsuleList)) {
      throw new HeaderNotFound("latest block not found");
    } else {
      headBlock = blockCapsuleList.get(0).getInstance();
    }

    Runtime runtime = new RuntimeImpl(khtCap.getInstance(), new BlockCapsule(headBlock), deposit,
        new ProgramInvokeFactoryImpl(), true);
    VMConfig.initVmHardFork();
    VMConfig.initAllowTvmTransferTrc10(
        dbManager.getDynamicPropertiesStore().getAllowTvmTransferTrc10());
    VMConfig.initAllowMultiSign(dbManager.getDynamicPropertiesStore().getAllowMultiSign());
    runtime.execute();
    runtime.go();
    runtime.finalization();
    // TODO exception
    if (runtime.getResult().getException() != null) {
      RuntimeException e = runtime.getResult().getException();
      logger.warn("Constant call has error {}", e.getMessage());
      throw e;
    }

    ProgramResult result = runtime.getResult();
    TransactionResultCapsule ret = new TransactionResultCapsule();

    builder.addConstantResult(ByteString.copyFrom(result.getHReturn()));
    ret.setStatus(0, Protocol.Transaction.Result.code.SUCESS);
    if (StringUtils.isNoneEmpty(runtime.getRuntimeError())) {
      ret.setStatus(0, Protocol.Transaction.Result.code.FAILED);
      retBuilder.setMessage(ByteString.copyFromUtf8(runtime.getRuntimeError())).build();
    }
    if (runtime.getResult().isRevert()) {
      ret.setStatus(0, Protocol.Transaction.Result.code.FAILED);
      retBuilder.setMessage(ByteString.copyFromUtf8("REVERT opcode executed")).build();
    }
    khtCap.setResult(ret);
    return khtCap.getInstance();
  }

  public Protocol.SmartContract getContract(GrpcAPI.BytesMessage bytesMessage) {
    byte[] address = bytesMessage.getValue().toByteArray();
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(address);
    if (accountCapsule == null) {
      logger.error(
          "Get contract failed, the account is not exist or the account does not have code hash!");
      return null;
    }

    ContractCapsule contractCapsule = dbManager.getContractStore()
        .get(bytesMessage.getValue().toByteArray());
    if (Objects.nonNull(contractCapsule)) {
      return contractCapsule.getInstance();
    }
    return null;
  }


  private static byte[] getSelector(byte[] data) {
    if (data == null ||
        data.length < 4) {
      return null;
    }

    byte[] ret = new byte[4];
    System.arraycopy(data, 0, ret, 0, 4);
    return ret;
  }

  private static boolean isConstant(Protocol.SmartContract.ABI abi, byte[] selector) {

    if (selector == null || selector.length != 4 || abi.getEntrysList().size() == 0) {
      return false;
    }

    for (int i = 0; i < abi.getEntrysCount(); i++) {
      Protocol.SmartContract.ABI.Entry entry = abi.getEntrys(i);
      if (entry.getType() != Protocol.SmartContract.ABI.Entry.EntryType.Function) {
        continue;
      }

      int inputCount = entry.getInputsCount();
      StringBuffer sb = new StringBuffer();
      sb.append(entry.getName());
      sb.append("(");
      for (int k = 0; k < inputCount; k++) {
        Protocol.SmartContract.ABI.Entry.Param param = entry.getInputs(k);
        sb.append(param.getType());
        if (k + 1 < inputCount) {
          sb.append(",");
        }
      }
      sb.append(")");

      byte[] funcSelector = new byte[4];
      System.arraycopy(Hash.sha3(sb.toString().getBytes()), 0, funcSelector, 0, 4);
      if (Arrays.equals(funcSelector, selector)) {
        if (entry.getConstant() == true || entry.getStateMutability()
            .equals(Protocol.SmartContract.ABI.Entry.StateMutabilityType.View)) {
          return true;
        } else {
          return false;
        }
      }
    }

    return false;
  }

  /*
  input
  offset:100,limit:10
  return
  id: 101~110
   */
  public GrpcAPI.ProposalList getPaginatedProposalList(long offset, long limit) {

    if (limit < 0 || offset < 0) {
      return null;
    }

    long latestProposalNum = dbManager.getDynamicPropertiesStore().getLatestProposalNum();
    if (latestProposalNum <= offset) {
      return null;
    }
    limit = limit > Parameter.DatabaseConstants.PROPOSAL_COUNT_LIMIT_MAX ? Parameter.DatabaseConstants.PROPOSAL_COUNT_LIMIT_MAX : limit;
    long end = offset + limit;
    end = end > latestProposalNum ? latestProposalNum : end;
    GrpcAPI.ProposalList.Builder builder = GrpcAPI.ProposalList.newBuilder();

    ImmutableList<Long> rangeList = ContiguousSet
        .create(Range.openClosed(offset, end), DiscreteDomain.longs()).asList();
    rangeList.stream().map(ProposalCapsule::calculateDbKey).map(key -> {
      try {
        return dbManager.getProposalStore().get(key);
      } catch (Exception ex) {
        return null;
      }
    }).filter(Objects::nonNull)
        .forEach(proposalCapsule -> builder.addProposals(proposalCapsule.getInstance()));
    return builder.build();
  }

  public GrpcAPI.ExchangeList getPaginatedExchangeList(long offset, long limit) {

    if (limit < 0 || offset < 0) {
      return null;
    }

    long latestExchangeNum = dbManager.getDynamicPropertiesStore().getLatestExchangeNum();
    if (latestExchangeNum <= offset) {
      return null;
    }
    limit = limit > Parameter.DatabaseConstants.EXCHANGE_COUNT_LIMIT_MAX ? Parameter.DatabaseConstants.EXCHANGE_COUNT_LIMIT_MAX : limit;
    long end = offset + limit;
    end = end > latestExchangeNum ? latestExchangeNum : end;

    GrpcAPI.ExchangeList.Builder builder = GrpcAPI.ExchangeList.newBuilder();
    ImmutableList<Long> rangeList = ContiguousSet
        .create(Range.openClosed(offset, end), DiscreteDomain.longs()).asList();
    rangeList.stream().map(ExchangeCapsule::calculateDbKey).map(key -> {
      try {
        return dbManager.getExchangeStoreFinal().get(key);
      } catch (Exception ex) {
        return null;
      }
    }).filter(Objects::nonNull)
        .forEach(exchangeCapsule -> builder.addExchanges(exchangeCapsule.getInstance()));
    return builder.build();

  }
}
