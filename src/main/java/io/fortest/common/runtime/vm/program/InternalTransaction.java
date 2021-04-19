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
package io.fortest.common.runtime.vm.program;

import static org.apache.commons.lang3.ArrayUtils.isEmpty;

import com.google.common.primitives.Longs;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.fortest.common.crypto.Hash;
import io.fortest.common.utils.ByteUtil;
import io.fortest.core.capsule.ContractCapsule;
import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.core.exception.ContractValidateException;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
import io.fortest.core.Wallet;
import io.fortest.protos.Contract.CreateSmartContract;
import io.fortest.protos.Contract.TriggerSmartContract;
import io.fortest.protos.Protocol.Transaction;

public class InternalTransaction {

  private Transaction transaction;
  private byte[] hash;
  private byte[] parentHash;
  /* the amount of kht to transfer (calculated as sun) */
  private long value;

  private Map<String, Long> tokenInfo = new HashMap<>();

  /* the address of the destination account (for message)
   * In creation transaction the receive address is - 0 */
  private byte[] receiveAddress;

  /* An unlimited size byte array specifying
   * input [data] of the message call or
   * Initialization code for a new contract */
  private byte[] data;
  private long nonce;
  private byte[] transferToAddress;

  /*  Message sender address */
  private byte[] sendAddress;
  @Getter
  private int deep;
  @Getter
  private int index;
  private boolean rejected;
  private String note;
  private byte[] protoEncoded;


  public enum khtType {
    kht_PRECOMPILED_TYPE,
    kht_CONTRACT_CREATION_TYPE,
    kht_CONTRACT_CALL_TYPE,
    kht_UNKNOWN_TYPE,
  }

  public enum ExecutorType {
    ET_PRE_TYPE,
    ET_NORMAL_TYPE,
    ET_CONSTANT_TYPE,
    ET_UNKNOWN_TYPE,
  }


  /**
   * Construct a root InternalTransaction
   */
  public InternalTransaction(Transaction kht, InternalTransaction.khtType khtType)
      throws ContractValidateException {
    this.transaction = kht;
    TransactionCapsule khtCap = new TransactionCapsule(kht);
    this.protoEncoded = khtCap.getData();
    this.nonce = 0;
    // outside transaction should not have deep, so use -1 to mark it is root.
    // It will not count in vm trace. But this deep will be shown in program result.
    this.deep = -1;
    if (khtType == khtType.kht_CONTRACT_CREATION_TYPE) {
      CreateSmartContract contract = ContractCapsule.getSmartContractFromTransaction(kht);
      if (contract == null) {
        throw new ContractValidateException("Invalid CreateSmartContract Protocol");
      }
      this.sendAddress = contract.getOwnerAddress().toByteArray();
      this.receiveAddress = ByteUtil.EMPTY_BYTE_ARRAY;
      this.transferToAddress = Wallet.generateContractAddress(kht);
      this.note = "create";
      this.value = contract.getNewContract().getCallValue();
      this.data = contract.getNewContract().getBytecode().toByteArray();
      this.tokenInfo.put(String.valueOf(contract.getTokenId()), contract.getCallTokenValue());
    } else if (khtType == khtType.kht_CONTRACT_CALL_TYPE) {
      TriggerSmartContract contract = ContractCapsule.getTriggerContractFromTransaction(kht);
      if (contract == null) {
        throw new ContractValidateException("Invalid TriggerSmartContract Protocol");
      }
      this.sendAddress = contract.getOwnerAddress().toByteArray();
      this.receiveAddress = contract.getContractAddress().toByteArray();
      this.transferToAddress = this.receiveAddress.clone();
      this.note = "call";
      this.value = contract.getCallValue();
      this.data = contract.getData().toByteArray();
      this.tokenInfo.put(String.valueOf(contract.getTokenId()), contract.getCallTokenValue());
    } else {
      // do nothing, just for running byte code
    }
    this.hash = khtCap.getTransactionId().getBytes();
  }

  /**
   * Construct a child InternalTransaction
   */

  public InternalTransaction(byte[] parentHash, int deep, int index,
      byte[] sendAddress, byte[] transferToAddress, long value, byte[] data, String note,
      long nonce, Map<String, Long> tokenInfo) {
    this.parentHash = parentHash.clone();
    this.deep = deep;
    this.index = index;
    this.note = note;
    this.sendAddress = ArrayUtils.nullToEmpty(sendAddress);
    this.transferToAddress = ArrayUtils.nullToEmpty(transferToAddress);
    if ("create".equalsIgnoreCase(note)) {
      this.receiveAddress = ByteUtil.EMPTY_BYTE_ARRAY;
    } else {
      this.receiveAddress = ArrayUtils.nullToEmpty(transferToAddress);
    }
    // in this case, value also can represent a tokenValue when tokenId is not null, otherwise it is a kht callvalue.
    this.value = value;
    this.data = ArrayUtils.nullToEmpty(data);
    this.nonce = nonce;
    this.hash = getHash();
    // in a contract call contract case, only one value should be used. kht or a token. can't be both. We should avoid using
    // tokenValue in this case.
    if (tokenInfo != null) {
      this.tokenInfo.putAll(tokenInfo);
    }
  }

  public Transaction getTransaction() {
    return transaction;
  }

  public void setTransaction(Transaction transaction) {
    this.transaction = transaction;
  }

  public byte[] getTransferToAddress() {
    return transferToAddress.clone();
  }

  public void reject() {
    this.rejected = true;
  }

  public boolean isRejected() {
    return rejected;
  }

  public String getNote() {
    if (note == null) {
      return "";
    }
    return note;
  }

  public Map<String, Long> getTokenInfo() {
    return tokenInfo;
  }

  public byte[] getSender() {
    if (sendAddress == null) {
      return ByteUtil.EMPTY_BYTE_ARRAY;
    }
    return sendAddress.clone();
  }

  public byte[] getReceiveAddress() {
    if (receiveAddress == null) {
      return ByteUtil.EMPTY_BYTE_ARRAY;
    }
    return receiveAddress.clone();
  }

  public byte[] getParentHash() {
    if (parentHash == null) {
      return ByteUtil.EMPTY_BYTE_ARRAY;
    }
    return parentHash.clone();
  }

  public long getValue() {
    return value;
  }

  public byte[] getData() {
    if (data == null) {
      return ByteUtil.EMPTY_BYTE_ARRAY;
    }
    return data.clone();
  }


  public final byte[] getHash() {
    if (!isEmpty(hash)) {
      return Arrays.copyOf(hash, hash.length);
    }

    byte[] plainMsg = this.getEncoded();
    byte[] nonceByte;
    nonceByte = Longs.toByteArray(nonce);
    byte[] forHash = new byte[plainMsg.length + nonceByte.length];
    System.arraycopy(plainMsg, 0, forHash, 0, plainMsg.length);
    System.arraycopy(nonceByte, 0, forHash, plainMsg.length, nonceByte.length);
    this.hash = Hash.sha3(forHash);
    return Arrays.copyOf(hash, hash.length);
  }

  public long getNonce() {
    return nonce;
  }

  public byte[] getEncoded() {
    if (protoEncoded != null) {
      return protoEncoded.clone();
    }
    byte[] parentHashArray = parentHash.clone();

    if (parentHashArray == null) {
      parentHashArray = ByteUtil.EMPTY_BYTE_ARRAY;
    }
    byte[] valueByte = Longs.toByteArray(this.value);
    byte[] raw = new byte[parentHashArray.length + this.receiveAddress.length + this.data.length
        + valueByte.length];
    System.arraycopy(parentHashArray, 0, raw, 0, parentHashArray.length);
    System
        .arraycopy(this.receiveAddress, 0, raw, parentHashArray.length, this.receiveAddress.length);
    System.arraycopy(this.data, 0, raw, parentHashArray.length + this.receiveAddress.length,
        this.data.length);
    System.arraycopy(valueByte, 0, raw,
        parentHashArray.length + this.receiveAddress.length + this.data.length,
        valueByte.length);
    this.protoEncoded = raw;
    return protoEncoded.clone();
  }

}
