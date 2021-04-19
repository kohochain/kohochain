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

import io.fortest.common.runtime.vm.DataWord;
import io.fortest.common.runtime.vm.program.invoke.ProgramInvoke;
import io.fortest.common.runtime.vm.program.listener.ProgramListener;
import io.fortest.common.runtime.vm.program.listener.ProgramListenerAware;
import io.fortest.common.storage.Deposit;
import io.fortest.common.storage.Key;
import io.fortest.common.storage.Value;
import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.AssetIssueCapsule;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.BytesCapsule;
import io.fortest.core.capsule.ContractCapsule;
import io.fortest.core.capsule.ProposalCapsule;
import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.core.capsule.VotesCapsule;
import io.fortest.core.capsule.WitnessCapsule;
import io.fortest.core.db.Manager;
import io.fortest.protos.Protocol;
import io.fortest.protos.Protocol.AccountType;

public class ContractState implements Deposit, ProgramListenerAware {

  private Deposit deposit;
  // contract address
  private final DataWord address;
  private ProgramListener programListener;

  ContractState(ProgramInvoke programInvoke) {
    this.address = programInvoke.getContractAddress();
    this.deposit = programInvoke.getDeposit();
  }

  @Override
  public Manager getDbManager() {
    return deposit.getDbManager();
  }

  @Override
  public void setProgramListener(ProgramListener listener) {
    this.programListener = listener;
  }

  @Override
  public AccountCapsule createAccount(byte[] addr, Protocol.AccountType type) {
    return deposit.createAccount(addr, type);
  }

  @Override
  public AccountCapsule createAccount(byte[] address, String accountName, AccountType type) {
    return deposit.createAccount(address, accountName, type);
  }


  @Override
  public AccountCapsule getAccount(byte[] addr) {
    return deposit.getAccount(addr);
  }

  @Override
  public WitnessCapsule getWitness(byte[] address) {
    return deposit.getWitness(address);
  }

  @Override
  public VotesCapsule getVotesCapsule(byte[] address) {
    return deposit.getVotesCapsule(address);
  }

  @Override
  public ProposalCapsule getProposalCapsule(byte[] id) {
    return deposit.getProposalCapsule(id);
  }

  @Override
  public BytesCapsule getDynamic(byte[] bytesKey) {
    return deposit.getDynamic(bytesKey);
  }

  @Override
  public void deleteContract(byte[] address) {
    deposit.deleteContract(address);
  }

  @Override
  public void createContract(byte[] codeHash, ContractCapsule contractCapsule) {
    deposit.createContract(codeHash, contractCapsule);
  }

  @Override
  public ContractCapsule getContract(byte[] codeHash) {
    return deposit.getContract(codeHash);
  }

  @Override
  public void updateContract(byte[] address, ContractCapsule contractCapsule) {
    deposit.updateContract(address, contractCapsule);
  }

  @Override
  public void updateAccount(byte[] address, AccountCapsule accountCapsule) {
    deposit.updateAccount(address, accountCapsule);
  }

  @Override
  public void saveCode(byte[] address, byte[] code) {
    deposit.saveCode(address, code);
  }

  @Override
  public byte[] getCode(byte[] address) {
    return deposit.getCode(address);
  }

  @Override
  public void putStorageValue(byte[] addr, DataWord key, DataWord value) {
    if (canListenTrace(addr)) {
      programListener.onStoragePut(key, value);
    }
    deposit.putStorageValue(addr, key, value);
  }

  private boolean canListenTrace(byte[] address) {
    return (programListener != null) && this.address.equals(new DataWord(address));
  }

  @Override
  public DataWord getStorageValue(byte[] addr, DataWord key) {
    return deposit.getStorageValue(addr, key);
  }

  @Override
  public long getBalance(byte[] addr) {
    return deposit.getBalance(addr);
  }

  @Override
  public long addBalance(byte[] addr, long value) {
    return deposit.addBalance(addr, value);
  }

  @Override
  public Deposit newDepositChild() {
    return deposit.newDepositChild();
  }

  @Override
  public void commit() {
    deposit.commit();
  }

  @Override
  public Storage getStorage(byte[] address) {
    return deposit.getStorage(address);
  }

  @Override
  public void putAccount(Key key, Value value) {
    deposit.putAccount(key, value);
  }

  @Override
  public void putTransaction(Key key, Value value) {
    deposit.putTransaction(key, value);
  }

  @Override
  public void putBlock(Key key, Value value) {
    deposit.putBlock(key, value);
  }

  @Override
  public void putWitness(Key key, Value value) {
    deposit.putWitness(key, value);
  }

  @Override
  public void putCode(Key key, Value value) {
    deposit.putCode(key, value);
  }

  @Override
  public void putContract(Key key, Value value) {
    deposit.putContract(key, value);
  }

  @Override
  public void putStorage(Key key, Storage cache) {
    deposit.putStorage(key, cache);
  }

  @Override
  public void putVotes(Key key, Value value) {
    deposit.putVotes(key, value);
  }

  @Override
  public void putProposal(Key key, Value value) {
    deposit.putProposal(key, value);
  }

  @Override
  public void putDynamicProperties(Key key, Value value) {
    deposit.putDynamicProperties(key, value);
  }

  @Override
  public void setParent(Deposit deposit) {
    this.deposit.setParent(deposit);
  }

  @Override
  public TransactionCapsule getTransaction(byte[] khtHash) {
    return this.deposit.getTransaction(khtHash);
  }

  @Override
  public void putAccountValue(byte[] address, AccountCapsule accountCapsule) {
    this.deposit.putAccountValue(address, accountCapsule);
  }

  @Override
  public void putVoteValue(byte[] address, VotesCapsule votesCapsule) {
    this.deposit.putVoteValue(address, votesCapsule);
  }

  @Override
  public void putProposalValue(byte[] address, ProposalCapsule proposalCapsule) {
    deposit.putProposalValue(address, proposalCapsule);
  }

  @Override
  public void putDynamicPropertiesWithLatestProposalNum(long num) {
    deposit.putDynamicPropertiesWithLatestProposalNum(num);
  }

  @Override
  public long getLatestProposalNum() {
    return deposit.getLatestProposalNum();
  }

  @Override
  public long getWitnessAllowanceFrozenTime() {
    return deposit.getWitnessAllowanceFrozenTime();
  }

  @Override
  public long getMaintenanceTimeInterval() {
    return deposit.getMaintenanceTimeInterval();
  }

  @Override
  public long getNextMaintenanceTime() {
    return deposit.getNextMaintenanceTime();
  }

  @Override
  public long addTokenBalance(byte[] address, byte[] tokenId, long value) {
    return deposit.addTokenBalance(address, tokenId, value);
  }

  @Override
  public long getTokenBalance(byte[] address, byte[] tokenId) {
    return deposit.getTokenBalance(address, tokenId);
  }

  @Override
  public AssetIssueCapsule getAssetIssue(byte[] tokenId) {
    return deposit.getAssetIssue(tokenId);
  }

  @Override
  public BlockCapsule getBlock(byte[] blockHash) {
    return this.deposit.getBlock(blockHash);
  }

  @Override
  public byte[] getBlackHoleAddress() {
    return deposit.getBlackHoleAddress();
  }

  @Override
  public AccountCapsule createNormalAccount(byte[] address) {
    return deposit.createNormalAccount(address);
  }

}
