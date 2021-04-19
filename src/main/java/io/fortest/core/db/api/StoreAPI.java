package io.fortest.core.db.api;

import static com.googlecode.cqengine.query.QueryFactory.ascending;
import static com.googlecode.cqengine.query.QueryFactory.equal;
import static com.googlecode.cqengine.query.QueryFactory.orderBy;
import static com.googlecode.cqengine.query.QueryFactory.queryOptions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.googlecode.cqengine.resultset.ResultSet;
import java.util.List;

import io.fortest.core.config.Parameter;
import io.fortest.core.exception.NonUniqueObjectException;
import io.fortest.protos.Protocol;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import io.fortest.core.db.api.index.Index;
import io.fortest.core.db.api.index.TransactionIndex;

@Component
@Slf4j(topic = "DB")
public class StoreAPI {

  @Autowired(required = false)
  private IndexHelper indexHelper;

  /********************************************************************************
   *                            account api                                       *
   ********************************************************************************
   */

//  public Account getAccountByAddress(String address) throws NonUniqueObjectException {
//    if (StringUtils.isEmpty(address)) {
//      logger.info("address is empty");
//      return null;
//    }
//    Index.Iface<Account> index = indexHelper.getAccountIndex();
//    try (ResultSet<Account> resultSet = index
//        .retrieve(equal(AccountIndex.Account_ADDRESS, address))) {
//      if (resultSet.isEmpty()) {
//        return null;
//      }
//
//      return resultSet.uniqueResult();
//    } catch (com.googlecode.cqengine.resultset.common.NonUniqueObjectException e) {
//      throw new NonUniqueObjectException(e);
//    }
//  }

  /********************************************************************************
   *                          block api                                           *
   ********************************************************************************
   */

//  public Block getBlockByNumber(long number) throws NonUniqueObjectException {
//    Index.Iface<Block> index = indexHelper.getBlockIndex();
//    try (ResultSet<Block> resultSet = index.retrieve(equal(BlockIndex.Block_NUMBER, number))) {
//      if (resultSet.isEmpty()) {
//        return null;
//      }
//
//      return resultSet.uniqueResult();
//    } catch (com.googlecode.cqengine.resultset.common.NonUniqueObjectException e) {
//      throw new NonUniqueObjectException(e);
//    }
//  }

  /*******************************************************************************
   *                       transaction api                                       *
   *******************************************************************************
   */
  public Protocol.Transaction getTransactionById(String id) throws NonUniqueObjectException {
    if (StringUtils.isEmpty(id)) {
      logger.info("id is empty");
      return null;
    }
    Index.Iface<Protocol.Transaction> index = indexHelper.getTransactionIndex();
    try (ResultSet<Protocol.Transaction> resultSet = index
        .retrieve(equal(TransactionIndex.Transaction_ID, id))) {
      if (resultSet.isEmpty()) {
        return null;
      }

      return resultSet.uniqueResult();
    } catch (com.googlecode.cqengine.resultset.common.NonUniqueObjectException e) {
      throw new NonUniqueObjectException(e);
    }
  }

  public List<Protocol.Transaction> getTransactionsFromThis(String address, long offset, long limit) {
    if (StringUtils.isEmpty(address)) {
      logger.info("address is empty");
      return Lists.newArrayList();
    }
    Index.Iface<Protocol.Transaction> index = indexHelper.getTransactionIndex();
    try (ResultSet<Protocol.Transaction> resultSet =
        index.retrieve(
            equal(TransactionIndex.OWNERS, address),
            queryOptions(
                orderBy(ascending(TransactionIndex.TIMESTAMP))))) {
      if (limit > Parameter.DatabaseConstants.TRANSACTIONS_COUNT_LIMIT_MAX) {
        limit = Parameter.DatabaseConstants.TRANSACTIONS_COUNT_LIMIT_MAX;
      }
      return ImmutableList.copyOf(Streams.stream(resultSet).skip(offset).limit(limit).iterator());
    }
  }

  public List<Protocol.Transaction> getTransactionsToThis(String address, long offset, long limit) {
    if (StringUtils.isEmpty(address)) {
      logger.info("address is empty");
      return Lists.newArrayList();
    }
    Index.Iface<Protocol.Transaction> index = indexHelper.getTransactionIndex();
    try (ResultSet<Protocol.Transaction> resultSet =
        index.retrieve(
            equal(TransactionIndex.TOS, address),
            queryOptions(
                orderBy(ascending(TransactionIndex.TIMESTAMP))))) {
      if (limit > Parameter.DatabaseConstants.TRANSACTIONS_COUNT_LIMIT_MAX) {
        limit = Parameter.DatabaseConstants.TRANSACTIONS_COUNT_LIMIT_MAX;
      }
      return ImmutableList.copyOf(Streams.stream(resultSet).skip(offset).limit(limit).iterator());
    }
  }

  /*******************************************************************************
   *                            witness api                                      *
   *******************************************************************************
   */
//  public List<Witness> getWitnessAll() {
//    Index.Iface<Witness> index = indexHelper.getWitnessIndex();
//    return ImmutableList.copyOf(index);
//  }

  /********************************************************************************
   *                         AssetIssue api                                       *
   ********************************************************************************
   */
//  public List<AssetIssueContract> getAssetIssueAll() {
//    Index.Iface<AssetIssueContract> index = indexHelper.getAssetIssueIndex();
//    return ImmutableList.copyOf(index);
//  }
}
