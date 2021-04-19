package io.fortest.core.db;

import java.util.ArrayList;
import java.util.List;

import io.fortest.core.capsule.TransactionCapsule;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import io.fortest.core.db.TransactionTrace.TimeResultType;

@Slf4j(topic = "DB")
public class PendingManager implements AutoCloseable {

  @Getter
  private List<TransactionCapsule> tmpTransactions = new ArrayList<>();
  private Manager dbManager;

  public PendingManager(Manager db) {

    this.dbManager = db;
    tmpTransactions.addAll(db.getPendingTransactions());
    db.getPendingTransactions().clear();
    db.getSession().reset();
  }

  @Override
  public void close() {

    for (TransactionCapsule tx : tmpTransactions) {
      try {
        if (tx.getkhtTrace() != null &&
            tx.getkhtTrace().getTimeResultType().equals(TimeResultType.NORMAL)) {
          dbManager.getRepushTransactions().put(tx);
        }
      } catch (InterruptedException e) {
        logger.error(e.getMessage());
        Thread.currentThread().interrupt();
      }
    }
    tmpTransactions.clear();

    for (TransactionCapsule tx : dbManager.getPoppedTransactions()) {
      try {
        if (tx.getkhtTrace() != null &&
            tx.getkhtTrace().getTimeResultType().equals(TimeResultType.NORMAL)) {
          dbManager.getRepushTransactions().put(tx);
        }
      } catch (InterruptedException e) {
        logger.error(e.getMessage());
        Thread.currentThread().interrupt();
      }
    }
    dbManager.getPoppedTransactions().clear();
  }
}
