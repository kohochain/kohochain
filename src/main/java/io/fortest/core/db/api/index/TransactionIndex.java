package io.fortest.core.db.api.index;

import static com.googlecode.cqengine.query.QueryFactory.attribute;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.disk.DiskIndex;
import com.googlecode.cqengine.persistence.disk.DiskPersistence;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;

import io.fortest.common.utils.ByteArray;
import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.core.db.common.WrappedByteArray;
import io.fortest.protos.Protocol;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import io.fortest.core.db2.core.IkhcChainBase;

@Component
@Slf4j(topic = "DB")
public class TransactionIndex extends AbstractIndex<TransactionCapsule, Protocol.Transaction> {

  public static SimpleAttribute<WrappedByteArray, String> Transaction_ID;
  public static Attribute<WrappedByteArray, String> OWNERS;
  public static Attribute<WrappedByteArray, String> TOS;
  public static Attribute<WrappedByteArray, Long> TIMESTAMP;

  @Autowired
  public TransactionIndex(
      @Qualifier("transactionStore") final IkhcChainBase<TransactionCapsule> database) {
    super(database);
  }

  @PostConstruct
  public void init() {
    initIndex(DiskPersistence.onPrimaryKeyInFile(Transaction_ID, indexPath));
//    index.addIndex(DiskIndex.onAttribute(Transaction_ID));
    index.addIndex(DiskIndex.onAttribute(OWNERS));
    index.addIndex(DiskIndex.onAttribute(TOS));
    index.addIndex(DiskIndex.onAttribute(TIMESTAMP));
  }

  @Override
  protected void setAttribute() {
    Transaction_ID =
        attribute("transaction id",
            bytes -> new TransactionCapsule(getObject(bytes)).getTransactionId().toString());
    OWNERS =
        attribute(String.class, "owner address",
            bytes -> getObject(bytes).getRawData().getContractList().stream()
                .map(TransactionCapsule::getOwner)
                .filter(Objects::nonNull)
                .map(ByteArray::toHexString)
                .collect(Collectors.toList()));
    TOS =
        attribute(String.class, "to address",
            bytes -> getObject(bytes).getRawData().getContractList().stream()
                .map(TransactionCapsule::getToAddress)
                .filter(Objects::nonNull)
                .map(ByteArray::toHexString)
                .collect(Collectors.toList()));
    TIMESTAMP =
        attribute("timestamp", bytes -> getObject(bytes).getRawData().getTimestamp());
  }
}
