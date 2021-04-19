package io.fortest.core.db.api.index;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.disk.DiskIndex;
import com.googlecode.cqengine.persistence.disk.DiskPersistence;
import io.fortest.common.utils.ByteArray;
import io.fortest.common.utils.Sha256Hash;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.core.db.common.WrappedByteArray;
import io.fortest.protos.Protocol;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import io.fortest.core.db2.core.IkhcChainBase;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.googlecode.cqengine.query.QueryFactory.attribute;

@Component
@Slf4j(topic = "DB")
public class BlockIndex extends AbstractIndex<BlockCapsule, Protocol.Block> {

  public static SimpleAttribute<WrappedByteArray, String> Block_ID;
  public static Attribute<WrappedByteArray, Long> Block_NUMBER;
  public static Attribute<WrappedByteArray, String> TRANSACTIONS;
  public static Attribute<WrappedByteArray, Long> WITNESS_ID;
  public static Attribute<WrappedByteArray, String> WITNESS_ADDRESS;
  public static Attribute<WrappedByteArray, String> OWNERS;
  public static Attribute<WrappedByteArray, String> TOS;

  @Autowired
  public BlockIndex(
      @Qualifier("blockStore") final IkhcChainBase<BlockCapsule> database) {
    super(database);
  }

  @PostConstruct
  public void init() {
    initIndex(DiskPersistence.onPrimaryKeyInFile(Block_ID, indexPath));
//    index.addIndex(DiskIndex.onAttribute(Block_ID));
    index.addIndex(DiskIndex.onAttribute(Block_NUMBER));
    index.addIndex(DiskIndex.onAttribute(TRANSACTIONS));
    index.addIndex(DiskIndex.onAttribute(WITNESS_ID));
    index.addIndex(DiskIndex.onAttribute(WITNESS_ADDRESS));
    index.addIndex(DiskIndex.onAttribute(OWNERS));
    index.addIndex(DiskIndex.onAttribute(TOS));
  }

  @Override
  protected void setAttribute() {
    Block_ID =
        attribute("block id",
            bytes -> {
              Protocol.Block block = getObject(bytes);
              return new BlockCapsule(block).getBlockId().toString();
            });
    Block_NUMBER =
        attribute("block number",
            bytes -> {
              Protocol.Block block = getObject(bytes);
              return block.getBlockHeader().getRawData().getNumber();
            });
    TRANSACTIONS =
        attribute(String.class, "transactions",
            bytes -> {
              Protocol.Block block = getObject(bytes);
              return block.getTransactionsList().stream()
                  .map(t -> Sha256Hash.of(t.getRawData().toByteArray()).toString())
                  .collect(Collectors.toList());
            });
    WITNESS_ID =
        attribute("witness id",
            bytes -> {
              Protocol.Block block = getObject(bytes);
              return block.getBlockHeader().getRawData().getWitnessId();
            });
    WITNESS_ADDRESS =
        attribute("witness address",
            bytes -> {
              Protocol.Block block = getObject(bytes);
              return ByteArray.toHexString(
                  block.getBlockHeader().getRawData().getWitnessAddress().toByteArray());
            });

    OWNERS =
        attribute(String.class, "owner address",
            bytes -> {
              Protocol.Block block = getObject(bytes);
              return block.getTransactionsList().stream()
                  .map(transaction -> transaction.getRawData().getContractList())
                  .flatMap(List::stream)
                  .map(TransactionCapsule::getOwner)
                  .filter(Objects::nonNull)
                  .distinct()
                  .map(ByteArray::toHexString)
                  .collect(Collectors.toList());
            });
    TOS =
        attribute(String.class, "to address",
            bytes -> {
              Protocol.Block block = getObject(bytes);
              return block.getTransactionsList().stream()
                  .map(transaction -> transaction.getRawData().getContractList())
                  .flatMap(List::stream)
                  .map(TransactionCapsule::getToAddress)
                  .filter(Objects::nonNull)
                  .distinct()
                  .map(ByteArray::toHexString)
                  .collect(Collectors.toList());
            });
  }
}
