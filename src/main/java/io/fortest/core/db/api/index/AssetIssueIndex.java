package io.fortest.core.db.api.index;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.disk.DiskIndex;
import com.googlecode.cqengine.persistence.disk.DiskPersistence;
import io.fortest.common.utils.ByteArray;
import io.fortest.core.capsule.AssetIssueCapsule;
import io.fortest.protos.Contract;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import io.fortest.core.db.common.WrappedByteArray;
import io.fortest.core.db2.core.IkhcChainBase;

import javax.annotation.PostConstruct;

import static com.googlecode.cqengine.query.QueryFactory.attribute;

@Component
@Slf4j(topic = "DB")
public class AssetIssueIndex extends AbstractIndex<AssetIssueCapsule, Contract.AssetIssueContract> {

  public static Attribute<WrappedByteArray, String> AssetIssue_OWNER_ADDRESS;
  public static SimpleAttribute<WrappedByteArray, String> AssetIssue_NAME;
  public static Attribute<WrappedByteArray, Long> AssetIssue_START;
  public static Attribute<WrappedByteArray, Long> AssetIssue_END;

  @Autowired
  public AssetIssueIndex(
      @Qualifier("assetIssueStore") final IkhcChainBase<AssetIssueCapsule> database) {
    super(database);
  }

  @PostConstruct
  public void init() {
    initIndex(DiskPersistence.onPrimaryKeyInFile(AssetIssue_NAME, indexPath));
    index.addIndex(DiskIndex.onAttribute(AssetIssue_OWNER_ADDRESS));
//    index.addIndex(DiskIndex.onAttribute(AssetIssue_NAME));
    index.addIndex(DiskIndex.onAttribute(AssetIssue_START));
    index.addIndex(DiskIndex.onAttribute(AssetIssue_END));
  }

  @Override
  protected void setAttribute() {
    AssetIssue_OWNER_ADDRESS =
        attribute(
            "assetIssue owner address",
            bytes -> {
              Contract.AssetIssueContract assetIssue = getObject(bytes);
              return ByteArray.toHexString(assetIssue.getOwnerAddress().toByteArray());
            });

    AssetIssue_NAME =
        attribute("assetIssue name", bytes -> {
          Contract.AssetIssueContract assetIssue = getObject(bytes);
          return assetIssue.getName().toStringUtf8();
        });

    AssetIssue_START =
        attribute("assetIssue start time", bytes -> {
          Contract.AssetIssueContract assetIssue = getObject(bytes);
          return assetIssue.getStartTime();
        });

    AssetIssue_END =
        attribute("assetIssue end time", bytes -> {
          Contract.AssetIssueContract assetIssue = getObject(bytes);
          return assetIssue.getEndTime();
        });

  }
}
