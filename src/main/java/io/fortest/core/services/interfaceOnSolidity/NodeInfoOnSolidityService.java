package io.fortest.core.services.interfaceOnSolidity;

import io.fortest.core.services.NodeInfoService;
import org.springframework.stereotype.Component;
import io.fortest.common.entity.NodeInfo;

@Component
public class NodeInfoOnSolidityService extends NodeInfoService {

  @Override
  protected void setBlockInfo(NodeInfo nodeInfo) {
    super.setBlockInfo(nodeInfo);
    nodeInfo.setBlock(nodeInfo.getSolidityBlock());
    nodeInfo.setBeginSyncNum(-1);
  }

  @Override
  protected void setCheatWitnessInfo(NodeInfo nodeInfo) {
  }

}
