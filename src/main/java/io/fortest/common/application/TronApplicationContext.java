package io.fortest.common.application;

import io.fortest.common.overlay.discover.DiscoverServer;
import io.fortest.common.overlay.discover.node.NodeManager;
import io.fortest.common.overlay.server.ChannelManager;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import io.fortest.core.db.Manager;

public class khcApplicationContext extends AnnotationConfigApplicationContext {

  public khcApplicationContext() {
  }

  public khcApplicationContext(DefaultListableBeanFactory beanFactory) {
    super(beanFactory);
  }

  public khcApplicationContext(Class<?>... annotatedClasses) {
    super(annotatedClasses);
  }

  public khcApplicationContext(String... basePackages) {
    super(basePackages);
  }

  @Override
  public void destroy() {

    Application appT = ApplicationFactory.create(this);
    appT.shutdownServices();
    appT.shutdown();

    DiscoverServer discoverServer = getBean(DiscoverServer.class);
    discoverServer.close();
    ChannelManager channelManager = getBean(ChannelManager.class);
    channelManager.close();
    NodeManager nodeManager = getBean(NodeManager.class);
    nodeManager.close();

    Manager dbManager = getBean(Manager.class);
    dbManager.stopRepushThread();
    dbManager.stopRepushTriggerThread();
    super.destroy();
  }
}
