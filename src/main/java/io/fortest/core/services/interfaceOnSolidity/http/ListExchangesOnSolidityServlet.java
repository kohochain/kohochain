package io.fortest.core.services.interfaceOnSolidity.http;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.fortest.core.services.http.ListExchangesServlet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.fortest.core.services.interfaceOnSolidity.WalletOnSolidity;


@Component
@Slf4j(topic = "API")
public class ListExchangesOnSolidityServlet
    extends ListExchangesServlet {

  @Autowired
  private WalletOnSolidity walletOnSolidity;

  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    walletOnSolidity.futureGet(() -> super.doPost(request, response));
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    walletOnSolidity.futureGet(() -> super.doGet(request, response));
  }
}