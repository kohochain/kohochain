package io.fortest.core.services.http;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.fortest.core.capsule.TransactionCapsule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.fortest.core.Wallet;
import io.fortest.protos.Protocol;
import io.fortest.protos.Protocol.Transaction;
import io.fortest.protos.Protocol.TransactionSign;


@Component
@Slf4j(topic = "API")
public class TransactionSignServlet extends HttpServlet {

  @Autowired
  private Wallet wallet;

  protected void doGet(HttpServletRequest request, HttpServletResponse response) {

  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    try {
      String contract = request.getReader().lines()
          .collect(Collectors.joining(System.lineSeparator()));
      Util.checkBodySize(contract);
      JSONObject input = JSONObject.parseObject(contract);
      boolean visible = Util.getVisibleOnlyForSign(input);
      String strTransaction = input.getJSONObject("transaction").toJSONString();
      Transaction transaction = Util.packTransaction(strTransaction, visible);
      JSONObject jsonTransaction = JSONObject.parseObject(JsonFormat.printToString(transaction,
          visible));
      input.put("transaction", jsonTransaction);
      TransactionSign.Builder build = TransactionSign.newBuilder();
      JsonFormat.merge(input.toJSONString(), build, visible);
      TransactionCapsule reply = wallet.getTransactionSign(build.build());
      if (reply != null) {
        response.getWriter().println(Util.printCreateTransaction(reply.getInstance(), visible));
      } else {
        response.getWriter().println("{}");
      }
    } catch (Exception e) {
      logger.debug("Exception: {}", e.getMessage());
      try {
        response.getWriter().println(Util.printErrorMsg(e));
      } catch (IOException ioe) {
        logger.debug("IOException: {}", ioe.getMessage());
      }
    }
  }
  public static JSONObject sign(JSONObject unSignTransaction, String privateKey, boolean visible) throws JsonFormat.ParseException {
    Wallet wallet = new Wallet();
    JSONObject input = new JSONObject();
    input.put("transaction", unSignTransaction);
    input.put("privateKey", privateKey);
    String strTransaction = input.getJSONObject("transaction").toJSONString();
    Protocol.Transaction transaction = Util.packTransaction(strTransaction, visible);
    JSONObject jsonTransaction = JSONObject.parseObject(JsonFormat.printToString(transaction,
            visible));
    input.put("transaction", jsonTransaction);
    Protocol.TransactionSign.Builder build = Protocol.TransactionSign.newBuilder();
    JsonFormat.merge(unSignTransaction.toJSONString(), build, visible);
    TransactionCapsule reply = wallet.getTransactionSign(build.build());
    return JSONObject.parseObject(Util.printCreateTransaction(reply.getInstance(), visible));
  }

  public static void main(String args[]) throws JsonFormat.ParseException {
    String s = "{\"visible\":false,\"txID\":\"8c96d4601c5f5c85a3ba6f7ff74e3f7dcd6f4c3bbbb6cb92ee4a8542eb354005\",\"raw_data\":{\"contract\":[{\"parameter\":{\"value\":{\"frozen_balance\":1000000000,\"owner_address\":\"324a39be0c4ea5544f6cf13390237f19755e81f2e7\"},\"type_url\":\"type.googleapis.com/protocol.FreezeBalanceContract\"},\"type\":\"FreezeBalanceContract\"}],\"ref_block_bytes\":\"8760\",\"ref_block_hash\":\"2129094027cb2f7e\",\"expiration\":1574781156000,\"timestamp\":1574781097822},\"raw_data_hex\":\"0a02876022082129094027cb2f7e40a0f595c2ea2d5a57080b12530a32747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e467265657a6542616c616e6365436f6e7472616374121d0a15324a39be0c4ea5544f6cf13390237f19755e81f2e7108094ebdc0370deae92c2ea2d\"}\n";
    JSONObject re = JSONObject.parseObject(s);
    sign(re, "27fcfc91638106b7f3509ac0edbcb858858102b87da35ba2c9ee662ab4ec1c50", false);
  }

}
