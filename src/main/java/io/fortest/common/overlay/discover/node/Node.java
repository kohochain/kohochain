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

package io.fortest.common.overlay.discover.node;

import static io.fortest.common.crypto.Hash.sha3;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import io.fortest.common.crypto.ECKey;
import io.fortest.common.crypto.Hash;
import io.fortest.common.utils.ByteArray;
import io.fortest.common.utils.Utils;
import org.apache.commons.lang3.StringUtils;
import org.spongycastle.util.encoders.Hex;

public class Node implements Serializable {

  private static final long serialVersionUID = -4267600517925770636L;

  private byte[] id;

  private String host;

  private int port;

  private boolean isFakeNodeId = false;

  public int getReputation() {
    return reputation;
  }

  public void setReputation(int reputation) {
    this.reputation = reputation;
  }

  private int reputation = 0;

  public static Node instanceOf(String addressOrEnode) {
    try {
      URI uri = new URI(addressOrEnode);
      if ("enode".equals(uri.getScheme())) {
        return new Node(addressOrEnode);
      }
    } catch (URISyntaxException e) {
      // continue
    }

    final ECKey generatedNodeKey = ECKey.fromPrivate(Hash.sha3(addressOrEnode.getBytes()));
    final String generatedNodeId = Hex.toHexString(generatedNodeKey.getNodeId());
    final Node node = new Node("enode://" + generatedNodeId + "@" + addressOrEnode);
    node.isFakeNodeId = true;
    return node;
  }

  public String getEnodeURL() {
    return new StringBuilder("enode://")
        .append(ByteArray.toHexString(id)).append("@")
        .append(host).append(":")
        .append(port).toString();
  }

  public Node(String enodeURL) {
    try {
      URI uri = new URI(enodeURL);
      if (!"enode".equals(uri.getScheme())) {
        throw new RuntimeException("expecting URL in the format enode://PUBKEY@HOST:PORT");
      }
      this.id = Hex.decode(uri.getUserInfo());
      this.host = uri.getHost();
      this.port = uri.getPort();
    } catch (URISyntaxException e) {
      throw new RuntimeException("expecting URL in the format enode://PUBKEY@HOST:PORT", e);
    }
  }

  public Node(byte[] id, String host, int port) {
    if (id != null) {
      this.id = id.clone();
    }
    this.host = host;
    this.port = port;
  }

  public String getHexId() {
    return Hex.toHexString(id);
  }

  public String getHexIdShort() {
    return Utils.getIdShort(getHexId());
  }

  public boolean isDiscoveryNode() {
    return isFakeNodeId;
  }

  public byte[] getId() {
    return id == null ? id : id.clone();
  }

  public void setId(byte[] id) {
    this.id = id == null ? null : id.clone();
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getIdString() {
    if (id == null) {
      return null;
    }
    return new String(id);
  }

  @Override
  public String toString() {
    return "Node{" + " host='" + host + '\'' + ", port=" + port
        + ", id=" + ByteArray.toHexString(id) + '}';
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (o == this) {
      return true;
    }

    if (o.getClass() == getClass()) {
      return StringUtils.equals(getIdString(), ((Node) o).getIdString());
    }

    return false;
  }
//374164dd6fde379d8c0fbbafe04f06314176e051730bfd466f84ab0ed3844cb1f48a42185b4ddf9c9608636ba17a1c7982f8867f3c49a15c9fc78e4798591c5a
//17a643823b0d654bf1b3b4f1208a3f06ec6c07fd6916780b6cd03cf21b8ceaa1a0feb7c0f5101748f9caa8768dc4342932264b0a1180fadd8b8295bfd08856fb
//06f6bcafdf04bacce94146b2ac1b7ae50e80ea9525d06ec602da52cdcb85c80952925f2743fa33d18598dbf129e6250f733e78cfa2f9c36c1d988b8865584e51
  public static void main(String args[]){
    Node n = Node.instanceOf("18.136.197.114:18888");
    System.out.println(n);
    Random gen = new Random();
    byte[] id = new byte[64];
    gen.nextBytes(id);
    System.out.println(ByteArray.toHexString(id));
  }
}
