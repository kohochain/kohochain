/*
 * java-khc is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * java-khc is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.fortest.core.capsule;

import com.google.protobuf.ByteString;
import io.fortest.common.utils.ByteArray;
import io.fortest.protos.Protocol.TXOutput;

public class TxOutputCapsule implements ProtoCapsule<TXOutput> {

  private TXOutput txOutput;

  /**
   * constructor TxOutputCapsule.
   *
   * @param value int value
   * @param address String address
   */
  public TxOutputCapsule(long value, String address) {
    this.txOutput = TXOutput.newBuilder()
        .setValue(value)
        .setPubKeyHash(ByteString.copyFrom(ByteArray.fromHexString(address)))
        .build();
  }

  public TXOutput getTxOutput() {
    return txOutput;
  }

  /**
   * validateSignature.
   */
  public boolean validate() {
    return true;
  }

  @Override
  public byte[] getData() {
    return new byte[0];
  }

  @Override
  public TXOutput getInstance() {
    return this.txOutput;
  }
}
