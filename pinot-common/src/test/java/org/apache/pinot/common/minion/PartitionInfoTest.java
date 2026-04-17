/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.minion;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PartitionInfoTest {

  @Test
  public void testEncodeAndDecodeValid() {
    PartitionInfo info = new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(10, 5000L), 1700006400000L);
    String encoded = info.encode();
    assertEquals(encoded, "V,10,5000,1700006400000");

    PartitionInfo decoded = PartitionInfo.decode(encoded);
    assertEquals(decoded.getState(), PartitionState.VALID);
    assertEquals(decoded.getFingerprint().getSegmentCount(), 10);
    assertEquals(decoded.getFingerprint().getCrcChecksum(), 5000L);
    assertEquals(decoded.getLastRefreshTime(), 1700006400000L);
    assertEquals(decoded, info);
  }

  @Test
  public void testEncodeAndDecodeStale() {
    PartitionInfo info = new PartitionInfo(
        PartitionState.STALE, new PartitionFingerprint(3, -999L), 0L);
    String encoded = info.encode();
    assertEquals(encoded, "S,3,-999,0");

    PartitionInfo decoded = PartitionInfo.decode(encoded);
    assertEquals(decoded.getState(), PartitionState.STALE);
    assertEquals(decoded.getFingerprint(), new PartitionFingerprint(3, -999L));
    assertEquals(decoded.getLastRefreshTime(), 0L);
    assertEquals(decoded, info);
  }

  @Test
  public void testEncodeAndDecodeExpired() {
    PartitionInfo info = new PartitionInfo(
        PartitionState.EXPIRED, new PartitionFingerprint(0, 0L), 1700006400000L);
    String encoded = info.encode();
    assertEquals(encoded, "E,0,0,1700006400000");

    PartitionInfo decoded = PartitionInfo.decode(encoded);
    assertEquals(decoded.getState(), PartitionState.EXPIRED);
    assertEquals(decoded.getFingerprint().getSegmentCount(), 0);
    assertEquals(decoded.getFingerprint().getCrcChecksum(), 0L);
    assertEquals(decoded.getLastRefreshTime(), 1700006400000L);
    assertEquals(decoded, info);
  }

  @Test
  public void testWithStateToExpired() {
    PartitionFingerprint fp = new PartitionFingerprint(5, 1234L);
    PartitionInfo valid = new PartitionInfo(PartitionState.VALID, fp, 1000L);
    PartitionInfo expired = valid.withState(PartitionState.EXPIRED);

    assertEquals(expired.getState(), PartitionState.EXPIRED);
    assertEquals(expired.getFingerprint(), fp);
    assertEquals(expired.getLastRefreshTime(), 1000L);
    assertNotEquals(expired, valid);
  }

  @Test
  public void testEncodeAndDecodeZeroValues() {
    PartitionInfo info = new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(0, 0L), 0L);
    PartitionInfo decoded = PartitionInfo.decode(info.encode());
    assertEquals(decoded, info);
  }

  @Test
  public void testWithState() {
    PartitionFingerprint fp = new PartitionFingerprint(5, 1234L);
    PartitionInfo valid = new PartitionInfo(PartitionState.VALID, fp, 1000L);
    PartitionInfo stale = valid.withState(PartitionState.STALE);

    assertEquals(stale.getState(), PartitionState.STALE);
    assertEquals(stale.getFingerprint(), fp);
    assertEquals(stale.getLastRefreshTime(), 1000L);
    assertNotEquals(stale, valid);
  }

  @Test
  public void testFromLegacyFingerprint() {
    PartitionFingerprint fp = new PartitionFingerprint(7, 42L);
    PartitionInfo info = PartitionInfo.fromLegacyFingerprint(fp);

    assertEquals(info.getState(), PartitionState.VALID);
    assertEquals(info.getFingerprint(), fp);
    assertEquals(info.getLastRefreshTime(), 0L);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalidNoSeparator() {
    PartitionInfo.decode("V");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalidTooFewFields() {
    PartitionInfo.decode("V,10");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalidThreeFields() {
    PartitionInfo.decode("V,10,5000");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalidState() {
    PartitionInfo.decode("X,10,5000,1000");
  }

  @Test
  public void testEqualsAndHashCode() {
    PartitionFingerprint fp = new PartitionFingerprint(5, 100L);
    PartitionInfo a = new PartitionInfo(PartitionState.VALID, fp, 1000L);
    PartitionInfo b = new PartitionInfo(PartitionState.VALID, fp, 1000L);
    PartitionInfo c = new PartitionInfo(PartitionState.STALE, fp, 1000L);
    PartitionInfo d = new PartitionInfo(PartitionState.VALID, fp, 2000L);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
    assertNotEquals(a, d);
    assertNotEquals(a, null);
  }

  @Test
  public void testToString() {
    PartitionInfo info = new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(3, 42L), 999L);
    String str = info.toString();
    assertTrue(str.contains("VALID"));
    assertTrue(str.contains("lastRefreshTime=999"));
  }
}
