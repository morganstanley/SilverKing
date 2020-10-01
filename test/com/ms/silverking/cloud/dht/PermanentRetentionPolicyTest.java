package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.TestUtil.getImplementationType;
import static com.ms.silverking.cloud.dht.TestUtil.getInitialState;
import static com.ms.silverking.cloud.dht.ValueRetentionPolicy.ImplementationType.RetainAll;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.test_Equals;
import static com.ms.silverking.testing.AssertFunction.test_Getters;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PermanentRetentionPolicyTest {

  private static final PermanentRetentionPolicy defaultPolicy = PermanentRetentionPolicy.template;

  @Test
  public void testGetters() {
    Object[][] testCases = { { RetainAll, getImplementationType(defaultPolicy) }, { null, getInitialState(
        defaultPolicy) }, };

    test_Getters(testCases);
  }

  @Test
  public void testRetains() {
    Object[][] testCases = { { defaultPolicy, null, 0L, 0L, false, new InvalidatedRetentionState(), 0L, true },
        { defaultPolicy, null, 0L, 0L, true, new InvalidatedRetentionState(), 0L, true },
        { defaultPolicy, null, -1L, -1L, true, new InvalidatedRetentionState(), -1L, true }, };

    TestUtil.checkRetains(testCases);
  }

  @Test
  public void testHashCode() {
    checkHashCodeEquals(defaultPolicy, defaultPolicy);
  }

  @Test
  public void testEqualsObject() {
    test_Equals(new Object[][] { { defaultPolicy, defaultPolicy }, });
    test_NotEquals(new Object[][] { { defaultPolicy, InvalidatedRetentionPolicy.template },
        { defaultPolicy, TimeAndVersionRetentionPolicy.template }, });
  }

  @Test
  public void testToStringAndParse() {
    PermanentRetentionPolicy[] testCases = { defaultPolicy, };

    for (PermanentRetentionPolicy testCase : testCases)
      checkStringAndParse(testCase);
  }

  private void checkStringAndParse(PermanentRetentionPolicy controller) {
    assertEquals(controller, PermanentRetentionPolicy.parse(controller.toString()));
  }
}
