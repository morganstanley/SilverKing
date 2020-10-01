package com.ms.silverking.numeric;

import static com.ms.silverking.testing.Util.byte_maxVal;
import static com.ms.silverking.testing.Util.byte_minVal;
import static com.ms.silverking.testing.Util.copy;
import static com.ms.silverking.testing.Util.createToString;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

// FIXME:bph: comments
public class NumConversionTest {

  private static final byte[] long_min = { byte_minVal, byte_minVal, byte_minVal, byte_minVal, byte_minVal, byte_minVal,
      byte_minVal, byte_minVal };
  private static final byte[] long_negOne = { -1, -1, -1, -1, -1, -1, -1, -1 };
  private static final byte[] long_zero = { 0, 0, 0, 0, 0, 0, 0, 0 };
  private static final byte[] long_one = { 1, 1, 1, 1, 1, 1, 1, 1 };
  private static final byte[] long_max = { byte_maxVal, byte_maxVal, byte_maxVal, byte_maxVal, byte_maxVal, byte_maxVal,
      byte_maxVal, byte_maxVal };
  private static final byte[] long_neg5To5 = { -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5 };

  private static final int firstIndex = 0;
  private static final byte[] byte_min = { long_min[firstIndex] };
  private static final byte[] byte_negOne = { long_negOne[firstIndex] };
  private static final byte[] byte_zero = { long_zero[firstIndex] };
  private static final byte[] byte_one = { long_one[firstIndex] };
  private static final byte[] byte_max = { long_max[firstIndex] };
  private static final byte[] byte_minToMax = { byte_minVal, -1, 0, 1, byte_maxVal };

  private static final byte[] short_min = { long_min[firstIndex], long_min[firstIndex + 1] };
  private static final byte[] short_negOne = { long_negOne[firstIndex], long_negOne[firstIndex + 1] };
  private static final byte[] short_zero = { long_zero[firstIndex], long_zero[firstIndex + 1] };
  private static final byte[] short_one = { long_one[firstIndex], long_one[firstIndex + 1] };
  private static final byte[] short_max = { long_max[firstIndex], long_max[firstIndex + 1] };

  private static final byte[] int_min = { long_min[firstIndex], long_min[firstIndex + 1], long_min[firstIndex + 2],
      long_min[firstIndex + 3] };
  private static final byte[] int_negOne = { long_negOne[firstIndex], long_negOne[firstIndex + 1],
      long_negOne[firstIndex + 2], long_negOne[firstIndex + 3] };
  private static final byte[] int_zero = { long_zero[firstIndex], long_zero[firstIndex + 1], long_zero[firstIndex + 2],
      long_zero[firstIndex + 3] };
  private static final byte[] int_one = { long_one[firstIndex], long_one[firstIndex + 1], long_one[firstIndex + 2],
      long_one[firstIndex + 3] };
  private static final byte[] int_max = { long_max[firstIndex], long_max[firstIndex + 1], long_max[firstIndex + 2],
      long_max[firstIndex + 3] };

  private static final byte[] short_tenThousand = { 39, 16 };
  private static final byte[] short_tenThousandOffset0 = { 39, 16, -3, -2, -1, 0, 1, 2, 3, 4, 5 };
  private static final byte[] short_tenThounandLittle0 = { 16, 39, -3, -2, -1, 0, 1, 2, 3, 4, 5 };
  private static final byte[] short_tenThousandOffset6 = { -5, -4, -3, -2, -1, 0, 39, 16, 3, 4, 5 };
  private static final byte[] short_tenThounandLittle6 = { -5, -4, -3, -2, -1, 0, 16, 39, 3, 4, 5 };
  private static final byte[] short_tenThounandOffset9 = { -5, -4, -3, -2, -1, 0, 1, 2, 3, 39, 16 };
  private static final byte[] short_tenThounandLittle9 = { -5, -4, -3, -2, -1, 0, 1, 2, 3, 16, 39 };

  @Test
  public void testByteToBoolean() {
    Object[][] testCases = {
        //            {new byte[]{},  false, 0, false},
        { byte_min, true, 0, true }, { byte_negOne, true, 0, true }, { byte_zero, false, 0, false },
        { byte_one, true, 0, true }, { byte_max, true, 0, true }, { byte_minToMax, true, 0, true },
        { byte_minToMax, true, 2, false }, { byte_minToMax, true, 4, true }, };

    for (Object[] testCase : testCases) {
      byte[] bytes = (byte[]) testCase[0];
      boolean expected = (boolean) testCase[1];
      int offset = (int) testCase[2];
      boolean offsetExpected = (boolean) testCase[3];

      checkByteToBoolean(bytes, expected, offset, offsetExpected);
    }
  }

  private void checkByteToBoolean(byte[] bytes, boolean expected, int offset, boolean offsetExpected) {
    String testName = "byteToBoolean";
    String bytesString = createToString(bytes);
    assertEquals(getTestMessage(testName, bytesString), expected, NumConversion.byteToBoolean(bytes));
    assertEquals(getTestMessage(testName, bytesString, offset), offsetExpected,
        NumConversion.byteToBoolean(bytes, offset));
  }

  @Test
  public void testBooleanToByte() {
    checkBooleanToByte(false, (byte) 0);
    checkBooleanToByte(true, (byte) 1);
  }

  private void checkBooleanToByte(boolean _boolean, byte expected) {
    assertEquals(getTestMessage("booleanToByte", _boolean), expected, NumConversion.booleanToByte(_boolean));
  }

  @Test
  public void testBytesToShort() {
    Object[][] testCases = { { short_min, -32_640, 0, -32_640 }, { short_negOne, -1, 0, -1 }, { short_zero, 0, 0, 0 },
        { short_one, 257, 0, 257 }, { short_max, 32_639, 0, 32_639 }, { byte_minToMax, -32_513, 0, -32_513 },
        { byte_minToMax, -32_513, 2, 1 }, { byte_minToMax, -32_513, 3, 383 }, };

    for (Object[] testCase : testCases) {
      byte[] bytes = (byte[]) testCase[0];
      short expected = (short) (int) testCase[1];
      int offset = (int) testCase[2];
      short offsetExpected = (short) (int) testCase[3];

      checkBytesToShort(bytes, expected, offset, offsetExpected);
    }
  }

  private void checkBytesToShort(byte[] bytes, short expected, int offset, short offsetExpected) {
    String testName = "bytesToShort";
    String bytesString = createToString(bytes);
    assertEquals(getTestMessage(testName, bytesString), expected, NumConversion.bytesToShort(bytes));
    assertEquals(getTestMessage(testName, bytesString, offset), offsetExpected,
        NumConversion.bytesToShort(bytes, offset));
  }

  @Test
  public void testShortToBytes() {
    Object[][] testCases = { { short_min, 32_639, short_max, 0, short_max, short_max },
        { short_negOne, 257, short_one, 0, short_one, short_one },
        { short_zero, 0, short_zero, 0, short_zero, short_zero },
        { short_one, -1, short_negOne, 0, short_negOne, short_negOne },
        { short_max, -32_640, short_min, 0, short_min, short_min },

        { long_neg5To5, 10_000, short_tenThousand, 0, short_tenThousandOffset0, short_tenThounandLittle0 },
        { long_neg5To5, 10_000, short_tenThousand, 6, short_tenThousandOffset6, short_tenThounandLittle6 },
        { long_neg5To5, 10_000, short_tenThousand, 9, short_tenThounandOffset9, short_tenThounandLittle9 }, };

    for (Object[] testCase : testCases) {
      byte[] bytes = (byte[]) testCase[0];
      short value = (short) (int) testCase[1];
      byte[] expected = (byte[]) testCase[2];
      int offset = (int) testCase[3];
      byte[] offsetExpected = (byte[]) testCase[4];
      byte[] littleEndianOffsetExpected = (byte[]) testCase[5];

      checkShortToBytes(value, bytes, expected, offset, offsetExpected, littleEndianOffsetExpected);
    }
  }

  private void checkShortToBytes(short value, byte[] bytes, byte[] expected, int offset, byte[] offsetExpected,
      byte[] littleEndianOffsetExpected) {
    String testName = "shortToBytes";
    String bytesString = createToString(bytes);

    byte[] actual = NumConversion.shortToBytes(value);
    assertArrayEquals(getTestMessage(testName, value, createToString(expected), createToString(actual)), expected,
        actual);

    byte[] bytesCopy = copy(bytes);
    NumConversion.shortToBytes(value, bytesCopy, offset);
    assertArrayEquals(
        getTestMessage(testName, bytesString, offset, value, createToString(offsetExpected), createToString(bytesCopy)),
        offsetExpected, bytesCopy);

    byte[] bytesCopy2 = copy(bytes);
    NumConversion.shortToBytesLittleEndian(value, bytesCopy2, offset);
    assertArrayEquals(getTestMessage(testName, bytesString, offset, value, createToString(littleEndianOffsetExpected),
        createToString(bytesCopy2)), littleEndianOffsetExpected, bytesCopy2);
  }

  @Test
  public void testBytesToUnsignedShort() {
    Object[][] testCases = { { short_min, 0, 32_896 }, { short_negOne, 0, 65_535 }, { short_zero, 0, 0 },
        { short_one, 0, 257 }, { short_max, 0, 32_639 }, { byte_minToMax, 0, 33_023 }, { byte_minToMax, 2, 1 },
        { byte_minToMax, 3, 383 }, };

    for (Object[] testCase : testCases) {
      byte[] bytes = (byte[]) testCase[0];
      int offset = (int) testCase[1];
      int offsetExpected = (int) testCase[2];

      checkBytesToUnsignedShort(bytes, offset, offsetExpected);
    }
  }

  private void checkBytesToUnsignedShort(byte[] bytes, int offset, int offsetExpected) {
    String testName = "bytesToUnsignedShort";
    String bytesString = createToString(bytes);
    assertEquals(getTestMessage(testName, bytesString, offset, offsetExpected), offsetExpected,
        NumConversion.bytesToUnsignedShort(bytes, offset));
  }

  @Test
  public void testUnsignedShortToBytes() {
    Object[][] testCases = { { short_min, 32_639, 0, short_max }, { short_negOne, 257, 0, short_one },
        { short_zero, 0, 0, short_zero }, { short_one, 65_535, 0, short_negOne }, { short_max, 32_896, 0, short_min },

        { long_neg5To5, 10_000, 0, short_tenThousandOffset0 }, { long_neg5To5, 10_000, 6, short_tenThousandOffset6 },
        { long_neg5To5, 10_000, 9, short_tenThounandOffset9 }, };

    for (Object[] testCase : testCases) {
      byte[] bytes = (byte[]) testCase[0];
      int value = (int) testCase[1];
      int offset = (int) testCase[2];
      byte[] offsetExpected = (byte[]) testCase[3];

      checkUnsignedShortToBytes(value, bytes, offset, offsetExpected);
    }
  }

  private void checkUnsignedShortToBytes(int value, byte[] bytes, int offset, byte[] offsetExpected) {
    String testName = "unsignedShortToBytes";
    String bytesString = createToString(bytes);

    byte[] bytesCopy = copy(bytes);
    NumConversion.unsignedShortToBytes(value, bytesCopy, offset);
    assertArrayEquals(
        getTestMessage(testName, bytesString, offset, value, createToString(offsetExpected), createToString(bytesCopy)),
        offsetExpected, bytesCopy);
  }

  @Test
  public void testBytesToInt() {
    Object[][] testCases = { { int_min, -2_139_062_144, 0, -2_139_062_144, -2_139_062_144, -2_139_062_144 },
        { int_negOne, -1, 0, -1, -1, -1 }, { int_zero, 0, 0, 0, 0, 0 },
        { int_one, 16_843_009, 0, 16_843_009, 16_843_009, 16_843_009 },
        { int_max, 2_139_062_143, 0, 2_139_062_143, 2_139_062_143, 2_139_062_143 },
        { long_neg5To5, -67_305_986, 0, -67_305_986, -16_909_061, -16_909_061 },
        { long_neg5To5, -673_05_986, 4, -16_776_958, -16_909_061, 33_620_223 },
        { long_neg5To5, -673_05_986, 7, 33_752_069, -16_909_061, 84_148_994 }, };

    for (Object[] testCase : testCases) {
      byte[] bytes = (byte[]) testCase[0];
      int expected = (int) testCase[1];
      int offset = (int) testCase[2];
      int offsetExpected = (int) testCase[3];
      int littleEndianExpected = (int) testCase[4];
      int littleEndianOffsetExpected = (int) testCase[5];

      checkBytesToInt(bytes, expected, offset, offsetExpected, littleEndianExpected, littleEndianOffsetExpected);
    }
  }

  private void checkBytesToInt(byte[] bytes, int expected, int offset, int offsetExpected, int littleEndianExpected,
      int littleEndianOffsetExpected) {
    String testName = "bytesToInt";
    String bytesString = createToString(bytes);
    assertEquals(getTestMessage(testName, bytesString), expected, NumConversion.bytesToInt(bytes));
    assertEquals(getTestMessage(testName, bytesString, offset), offsetExpected,
        NumConversion.bytesToInt(bytes, offset));
    assertEquals(getTestMessage(testName, bytesString), littleEndianExpected,
        NumConversion.bytesToIntLittleEndian(bytes));
    assertEquals(getTestMessage(testName, bytesString, offset), littleEndianOffsetExpected,
        NumConversion.bytesToIntLittleEndian(bytes, offset));
  }

  @Test
  public void testIntToBytes() {
    byte[] int_oneMillion = { 0, 15, 66, 64 };
    byte[] int_oneMillionOffset0 = { 0, 15, 66, 64, -1, 0, 1, 2, 3, 4, 5 };
    byte[] int_oneMillionLittle0 = { 64, 66, 15, 0, -1, 0, 1, 2, 3, 4, 5 };
    byte[] int_oneMillionOffset4 = { -5, -4, -3, -2, 0, 15, 66, 64, 3, 4, 5 };
    byte[] int_oneMillionLittle4 = { -5, -4, -3, -2, 64, 66, 15, 0, 3, 4, 5 };
    byte[] int_oneMillionOffset7 = { -5, -4, -3, -2, -1, 0, 1, 0, 15, 66, 64 };
    byte[] int_oneMillionLittle7 = { -5, -4, -3, -2, -1, 0, 1, 64, 66, 15, 0 };

    Object[][] testCases = { { int_min, 2_139_062_143, int_max, 0, int_max, int_max },
        { int_negOne, 16_843_009, int_one, 0, int_one, int_one }, { int_zero, 0, int_zero, 0, int_zero, int_zero },
        { int_one, -1, int_negOne, 0, int_negOne, int_negOne },
        { int_max, -2_139_062_144, int_min, 0, int_min, int_min },

        { long_neg5To5, 1_000_000, int_oneMillion, 0, int_oneMillionOffset0, int_oneMillionLittle0 },
        { long_neg5To5, 1_000_000, int_oneMillion, 4, int_oneMillionOffset4, int_oneMillionLittle4 },
        { long_neg5To5, 1_000_000, int_oneMillion, 7, int_oneMillionOffset7, int_oneMillionLittle7 }, };

    for (Object[] testCase : testCases) {
      byte[] bytes = (byte[]) testCase[0];
      int value = (int) testCase[1];
      byte[] expected = (byte[]) testCase[2];
      int offset = (int) testCase[3];
      byte[] offsetExpected = (byte[]) testCase[4];
      byte[] littleEndianOffsetExpected = (byte[]) testCase[5];

      checkIntToBytes(value, bytes, expected, offset, offsetExpected, littleEndianOffsetExpected);
    }
  }

  private void checkIntToBytes(int value, byte[] bytes, byte[] expected, int offset, byte[] offsetExpected,
      byte[] littleEndianOffsetExpected) {
    String testName = "intToBytes";
    String bytesString = createToString(bytes);

    byte[] actual = NumConversion.intToBytes(value);
    assertArrayEquals(getTestMessage(testName, value, createToString(expected), createToString(actual)), expected,
        actual);

    byte[] bytesCopy = copy(bytes);
    NumConversion.intToBytes(value, bytesCopy, offset);
    assertArrayEquals(
        getTestMessage(testName, bytesString, offset, value, createToString(offsetExpected), createToString(bytesCopy)),
        offsetExpected, bytesCopy);

    byte[] bytesCopy2 = copy(bytes);
    NumConversion.intToBytesLittleEndian(value, bytesCopy2, offset);
    assertArrayEquals(getTestMessage(testName, bytesString, offset, value, createToString(littleEndianOffsetExpected),
        createToString(bytesCopy2)), littleEndianOffsetExpected, bytesCopy2);
  }

  @Test
  public void testBytesToLong() {
    Object[][] testCases = {
        { long_min, 0, -9_187_201_950_435_737_472L, -9_187_201_950_435_737_472L, -9_187_201_950_435_737_472L,
            -9_187_201_950_435_737_472L }, { long_negOne, 0, -1L, -1L, -1L, -1L }, { long_zero, 0, 0L, 0L, 0L, 0L },
        { long_one, 0, 72_340_172_838_076_673L, 72_340_172_838_076_673L, 72_340_172_838_076_673L,
            72_340_172_838_076_673L },
        { long_max, 0, 9_187_201_950_435_737_471L, 9_187_201_950_435_737_471L, 9_187_201_950_435_737_471L,
            9_187_201_950_435_737_471L },
        { long_neg5To5, 0, -289_077_004_416_843_518L, -289_077_004_416_843_518L, 144_397_762_547_285_243L,
            144_397_762_547_285_243L },
        { long_neg5To5, 2, -289_077_004_416_843_518L, -144_397_762_547_285_244L, 144_397_762_547_285_243L,
            289_077_004_416_843_517L },
        { long_neg5To5, 3, -289_077_004_416_843_518L, -72_339_064_685_919_227L, 144_397_762_547_285_243L,
            361_417_177_238_142_974L }, };

    for (Object[] testCase : testCases) {
      byte[] bytes = (byte[]) testCase[0];
      int offset = (int) testCase[1];
      long expected = (long) testCase[2];
      long offsetExpected = (long) testCase[3];
      long littleEndianExpected = (long) testCase[4];
      long littleEndianOffsetExpected = (long) testCase[5];

      checkBytesToLong(bytes, expected, offset, offsetExpected, littleEndianExpected, littleEndianOffsetExpected);
    }
  }

  private void checkBytesToLong(byte[] bytes, long expected, int offset, long offsetExpected, long littleEndianExpected,
      long littleEndianOffsetExpected) {
    String testName = "bytesToLong";
    String bytesString = createToString(bytes);
    assertEquals(getTestMessage(testName, bytesString), expected, NumConversion.bytesToLong(bytes));
    assertEquals(getTestMessage(testName, bytesString, offset), offsetExpected,
        NumConversion.bytesToLong(bytes, offset));
    assertEquals(getTestMessage(testName, bytesString), littleEndianExpected,
        NumConversion.bytesToLongLittleEndian(bytes));
    assertEquals(getTestMessage(testName, bytesString, offset), littleEndianOffsetExpected,
        NumConversion.bytesToLongLittleEndian(bytes, offset));
  }

  @Test
  public void testLongToBytes() {
    byte[] long_oneMillion = { 0, 0, 0, 0, 0, 15, 66, 64 };
    byte[] long_oneMillionOffset0 = { 0, 0, 0, 0, 0, 15, 66, 64, 3, 4, 5 };
    byte[] long_oneMillionLittle0 = { 64, 66, 15, 0, 0, 0, 0, 0, 3, 4, 5 };
    byte[] long_oneMillionOffset2 = { -5, -4, 0, 0, 0, 0, 0, 15, 66, 64, 5 };
    byte[] long_oneMillionLittle2 = { -5, -4, 64, 66, 15, 0, 0, 0, 0, 0, 5 };
    byte[] long_oneMillionOffset3 = { -5, -4, -3, 0, 0, 0, 0, 0, 15, 66, 64 };
    byte[] long_oneMillionLittle3 = { -5, -4, -3, 64, 66, 15, 0, 0, 0, 0, 0 };

    Object[][] testCases = { { long_min, 9_187_201_950_435_737_471L, long_max, 0, long_max, long_max },
        { long_negOne, 72_340_172_838_076_673L, long_one, 0, long_one, long_one },
        { long_zero, 0L, long_zero, 0, long_zero, long_zero },
        { long_one, -1L, long_negOne, 0, long_negOne, long_negOne },
        { long_max, -9_187_201_950_435_737_472L, long_min, 0, long_min, long_min },

        { long_neg5To5, 1_000_000L, long_oneMillion, 0, long_oneMillionOffset0, long_oneMillionLittle0 },
        { long_neg5To5, 1_000_000L, long_oneMillion, 2, long_oneMillionOffset2, long_oneMillionLittle2 },
        { long_neg5To5, 1_000_000L, long_oneMillion, 3, long_oneMillionOffset3, long_oneMillionLittle3 }, };

    for (Object[] testCase : testCases) {
      byte[] bytes = (byte[]) testCase[0];
      long value = (long) testCase[1];
      byte[] expected = (byte[]) testCase[2];
      int offset = (int) testCase[3];
      byte[] offsetExpected = (byte[]) testCase[4];
      byte[] littleEndianOffsetExpected = (byte[]) testCase[5];

      checkLongToBytes(value, bytes, expected, offset, offsetExpected, littleEndianOffsetExpected);
    }
  }

  private void checkLongToBytes(long value, byte[] bytes, byte[] expected, int offset, byte[] offsetExpected,
      byte[] littleEndianOffsetExpected) {
    String testName = "longToBytes";
    String bytesString = createToString(bytes);

    byte[] actual = NumConversion.longToBytes(value);
    assertArrayEquals(getTestMessage(testName, value, createToString(expected), createToString(actual)), expected,
        actual);

    byte[] bytesCopy = copy(bytes);
    NumConversion.longToBytes(value, bytesCopy, offset);
    assertArrayEquals(
        getTestMessage(testName, bytesString, offset, value, createToString(offsetExpected), createToString(bytesCopy)),
        offsetExpected, bytesCopy);

    byte[] bytesCopy2 = copy(bytes);
    NumConversion.longToBytesLittleEndian(value, bytesCopy2, offset);
    assertArrayEquals(getTestMessage(testName, bytesString, offset, value, createToString(littleEndianOffsetExpected),
        createToString(bytesCopy2)), littleEndianOffsetExpected, bytesCopy2);
  }

  @Test
  public void testBytesToDouble() {
    Object[][] testCases = {
        //            {long_min,     0, -9_187_201_950_435_737_472d, -9_187_201_950_435_737_472d},
        //            {long_negOne,  0,                         -1d,                         -1d},
        { long_zero, 0, 0d, 0d },
        //            {long_one,     0,     72_340_172_838_076_673d,     72_340_172_838_076_673d},
        //            {long_max,     0,  9_187_201_950_435_737_471L,  9_187_201_950_435_737_471L},
        //            {long_neg5To5, 0,   -289_077_004_416_843_518L,   -289_077_004_416_843_518L},
        //            {long_neg5To5, 2,   -289_077_004_416_843_518L,   -144_397_762_547_285_244L},
        //            {long_neg5To5, 3,   -289_077_004_416_843_518L,    -72_339_064_685_919_227L},
    };

    for (Object[] testCase : testCases) {
      byte[] bytes = (byte[]) testCase[0];
      int offset = (int) testCase[1];
      double expected = (double) testCase[2];
      double offsetExpected = (double) testCase[3];

      double actualBytesToDouble = NumConversion.bytesToDouble(bytes);
      double actualBytesToDoubleOffset = NumConversion.bytesToDouble(bytes, offset);
      byte[] actualDoubleToBytes = NumConversion.doubleToBytes(bytes[0]);
      byte[] bytesCopy = copy(bytes);
      NumConversion.doubleToBytes(bytesCopy[offset], bytesCopy, offset);

      String testName = "bytesToDouble->doubleToBytes";
      String bytesString = createToString(bytes);
      assertArrayEquals(getTestMessage(testName, bytesString, createToString(actualDoubleToBytes)), bytes,
          actualDoubleToBytes);
      assertArrayEquals(getTestMessage(testName, bytesString, offset), bytes, bytesCopy);
      //            checkBytesToDouble(bytes, expected, offset, offsetExpected);
      //            checkDoubleToBytes();
    }
  }

  //    private void checkBytesToDouble(byte[] bytes, double expected, int offset, double offsetExpected) {
  //        String testName = "bytesToDouble";
  //        String bytesString = createToString(bytes);
  //        assertEquals(getTestMessage(testName, bytesString),                               expected, NumConversion
  //        .bytesToDouble(bytes),         0);
  //        assertEquals(getTestMessage(testName, bytesString, offset, offsetExpected), offsetExpected, NumConversion
  //        .bytesToDouble(bytes, offset), 0);
  //    }
  //
  //    private void checkDoubleToBytes(long value, byte[] bytes, byte[] expected, int offset, byte[] offsetExpected) {
  //        String testName = "doubleToBytes";
  //        String bytesString = createToString(bytes);
  //
  //        byte[] actual = NumConversion.doubleToBytes(value);
  //        assertArrayEquals(getTestMessage(testName, value, createToString(expected), createToString(actual)),
  //        expected, actual);
  //
  //        byte[] bytesCopy = copy(bytes);
  //        NumConversion.doubleToBytes(value, bytesCopy, offset);
  //        assertArrayEquals(getTestMessage(testName, bytesString, offset, value, createToString(offsetExpected),
  //        createToString(bytesCopy)), offsetExpected, bytesCopy);
  //    }

  @Test
  public void testIntsToLong() {
    int int_minVal = Integer.MIN_VALUE;
    int int_maxVal = Integer.MAX_VALUE;

    Object[][] testCases = { { int_minVal, int_minVal, -9_223_372_034_707_292_160L }, { -1, -1, -1L }, { 0, 0, 0L },
        { 1, 1, 4_294_967_297L }, { int_maxVal, int_maxVal, 9_223_372_034_707_292_159L },
        { int_minVal, int_maxVal, -9_223_372_034_707_292_161L },
        { int_maxVal, int_minVal, 9_223_372_034_707_292_160L }, };

    for (Object[] testCase : testCases) {
      int msi = (int) testCase[0];
      int lsi = (int) testCase[1];
      long expected = (long) testCase[2];

      checkIntsToLong(msi, lsi, expected);
    }
  }

  private void checkIntsToLong(int msi, int lsi, long expected) {
    String testName = "intsToLong";
    assertEquals(getTestMessage(testName, msi, lsi), expected, NumConversion.intsToLong(msi, lsi));
  }

  @Test
  public void testParseHexStringAsUnsignedLong() {
    Object[][] testCases = { { "1000000000000000", 1_152_921_504_606_846_976L }, { "FFFFFFFFFFFFFFFF", -1L },
        { "0", 0L }, { "0000000000000001", 1L }, { "0FFFFFFFFFFFFFFF", 1_152_921_504_606_846_975L }, };

    for (Object[] testCase : testCases) {
      String hexString = (String) testCase[0];
      long expected = (long) testCase[1];

      checkParseHexStringAsUnsignedLong(hexString, expected);
    }
  }

  private void checkParseHexStringAsUnsignedLong(String hexString, long expected) {
    String testName = "parseHexStringAsUnsignedLong";
    assertEquals(getTestMessage(testName, hexString), expected, NumConversion.parseHexStringAsUnsignedLong(hexString));
  }
}
