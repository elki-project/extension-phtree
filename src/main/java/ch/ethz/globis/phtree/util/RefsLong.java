package ch.ethz.globis.phtree.util;

/*
This file is part of ELKI:
Environment for Developing KDD-Applications Supported by Index-Structures

Copyright (C) 2011-2015
Eidgenössische Technische Hochschule Zürich (ETH Zurich)
Institute for Information Systems
GlobIS Group

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import ch.ethz.globis.phtree.PhTreeHelper;

/**
 * Manipulation methods and pool for long[].
 * 
 * @author ztilmann
 */
public class RefsLong {

  private static final long[] EMPTY_REF_ARRAY = {};
  private static final ArrayPoolN POOL = 
      new ArrayPoolN(PhTreeHelper.ARRAY_POOLING_MAX_ARRAY_SIZE, 
          PhTreeHelper.ARRAY_POOLING_POOL_SIZE);

  private RefsLong() {
    //nothing
  }

  private static class ArrayPoolN {
    private final int maxArraySize;
    private final int maxArrayCount;
    long[][][] pool;
    int[] poolSize;
    ArrayPoolN(int maxArraySize, int maxArrayCount) {
      this.maxArraySize = maxArraySize;
      this.maxArrayCount = maxArrayCount;
      this.pool = new long[maxArraySize+1][maxArrayCount][];
      this.poolSize = new int[maxArraySize+1];
    }

    long[] getArray(int size) {
      if (size == 0) {
        return EMPTY_REF_ARRAY;
      }
      if (size > maxArraySize || !PhTreeHelper.ARRAY_POOLING) {
        return new long[size];
      }
      synchronized (this) {
        int ps = poolSize[size]; 
        if (ps > 0) {
          poolSize[size]--;
          long[] ret = pool[size][ps-1];
          Arrays.fill(ret, 0);
          return ret;
        }
      }
      return new long[size];
    }

    synchronized void offer(long[] a) {
      int size = a.length;
      if (size == 0 || size > maxArraySize || !PhTreeHelper.ARRAY_POOLING) {
        return;
      }
      synchronized (this) {
        int ps = poolSize[size]; 
        if (ps < maxArrayCount) {
          pool[size][ps] = a;
          poolSize[size]++;
        }
      }
    }
  }


  /**
   * Calculate array size for given number of bits.
   * This takes into account JVM memory management, which allocates multiples of 8 bytes.
   * @param nLong
   * @return array size.
   */
  private static int calcArraySize(int nLong) {
    return nLong;
  }

  /**
   * Create an array.
   * @param size size
   * @return a new array
   */
  public static long[] arrayCreate(int size) {
    return POOL.getArray(calcArraySize(size));
  }

  /**
   * Replaces an array with another array. The replaced array is returned to the pool.
   * @param oldA old array
   * @param newA new array
   * @return new array
   */
  public static long[] arrayReplace(long[] oldA, long[] newA) {
    if (oldA != null) {
      POOL.offer(oldA);
    }
    return newA;
  }

  /**
   * Clones an array.
   * @param oldA old array
   * @return a copy or the input array
   */
  public static long[] arrayClone(long[] oldA) {
    long[] newA = arrayCreate(oldA.length);
    arraycopy(oldA, 0, newA, 0, oldA.length);
    return newA;
  }

  /**
   * Write the src array into the dst array at position dstPos. 
   * @param src source array
   * @param dst destination array
   * @param dstPos destination position
   */
  public static void writeArray(long[] src, long[] dst, int dstPos) {
    arraycopy(src, 0, dst, dstPos, src.length);
  }

  /**
   * Same a {@link #arraycopy(long[], int, long[], int, int)}. 
   * @param src source array
   * @param srcPos source position
   * @param dst destination array
   * @param dstPos destination position
   * @param length length
   */
  public static void writeArray(long[] src, int srcPos, long[] dst, int dstPos, int length) {
    arraycopy(src, srcPos, dst, dstPos, length);
  }

  /**
   * Reads data from srcPos in src[] into dst[]. 
   * @param src source array
   * @param srcPos source position
   * @param dst destination array
   */
  public static void readArray(long[] src, int srcPos, long[] dst) {
    arraycopy(src, srcPos, dst, 0, dst.length);
  }

  /**
   * Creates a new array with from copying oldA and inserting insertA at position pos.
   * The old array is returned to the pool. 
   * @param oldA old array
   * @param insertA array to insert
   * @param dstPos destination position
   * @return new array
   */
  public static long[] insertArray(long[] oldA, long[] insertA, int dstPos) {
    long[] ret = arrayCreate(oldA.length + insertA.length);
    arraycopy(oldA, 0, ret, 0, dstPos);
    arraycopy(insertA, 0, ret, dstPos, insertA.length);
    arraycopy(oldA, dstPos, ret, dstPos+insertA.length, oldA.length-dstPos);
    POOL.offer(oldA);
    return ret;
  }

  /**
   * Creates a new array with from copying oldA and removing 'length' entries at position pos.
   * The old array is returned to the pool. 
   * @param oldA old array
   * @param dstPos destination 
   * @param length length
   * @return new array
   */
  public static long[] arrayRemove(long[] oldA, int dstPos, int length) {
    long[] ret = arrayCreate(oldA.length - length);
    arraycopy(oldA, 0, ret, 0, dstPos);
    arraycopy(oldA, dstPos+length, ret, dstPos, ret.length-dstPos);
    POOL.offer(oldA);
    return ret;
  }

  /**
   * Same as System.arraycopy(), but uses a faster copy-by-loop approach for small arrays.
   * @param src source array
   * @param srcPos source position
   * @param dst destination array
   * @param dstPos destination position
   * @param len length
   */
  public static void arraycopy(long[] src, int srcPos, long[] dst, int dstPos, int len) {
    if (len < 10) {
      for (int i = 0; i < len; i++) {
        dst[dstPos+i] = src[srcPos+i]; 
      }
    } else {
      System.arraycopy(src, srcPos, dst, dstPos, len);
    }
  }

  /**
   * Writes a long array to a stream.
   * @param a array
   * @param out output stream
   * @throws IOException if writing fails
   */
  public static void write(long[] a, ObjectOutput out) throws IOException {
    out.writeInt(a.length);
    for (int i = 0; i < a.length; i++) {
      out.writeLong(a[i]);
    }
  }

  /**
   * Reads a long array from a stream.
   * @param in input stream
   * @return the long array.
   * @throws IOException if reading fails
   */
  public static long[] read(ObjectInput in) throws IOException {
    int size = in.readInt();
    long[] ret = POOL.getArray(size);
    for (int i = 0; i < size; i++) {
      ret[i] = in.readLong();
    }
    return ret;
  }

}
