/*
 * Copyright 2011-2016 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 */
package ch.ethz.globis.pht64kd;

public interface NodeTree64<T> extends MaxKTreeI {

	T put(long key, long[] kdKey, T value);

	boolean putB(long key, long[] kdKey);

	boolean contains(long key, long[] outKdKey);

	T get(long key, long[] outKdKey);

	T remove(long key);

	boolean removeB(long key);

	String toStringTree();

	PhIterator64<T> queryWithMask(long minMask, long maxMask);

	PhIterator64<T> query(long min, long max);

	PhIterator64<T> iterator();

	boolean checkTree();

}