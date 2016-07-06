package ch.ethz.globis.pht64kd;

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

import ch.ethz.globis.pht.v11.nt.NodeTreeV11;

public class MaxKTree64<T> implements MaxKTreeI {

	private final long[] dummy;
	private final NodeTreeV11<T> tree;
	
	private MaxKTree64() {
		this.tree = NodeTreeV11.create(64);
		this.dummy = new long[getKeyBitWidth()];
	}
	
	private MaxKTree64(int keyWidth) {
		this.tree = NodeTreeV11.create(keyWidth);
		this.dummy = new long[getKeyBitWidth()];
	}
	
	public static <T> MaxKTree64<T> create() {
		return new MaxKTree64<>();
	}
	
	public static <T> MaxKTree64<T> create(int keyWidth) {
		return new MaxKTree64<>(keyWidth);
	}
	
	public T put(long key, T value) {
		return tree.put(key, dummy, value);
	}
	
	public T putKD(long key, long[] kdKey, T value) {
		return tree.put(key, kdKey, value);
	}
	
	public boolean contains(long key) {
		return tree.contains(key, dummy);
	}
	
	public T get(long key) {
		return tree.get(key, dummy);
	}
	
	public T getKd(long key, long[] kdKeyOut) {
		return tree.get(key, kdKeyOut);
	}
	
	public T remove(long key) {
		return tree.remove(key);
	}
	
	public boolean delete(long key) {
		return tree.remove(key) != null;
	}
	
	public String toStringTree() {
		return tree.toStringTree();
	}
	
	public PhIterator64<T> queryWithMask(long minMask, long maxMask) {
		return tree.queryWithMask(minMask, maxMask);
	}
	
	public PhIterator64<T> query(long min, long max) {
		return tree.query(min, max);
	}
	
	public PhIterator64<T> iterator() {
		return tree.iterator();
	}
	
	public boolean checkTree() {
		return tree.checkTree();
	}

	@Override
	public int size() {
		return tree.size();
	}

	@Override
	public int getKeyBitWidth() {
		return tree.getKeyBitWidth();
	}

	@Override
	public Object getRoot() {
		return tree.getRoot();
	}
	
}