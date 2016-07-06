package ch.ethz.globis.pht.v11.nt;

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

import java.util.NoSuchElementException;

import ch.ethz.globis.pht64kd.MaxKTreeI;
import ch.ethz.globis.pht64kd.MaxKTreeI.NtEntry;
import ch.ethz.globis.pht64kd.MaxKTreeI.PhIterator64;


/**
 * Iterator for the individual nodes in a NodeTree.
 * 
 * This PhIterator reuses PhEntry objects to avoid unnecessary creation of objects.
 * Calls to next() and nextKey() will result in creation of new PhEntry and long[] objects
 * respectively to maintain expected behaviour. However, the nextEntryUnstable() method
 * returns the internal PhEntry without creating any new objects. The returned PhEntry and long[]
 * are valid until the next call to nextXXX().
 * 
 * @author ztilmann
 *
 * @param <T>
 */
public final class NtIteratorMinMax<T> implements PhIterator64<T> {

	private class PhIteratorStack {
		private final NtNodeIteratorMinMax<T>[] stack;
		private int size = 0;
		
		@SuppressWarnings("unchecked")
		public PhIteratorStack(int depth) {
			stack = new NtNodeIteratorMinMax[depth];
		}

		public boolean isEmpty() {
			return size == 0;
		}

		public NtNodeIteratorMinMax<T> prepareAndPush(NtNode<T> node, long currentPrefix) {
			NtNodeIteratorMinMax<T> ni = stack[size++];
			if (ni == null)  {
				ni = new NtNodeIteratorMinMax<>();
				stack[size-1] = ni;
			}
			ni.init(min, max, currentPrefix, node, isRootNegative && size==1);
			return ni;
		}

		public NtNodeIteratorMinMax<T> peek() {
			return stack[size-1];
		}

		public NtNodeIteratorMinMax<T> pop() {
			return stack[--size];
		}
	}

	private final PhIteratorStack stack;
	private long min;
	private long max;
	private final boolean isRootNegative;
	
	private final NtEntry<T> resultBuf1;
	private final NtEntry<T> resultBuf2;
	private boolean isFreeBuf1;
	boolean isFinished = false;
	
	public NtIteratorMinMax(int keyBitWidth) {
		this.stack = new PhIteratorStack(NtNode.calcTreeHeight(keyBitWidth));
		this.isRootNegative = keyBitWidth == 64;
		this.resultBuf1 = new NtEntry<>(0, new long[keyBitWidth], null);
		this.resultBuf2 = new NtEntry<>(0, new long[keyBitWidth], null);
	}	
		
	@SuppressWarnings("unchecked")
	@Override
	public void reset(MaxKTreeI tree, long min, long max) {
		reset((NtNode<T>)tree.getRoot(), min, max);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void reset(MaxKTreeI tree) {
		reset((NtNode<T>)tree.getRoot(), Long.MIN_VALUE, Long.MAX_VALUE);
	}
	
	public PhIterator64<T> reset(NtNode<T> root, long min, long max) {	
		this.min = min;
		this.max = max;
		this.stack.size = 0;
		this.isFinished = false;
		
		if (root == null) {
			//empty index
			isFinished = true;
			return this;
		}
		
		stack.prepareAndPush(root, 0);
		findNextElement();
		return this;
	}

	private void findNextElement() {
		NtEntry<T> result = isFreeBuf1 ? resultBuf1 : resultBuf2; 
		while (!stack.isEmpty()) {
			NtNodeIteratorMinMax<T> p = stack.peek();
			while (p.increment(result)) {
				if (p.isNextSub()) {
					p = stack.prepareAndPush(p.getCurrentSubNode(), p.getPrefix());
					continue;
				} else {
					isFreeBuf1 = !isFreeBuf1;
					return;
				}
			}
			// no matching (more) elements found
			stack.pop();
		}
		//finished
		isFinished = true;
	}
	
	@Override
	public long nextKey() {
		return nextEntryReuse().getKey();
	}

	@Override
	public long[] nextKdKey() {
		long[] key = nextEntryReuse().getKdKey();
		long[] ret = new long[key.length];
		System.arraycopy(key, 0, ret, 0, key.length);
		return ret;
	}

	@Override
	public T nextValue() {
		return nextEntryReuse().getValue();
	}

	@Override
	public boolean hasNext() {
		return !isFinished;
	}

	@Override
	@Deprecated
	public NtEntry<T> nextEntry() {
		return new NtEntry<>(nextEntryReuse());
	}
	
	@Override
	public T next() {
		return nextEntryReuse().getValue();
	}

	/**
	 * Special 'next' method that avoids creating new objects internally by reusing Entry objects.
	 * Advantage: Should completely avoid any GC effort.
	 * Disadvantage: Returned PhEntries are not stable and are only valid until the
	 * next call to next(). After that they may change state.
	 * @return The next entry
	 */
	@Override
	public NtEntry<T> nextEntryReuse() {
		if (isFinished) {
			throw new NoSuchElementException();
		}
		NtEntry<T> ret = isFreeBuf1 ? resultBuf2 : resultBuf1;
		findNextElement();
		return ret;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
}