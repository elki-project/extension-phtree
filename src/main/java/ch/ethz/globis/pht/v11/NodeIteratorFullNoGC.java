package ch.ethz.globis.pht.v11;

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

import ch.ethz.globis.pht.PhFilter;
import ch.ethz.globis.pht.PhTreeHelper;
import ch.ethz.globis.pht.v11.PhTree11.NodeEntry;
import ch.ethz.globis.pht.v11.nt.NtIteratorMinMax;
import ch.ethz.globis.pht64kd.MaxKTreeI.NtEntry;



/**
 * This NodeIterator reuses existing instances, which may be easier on the Java GC.
 * 
 * 
 * @author ztilmann
 *
 * @param <T>
 */
public class NodeIteratorFullNoGC<T> {
	
	private static final long FINISHED = Long.MAX_VALUE; 
	
	private final int dims;
	private boolean isHC;
	private boolean isNI;
	private int postLen;
	private long next = -1;
	private Node node;
	private int currentOffsetKey;
	private NtIteratorMinMax<Object> ntIterator;
	private int nMaxEntries;
	private int nEntriesFound = 0;
	private int postEntryLenLHC;
	private final long[] valTemplate;
	private PhFilter checker;
	private final long maxPos;


	/**
	 * 
	 * @param dims
	 * @param valTemplate A null indicates that no values are to be extracted.
	 */
	public NodeIteratorFullNoGC(int dims, long[] valTemplate) {
		this.dims = dims;
		this.maxPos = (1L << dims) -1;
		this.valTemplate = valTemplate;
	}
	
	/**
	 * 
	 * @param node
	 * @param rangeMin The minimum value that any found value should have. If the found value is
	 *  lower, the search continues.
	 * @param rangeMax
	 * @param lower The minimum HC-Pos that a value should have.
	 * @param upper
	 * @param checker result verifier, can be null.
	 */
	private void reinit(Node node, PhFilter checker) {
		next = -1;
		nEntriesFound = 0;
		this.checker = checker;
	
		this.node = node;
		this.isHC = node.isAHC();
		this.isNI = node.isNT();
		this.postLen = node.getPostLen();
		nMaxEntries = node.getEntryCount();
		
		
		//Position of the current entry
		if (isNI) {
			if (ntIterator == null) {
				ntIterator = new NtIteratorMinMax<>(dims);
			}
			ntIterator.reset(node.ind(), 0, Long.MAX_VALUE);
		} else {
			currentOffsetKey = node.getBitPosIndex();
			postEntryLenLHC = Node.IK_WIDTH(dims)+dims*postLen;
		}
	}

	/**
	 * Advances the cursor. 
	 * @return TRUE iff a matching element was found.
	 */
	boolean increment(NodeEntry<T> result) {
		getNext(result);
		return next != FINISHED;
	}

	/**
	 * 
	 * @return False if the value does not match the range, otherwise true.
	 */
	@SuppressWarnings("unchecked")
	private boolean readValue(int posInNode, long hcPos, NodeEntry<T> result) {
		long[] key = result.getKey();
		Object v = node.getEntryPIN(posInNode, hcPos, valTemplate, key);
		if (v == null) {
			return false;
		}
		
		if (v instanceof Node) {
			result.setNodeKeepKey((Node) v);
		} else {
			if (checker != null && !checker.isValid(key)) {
				return false;
			}
			//ensure that 'node' is set to null
			result.setPost((T) v );
		}
		next = hcPos;
		
		return true;
	}

	@SuppressWarnings("unchecked")
	private boolean readValue(long pos, long[] kdKey, Object value, NodeEntry<T> result) {
		PhTreeHelper.applyHcPos(pos, postLen, valTemplate);
		if (value instanceof Node) {
			result.setNodeKeepKey((Node) value);
			node.getInfixOfSubNt(kdKey, valTemplate);
		} else {
			long[] resultKey = result.getKey();
			final long mask = (~0L)<<postLen;
			for (int i = 0; i < resultKey.length; i++) {
				resultKey[i] = (valTemplate[i] & mask) | kdKey[i];
			}

			if (checker != null && !checker.isValid(resultKey)) {
				return false;
			}

			//ensure that 'node' is set to null
			result.setPost((T) value);
		}
		return true;
	}


	private void getNext(NodeEntry<T> result) {
		if (isNI) {
			niFindNext(result);
			return;
		}

		if (isHC) {
			getNextAHC(result);
		} else {
			getNextLHC(result);
		}
	}
	
	private void getNextAHC(NodeEntry<T> result) {
		//while loop until 1 is found.
		long currentPos = next; 
		do {
			currentPos++;  //pos w/o bit-offset
			if (currentPos > maxPos) {
				next = FINISHED;
				break;
			}
		} while (!readValue((int) currentPos, currentPos, result));
	}
	
	private void getNextLHC(NodeEntry<T> result) {
		long currentPos;
		do {
			if (++nEntriesFound > nMaxEntries) {
				next = FINISHED;
				break;
			}
			currentPos = Bits.readArray(node.ba, currentOffsetKey, Node.IK_WIDTH(dims));
			currentOffsetKey += postEntryLenLHC;
			//check post-fix
		} while (!readValue(nEntriesFound-1, currentPos, result));
	}
	
	private void niFindNext(NodeEntry<T> result) {
		while (ntIterator.hasNext()) {
			NtEntry<Object> e = ntIterator.nextEntryReuse();
			next = e.key();
			if (e.value() instanceof Node) {
				Node sub = (Node) e.value();
				PhTreeHelper.applyHcPos(next, postLen, valTemplate);
				node.getInfixOfSubNt(e.getKdKey(), valTemplate);
				if (checker != null && !checker.isValid(sub.getPostLen()+1, valTemplate)) {
					continue;
				}
				result.setNodeKeepKey(sub);
			} else {
				if (!readValue(e.key(), e.getKdKey(), e.value(), result)) {
					continue;
				}
			}
			return;
		}
		next = FINISHED;
	}

	public Node node() {
		return node;
	}

	void init(Node node, PhFilter checker) {
		reinit(node, checker);
	}

}