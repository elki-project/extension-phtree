package ch.ethz.globis.phtree.v11.nt;

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

import ch.ethz.globis.pht64kd.MaxKTreeI.NtEntry;

/**
 * Iterator over a NodeTree.
 * 
 * This NodeIterator reuses existing instances, which may be easier on the Java GC.
 * 
 * @author ztilmann
 *
 * @param <T>
 */
public class NtNodeIteratorMask<T> {
	
	private static final long FINISHED = Long.MAX_VALUE; 
	private static final long START = -1; 
	
	private boolean isHC;
	private long next;
	private NtNode<T> nextSubNode;
	private NtNode<T> node;
	private int currentOffsetKey;
	private int nMaxEntry;
	private int nFound = 0;
	private int postEntryLenLHC;
	//The valTemplate contains the known prefix
	private long prefix;
	private long maskLower;
	private long maskUpper;
	private long globalMinMask;
	private long globalMaxMask;
	private boolean useHcIncrementer;

	/**
	 * 
	 */
	public NtNodeIteratorMask() {
		//
	}
	
	/**
	 * 
	 * @param node
	 * @param globalMinMask The minimum value that any found value should have. If the found value is
	 *  lower, the search continues.
	 * @param globalMaxMask
	 * @param prefix
	 */
	private void reinit(NtNode<T> node, long prefix) {
		this.prefix = prefix;
		next = START;
		nextSubNode = null;
		currentOffsetKey = 0;
		nFound = 0;
	
		this.node = node;
		this.isHC = node.isAHC();
		nMaxEntry = node.getEntryCount();
		//Position of the current entry
		currentOffsetKey = node.getBitPosIndex();
		if (!isHC) {
			//length of post-fix WITH key
			postEntryLenLHC = NtNode.IK_WIDTH(NtNode.MAX_DIM) + NtNode.MAX_DIM*node.getPostLen();
		}

		useHcIncrementer = false;

		if (NtNode.MAX_DIM > 3) {
			//LHC, NI, ...
			long maxHcAddr = ~((-1L)<<NtNode.MAX_DIM);
			int nSetFilterBits = Long.bitCount(maskLower | ((~maskUpper) & maxHcAddr));
			//nPossibleMatch = (2^k-x)
			long nPossibleMatch = 1L << (NtNode.MAX_DIM - nSetFilterBits);
			if (isHC) {
				//nPossibleMatch < 2^k?
				useHcIncrementer = nPossibleMatch < maxHcAddr;
			} else {
				int logNPost = Long.SIZE - Long.numberOfLeadingZeros(nMaxEntry) + 1;
				useHcIncrementer = nMaxEntry > nPossibleMatch*(double)logNPost; 
			}
		}
	}

	/**
	 * Advances the cursor. 
	 * @return TRUE iff a matching element was found.
	 */
	boolean increment(NtEntry<T> result) {
		getNext(result);
		return next != FINISHED;
	}

	/**
	 * Return whether the next value returned by next() is a sub-node or not.
	 * 
	 * @return True if the current value (returned by next()) is a sub-node, 
	 * otherwise false
	 */
	boolean isNextSub() {
		return nextSubNode != null;
	}

	/**
	 * 
	 * @return False if the value does not match the range, otherwise true.
	 */
	@SuppressWarnings("unchecked")
	private boolean readValue(int pin, long pos, NtEntry<T> result) {
		Object v = node.getValueByPIN(pin);
		
		if (v == null) {
			return false;
		}
		
		prefix = node.localReadAndApplyReadPostfixAndHc(pin, pos, prefix);
		
		if (v instanceof NtNode) {
			NtNode<T> sub = (NtNode<T>) v;
			long mask = (-1L) << ((sub.getPostLen()+1)*NtNode.MAX_DIM);
			if (((prefix | globalMinMask) & globalMaxMask & mask) != (prefix & mask)) {
				return false;
			}
			nextSubNode = sub;
		} else {
			if (((prefix | globalMinMask) & globalMaxMask) != prefix) {
				return false;
			}
			nextSubNode = null;
			node.getKdKeyByPIN(pin, result.getKdKey());
			result.setValue( v == NodeTreeV11.NT_NULL ? null : (T)v );
		}
		result.setKey(prefix);
		return true;
	}

	private long getNextHCI(NtEntry<T> result) {
		//Ideally we would switch between b-serch-HCI and incr-search depending on the expected
		//distance to the next value.
		long currentPos = next;
		do {
			if (currentPos == START) {
				//starting position
				currentPos = maskLower;
			} else {
				currentPos = NodeTreeV11.inc(currentPos, maskLower, maskUpper);
				if (currentPos <= maskLower) {
					return FINISHED;
				}
			}

			int pin = node.getPosition(currentPos, NtNode.MAX_DIM);
			if (pin >= 0 && readValue(pin, currentPos, result)) {
				return currentPos;
			}
		} while (true);
	}

	private void getNext(NtEntry<T> result) {
		if (useHcIncrementer) {
			next = getNextHCI(result);
		} else if (isHC) {
			getNextAHC(result);
		} else {
			getNextLHC(result);
		}
	}
	
	private void getNextAHC(NtEntry<T> result) {
		long currentPos = next == START ? maskLower : next+1; 
		while (currentPos <= maskUpper) {
			//check HC-pos
			if (checkHcPos(currentPos) && readValue((int)currentPos, currentPos, result)) {
				next = currentPos;
				return;
			}
			currentPos++;  //pos w/o bit-offset
		}
		next = FINISHED;
	}
	
	private void getNextLHC(NtEntry<T> result) {
		while (++nFound <= nMaxEntry) {
			long currentPos = 
					Bits.readArray(node.ba, currentOffsetKey, NtNode.IK_WIDTH(NtNode.MAX_DIM));
			currentOffsetKey += postEntryLenLHC;
			//check HC-pos
			if (checkHcPos(currentPos)) {
				//check post-fix
				if (readValue(nFound-1, currentPos, result)) {
					next = currentPos;
					return;
				}
			} else {
				if (currentPos > maskUpper) {
					break;
				}
			}
		}
		next = FINISHED;
	}
	

	private boolean checkHcPos(long pos) {
		return ((pos | maskLower) & maskUpper) == pos;
	}

	public NtNode<T> getCurrentSubNode() {
		return nextSubNode;
	}

	public NtNode<T> node() {
		return node;
	}

	/**
	 * 
	 * @param globalMinMask
	 * @param globalMaxMask
	 * @param prefix
	 * @param postLen
	 */
	private void calcLimits(long globalMinMask, long globalMaxMask) {
		//create limits for the local node. there is a lower and an upper limit. Each limit
		//consists of a series of DIM bit, one for each dimension.
		//For the lower limit, a '1' indicates that the 'lower' half of this dimension does 
		//not need to be queried.
		//For the upper limit, a '0' indicates that the 'higher' half does not need to be 
		//queried.
		//
		//              ||  lowerLimit=0 || lowerLimit=1 || upperLimit = 0 || upperLimit = 1
		// =============||===================================================================
		// query lower  ||     YES             NO
		// ============ || ==================================================================
		// query higher ||                                     NO               YES
		//
		int postLen = node.getPostLen();
		this.maskLower = NtNode.pos2LocalPos(globalMinMask, postLen);
		this.maskUpper = NtNode.pos2LocalPos(globalMaxMask, postLen);
		this.globalMinMask = globalMinMask;
		this.globalMaxMask = globalMaxMask;
	}
	
	boolean adjustMinMax(long globalMinMask, long globalMaxMask) {
		calcLimits(globalMinMask, globalMaxMask);

		if (next >= this.maskUpper) {
			//we already fully traversed this node
			return false;
		}

		if (next < this.maskLower) {
			//we do NOT set currentOffsetKey here:
			//It is only used by LHC, and for LHC we simply keep iterating with checkHcPos()
			//until we hit the next valid entry. There are only 2^6=64 entries...
			//currentOffsetKey = node.getBitPosIndex();
			next = START;
			return true;
		}
			
		//TODO switch to HCI if too few quadrants remain???
		if (useHcIncrementer && !checkHcPos(next)) {
			//Adjust pos in HCI mode such that it works for the next inc()
			//At this point, next is >= maskLower
			long pos = next-1;
			//TODO the following is a bit brute force. But, it is still fast and should
			//     rarely happen, i.e. only for:
			//        (HCI mode) && (lowerLimit changes) && (newLowerLimit < next)
			//After the following, pos==START or pos==(a valid entry such that inc(pos) is
			//the next valid entry after the original `next`)
			while (!checkHcPos(pos) && pos > START) {
				//normal iteration to ensure we to get a valid POS for HCI-inc()
				pos--;
			}
			next = pos;
		}
		return true;
	}

	void init(long globalMinMask, long globalMaxMask, long valTemplate, NtNode<T> node) {
		this.node = node; //for calcLimits
		calcLimits(globalMinMask, globalMaxMask);
		reinit(node, valTemplate);
	}

	boolean verifyMinMax(long globalMinMask, long globalMaxMask) {
		long mask = (-1L) << node.getPostLen()+1;
		if ((prefix | ~mask) < globalMinMask ||
				(prefix & mask) > globalMaxMask) {
			return false;
		}
		return true;
	}

	public long getPrefix() {
		return prefix;
	}
}
