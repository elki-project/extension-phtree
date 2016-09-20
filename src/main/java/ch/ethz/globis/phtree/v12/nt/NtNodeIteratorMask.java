package ch.ethz.globis.phtree.v12.nt;

import ch.ethz.globis.phtree.PersistenceProvider;
import ch.ethz.globis.phtree.v12.nt.NodeTreeV12.NtEntry12;

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
	private final PersistenceProvider store;

	/**
	 * 
	 */
	public NtNodeIteratorMask(PersistenceProvider store) {
		this.store = store;
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
	boolean increment(NtEntry12<T> result) {
		result.reset();
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
	private boolean readValue(int pin, long pos, NtEntry12<T> result) {
		byte ntSubCode = node.getNtSubCode(pin);
		if (NtNode.isNtSubEmpty(ntSubCode)) {
			return false;
		}
		
		prefix = node.localReadAndApplyReadPostfixAndHc(pin, pos, prefix);
		
		if (NtNode.isNtSubNode(ntSubCode)) {
			long mask = (-1L) << ((node.calcSubPostLen(ntSubCode)+1)*NtNode.MAX_DIM);
			if (((prefix | globalMinMask) & globalMaxMask & mask) != (prefix & mask)) {
				return false;
			}
			nextSubNode = (NtNode<T>) store.loadNode(node.getValueByPIN(pin));
		} else {
			if (((prefix | globalMinMask) & globalMaxMask) != prefix) {
				return false;
			}
			nextSubNode = null;
			node.getKdKeyByPIN(pin, result.getKdKey());
			Object v = node.getValueByPIN(pin);
			byte kdSubCode = node.getKdSubCode(pin);
			result.setValue(kdSubCode, ntSubCode, v == NodeTreeV12.NT_NULL ? null : (T)v );
		}
		result.setKey(prefix);
		return true;
	}

	private long getNextHCI(NtEntry12<T> result) {
		//Ideally we would switch between b-serch-HCI and incr-search depending on the expected
		//distance to the next value.
		long currentPos = next;
		do {
			if (currentPos == START) {
				//starting position
				currentPos = maskLower;
			} else {
				currentPos = NodeTreeV12.inc(currentPos, maskLower, maskUpper);
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

	private void getNext(NtEntry12<T> result) {
		if (useHcIncrementer) {
			next = getNextHCI(result);
		} else if (isHC) {
			getNextAHC(result);
		} else {
			getNextLHC(result);
		}
	}
	
	private void getNextAHC(NtEntry12<T> result) {
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
	
	private void getNextLHC(NtEntry12<T> result) {
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
