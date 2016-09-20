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
public class NtNodeIteratorMinMax<T> {
	
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
	private long localMin;
	private long localMax;
	private long globalMin;
	private long globalMax;
	private final PersistenceProvider pp;

	/**
	 */
	public NtNodeIteratorMinMax(PersistenceProvider pp) {
		this.pp = pp;
	}
	
	/**
	 * 
	 * @param node
	 * @param globalMinMask The minimum value that any found value should have. If the found value is
	 *  lower, the search continues.
	 * @param globalMaxMask
	 * @param prefix
	 */
	private void reinit(NtNode<T> node, long prefix, long globalMin, long globalMax) {
		this.prefix = prefix;
		this.globalMin = globalMin;
		this.globalMax = globalMax;
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

	long getCurrentPos() {
		return next;
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
			if (prefix < (globalMin & mask) || (prefix & mask) > globalMax) {
				return false;
			}
			nextSubNode = (NtNode<T>) pp.loadNode(node.getValueByPIN(pin));
		} else {
			if (prefix < globalMin || prefix > globalMax) {
				return false;
			}
			nextSubNode = null;
			node.getKdKeyByPIN(pin, result.getKdKey());
			Object v = node.getValueByPIN(pin);
			byte kdSubCode = node.getKdSubCode(pin);
			result.setValue(kdSubCode, ntSubCode, v == NodeTreeV12.NT_NULL ? null : (T)v);
		}
		result.setKey(prefix);
		return true;
	}

	private void getNext(NtEntry12<T> result) {
		if (isHC) {
			getNextAHC(result);
		} else {
			getNextLHC(result);
		}
	}
	
	private void getNextAHC(NtEntry12<T> result) {
		long currentPos = next == START ? localMin : next+1; 
		while (currentPos <= localMax) {
			if (readValue((int)currentPos, currentPos, result)) {
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
			if (currentPos <= localMax) {
				//check post-fix
				if (readValue(nFound-1, currentPos, result)) {
					next = currentPos;
					return;
				}
			} else {
				break;
			}
		}
		next = FINISHED;
	}
	

	public NtNode<T> getCurrentSubNode() {
		return nextSubNode;
	}

	/**
	 * 
	 * @param globalMin
	 * @param globalMax
	 * @param prefix
	 * @param postLen
	 * @return 'false' if the new upper limit is smaller than the current HC-pos.
	 */
	boolean calcLimits(long globalMin, long globalMax, long prefix, boolean isNegativeRoot) {
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
		if (isNegativeRoot) {
			//TODO this doesn't really work. Is there a better way that also keeps the ordering?
//			this.localMin = NtNode.pos2LocalPosNegative(globalMin, postLen);
//			this.localMax = NtNode.pos2LocalPosNegative(globalMax, postLen);
			this.localMin = 0;
			this.localMax = ~((-1L) << NtNode.MAX_DIM);
		} else {
			if ((globalMin ^ prefix) >> postLen == 0) {
				this.localMin = NtNode.pos2LocalPos(globalMin, postLen);
			} else {
				this.localMin = 0;
			}
			if ((globalMax ^ prefix) >> postLen == 0) {
				this.localMax = NtNode.pos2LocalPos(globalMax, postLen);
			} else {
				this.localMax = ~((-1L) << NtNode.MAX_DIM);
			}
		}
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		if (localMin > localMax) {
			throw new IllegalStateException("localMin=" + localMin + " / " + localMax);
		}
		return true;
	}
	
	void init(long globalMin, long globalMax, long valTemplate, NtNode<T> node, 
			boolean isNegativeRoot) {
		this.node = node; //for calcLimits
		calcLimits(globalMin, globalMax, valTemplate, isNegativeRoot);
		reinit(node, valTemplate, globalMin, globalMax);
	}

	public long getPrefix() {
		return prefix;
	}
}
