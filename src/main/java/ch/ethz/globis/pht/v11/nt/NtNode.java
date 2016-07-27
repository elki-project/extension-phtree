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

import ch.ethz.globis.pht.util.Refs;
import ch.ethz.globis.pht.util.RefsLong;


/**
 * Node of the nested PH-tree.
 * 
 * @author ztilmann
 *
 * @param <T>
 */
public class NtNode<T> {
	
	/**
	 * All nodes have MAX_DIM as DIM. 
	 * This may not be correct for all sub-nodes, but it does no harm and simplifies
	 * algorithms that iterator over the tree (avoid switching dimensions, etc).
	 * 
	 * WARNING using MAX_DIM > 15 will fail because entryCnt is of type 'short'! 
	 */
	public static final int MAX_DIM = 6;
	private static final long MAX_DIM_MASK = ~((-1L) << MAX_DIM);
	
	//size of references in bytes
	private static final int REF_BITS = 4*8;

	static final int HC_BITS = 0;  //number of bits required for storing current (HC)-representation
	static final int INN_HC_WIDTH = 0; //Index-NotNull: width of not-null flag for post/infix-hc

	private Object[] values;
	private short entryCnt = 0;
	long[] ba = null;
	private long[] kdKeys = null;
	private boolean isAHC = false;
	private byte postLen = 0;

	static final int IK_WIDTH(int dims) { return dims; }; //post index key width 
	static final int KD_WIDTH(int kdDims) { return kdDims * 64; }; //post index key width 

	protected NtNode(NtNode<T> original) {
        this.values = Refs.arrayClone(original.values);
        this.entryCnt = original.entryCnt;
        this.isAHC = original.isAHC;
        this.postLen = original.postLen;
        this.ba = Bits.arrayClone(original.ba);
        this.kdKeys = RefsLong.arrayClone(original.kdKeys);
    }

	private NtNode() {
		//nothing
	}

	static <T> NtNode<T> createNode(NtNode<T> original) {
		return new NtNode<>(original);
	}

	@SuppressWarnings("unchecked")
	public static <T> NtNode<T> createRoot(int keyBitWidth) {
		NtNode<T> n = (NtNode<T>) NtNodePool.getNode();
		n.initNode(calcTreeHeight(keyBitWidth) - 1, keyBitWidth);
		return n;
	}
	
	@SuppressWarnings("unchecked")
	static <T> NtNode<T> createNode(int postLen, int keyBitWidth) {
		NtNode<T> n = (NtNode<T>) NtNodePool.getNode();
		n.initNode(postLen, keyBitWidth);
		return n;
	}
	
	/**
	 * To be used by the NtNodePool only.
	 * @return returns an empty node.
	 */
	static <T> NtNode<T> createEmptyNode() {
		return new NtNode<>();
	}
	
	void initNode(int postLen, int keyBitWidth) {
		this.postLen = (byte) postLen;
		this.entryCnt = 0;
		this.isAHC = false;
		int size = calcArraySizeTotalBits(2, MAX_DIM);
		this.ba = Bits.arrayCreate(size);
		this.kdKeys = RefsLong.arrayCreate(calcArraySizeTotalLongs(2, MAX_DIM, keyBitWidth));
		this.values = Refs.arrayCreate(2);
	}
	
	void discardNode() {
		Bits.arrayReplace(ba, null);
		RefsLong.arrayReplace(kdKeys, null);
		Refs.arrayReplace(values, null);
		NtNodePool.offer(this);
	}
	
	int calcArraySizeTotalBits(int entryCount, final int dims) {
		int nBits = getBitPosIndex();
		//post-fixes
		if (isAHC()) {
			//hyper-cube
			nBits += (INN_HC_WIDTH + dims * postLen) * (1 << dims);
		} else {
			//hc-pos index
			nBits += entryCount * (IK_WIDTH(dims) + dims * postLen);
		}
		return nBits;
	}

	int calcArraySizeTotalLongs(int entryCount, final int dims, final int kdDims) {
		return isAHC() ? 
				//hyper-cube
				kdDims * (1 << dims)
				:
				//LHC index
				kdDims * entryCount;
	}

	//
	//  NtNode stuff
	//
	
	static int calcTreeHeight(int k) {
		return (k-1) / MAX_DIM + 1;
	}
	
	/**
	 * This conversion has to be made such that the z-order of all entries is
	 * preserved.
	 * Therefore, we have to keep higher-order bits in the top-level nodes of the
	 * tree. As a downside, it requires additional effort (TODO) to have 2^MAX_DIM
	 * children in the root node if the dimension is not a multiple of MAX_DIM.
	 * 
	 * @param hcPos
	 * @param postLen
	 * @return The local hcPos / z-value.
	 */
	static long pos2LocalPos(long hcPos, int postLen) {
		return (hcPos >>> (postLen*MAX_DIM)) & MAX_DIM_MASK;
	}

	static long pos2LocalPosNegative(long hcPos, int postLen) {
		return hcPos >> (postLen*MAX_DIM);
	}
	
    long localReadInfix(int pin, long localHcPos) {
		int infixBits = getPostLen() * MAX_DIM;
		int infixPos = pinToOffsBitsData(pin, localHcPos, MAX_DIM); 
		return Bits.readArray(ba, infixPos, infixBits);
		//TODO erase lower bits?
	}

    long localReadPostfix(int pin, long localHcPos) {
		int postBits = getPostLen() * MAX_DIM;
		int postPos = pinToOffsBitsData(pin, localHcPos, MAX_DIM); 
		return Bits.readArray(ba, postPos, postBits);
	}

    long localReadAndApplyReadPostfixAndHc(int pin, long localHcPos, long prefix) {
		int postBits = getPostLen() * MAX_DIM;
		int postPos = pinToOffsBitsData(pin, localHcPos, MAX_DIM); 
		long postFix = Bits.readArray(ba, postPos, postBits);
 		long mask = (-1L) << postBits; //  = 111100000000
 		mask <<= MAX_DIM;
     	return (prefix & mask) | (localHcPos << postBits) | postFix;
     }

    long localReadKey(int pin) {
    	if (isAHC()) {
    		return pin;
    	}
		int keyPos = pinToOffsBitsLHC(pin, getBitPosIndex(), MAX_DIM); 
		return Bits.readArray(ba, keyPos, MAX_DIM);
	}

    void localAddEntry(long localHcPos, long postFix, long[] kdKey, Object value) {
    	int pin = getPosition(localHcPos, MAX_DIM);
    	localAddEntryPIN(pin, localHcPos, postFix, kdKey, value);
    }
    
    void localAddEntryPIN(int pin, long localHcPos, long postFix, long[] key, Object value) {
    	addEntryPIN(localHcPos, pin, postFix, key, value, MAX_DIM);
    }
    
    void localReplaceEntryWithSub(int pin, long localHcPos, long hcPos, NtNode<T> newSub) {
    	//let's write the whole postfix...
		int totalInfixLen = getPostLen() * MAX_DIM;
		int infixPos = pinToOffsBitsData(pin, localHcPos, MAX_DIM);
		Bits.writeArray(ba, infixPos, totalInfixLen, hcPos);
		
    	replaceValueSub(pin, newSub);
    }
    
    Object localReplaceEntry(int pin, long[] kdKey, Object newValue) {
    	return replaceEntry(pin, kdKey, newValue);
    }

	/**
	 * Replace an value in a node without modifying the postfix or infix.
	 */
    Object localReplaceValue(int pin, Object newValue) {
		Object ret = values[pin];
		values[pin] = newValue;
		return ret;
    }

	
    /**
     * Calculates the number of conflicting bits, consisting of the most significant bit
     * and all bit 'right'of it (all less significant bits).
     * @param v1
     * @param v2
     * @param mask Mask that indicates which bits to check. Only bits where mask=1 are checked.
     * @return Number of conflicting bits or 0 if none.
     */
    private static final int getMaxConflictingBitsWithMask(long v1, long v2, long mask) {
        //write all differences to x, we just check x afterwards
        long x = v1 ^ v2;
        x &= mask;
        return Long.SIZE - Long.numberOfLeadingZeros(x);
    }

	static int getConflictingLevels(long key, long infix, 
			int parentPostLen, int subPostLen) {
		int subInfixLen = parentPostLen-subPostLen-1;
		if (subInfixLen == 0) {
			return 0;
		}
		long mask = ~((-1L)<<(subInfixLen*MAX_DIM)); // e.g. (0-->0), (1-->1), (8-->127=0x01111111)
		int maskOffset = (subPostLen+1) * MAX_DIM;
		mask = maskOffset==64 ? 0 : mask<< maskOffset; //last bit is stored in bool-array
		return getMaxConflictingLevelsWithMask(key, infix, mask);
	}

    /**
     * Calculates the number of conflicting tre-level, consisting of the most significant level
     * and all levels 'below' it (all less significant levels).
     * @param v1
     * @param v2
     * @param mask Mask that indicates which bits to check. Only bits where mask=1 are checked.
     * @return Number of conflicting bits or 0 if none.
     */
    static final int getMaxConflictingLevelsWithMask(long v1, long v2, long mask) {
        int confBits = getMaxConflictingBitsWithMask(v1, v2, mask);
        return (confBits + MAX_DIM -1) / MAX_DIM;
    }

    static long applyHcPos(long localHcPos, int postLen, long prefix) {
		long mask = ~((-1L) << MAX_DIM); // =000000001111
		int postLen2 = postLen*MAX_DIM;
		mask = ~(mask << postLen2); //  = 111100001111
    	return (prefix & mask) | (localHcPos << postLen2);
    }
    
    long applyHcPos(long localHcPos, long prefix) {
		long mask = ~((-1L) << MAX_DIM); // =000000001111
		int postLen2 = postLen*MAX_DIM;
		mask = ~(mask << postLen2); //  = 111100001111
    	return (prefix & mask) | (localHcPos << postLen2);
    }
    
    static long applyPostFix(long postFix, int postLen, long prefix) {
		int postLen2 = postLen*MAX_DIM;
		long mask = ~((-1L) << postLen2); //  = 111100000000
    	return (prefix & mask) | postFix;
    }
    
    static long applyHcAndPostFix(long localHcPos, long postFix, int postLen, long prefix) {
 		int postLen2 = postLen*MAX_DIM;
 		long mask = ~((-1L) << postLen2); //  = 111100000000
 		mask <<= MAX_DIM;
     	return (prefix & mask) | (localHcPos << postLen2) | postFix;
     }
	
	
	//
	// Normal Node stuff
	//
	

	/**
	 * @param posInNode
	 * @return The sub node or null.
	 */
	Object getValueByPIN(int posInNode) {
		return values[posInNode];
	}


	/**
	 * @param posInNode
	 * @param outKey Result container.
	 * @return The sub node or null.
	 */
	Object getEntryByPIN(int posInNode, long[] outKey) {
	  RefsLong.readArray(kdKeys, pinToKdPos(posInNode, outKey.length), outKey);
		return values[posInNode];
	}


	/**
	 * @param posInNode
	 * @param outKey Result container.
	 */
	void getKdKeyByPIN(int posInNode, long[] outKey) {
	  RefsLong.readArray(kdKeys, pinToKdPos(posInNode, outKey.length), outKey);
	}


	private boolean shouldSwitchToAHC(int entryCount, int dims, int kdDims) {
		return useAHC(entryCount, dims, kdDims);
	}
	
	private boolean shouldSwitchToLHC(int entryCount, int dims, int kdDims) {
		return !useAHC(entryCount+2, dims, kdDims);
	}
	
	private boolean useAHC(int entryCount, int dims, int kdDims) {
		//calc post mode.
		//+1 bit for null/not-null flag
		long sizeAHC = (dims * postLen + INN_HC_WIDTH + REF_BITS + KD_WIDTH(kdDims)) * (1L << dims); 
		//+DIM because every index entry needs DIM bits
		long sizeLHC = (dims * postLen + IK_WIDTH(dims) + REF_BITS + KD_WIDTH(kdDims)) 
				* (long)entryCount;
		//use 1.5 as bias
		return NodeTreeV11.AHC_ENABLED && (dims<=31) && (sizeLHC*1.5 >= sizeAHC);
	}

	void replacePost(int pin, long hcPos, long newKey, int dims) {
		int offs = pinToOffsBitsData(pin, hcPos, dims);
		Bits.writeArray(ba, offs, postLen*dims, newKey);
	}

	/**
	 * Replace an value in a node without modifying the postfix or infix.
	 */
	void replaceValueSub(int pin, NtNode<T> sub) {
		values[pin] = sub;
	}
	
	/**
	 * Replace an value in a node without modifying the postfix or infix.
	 */
	Object replaceEntry(int pin, long[] kdKey, Object val) {
		Object ret = values[pin];
		RefsLong.writeArray(kdKey, kdKeys, pinToKdPos(pin, kdKey.length));
		values[pin] = val;
		return ret;
	}

	void readKdKeyPIN(int pin, long[] outKdKey) {
	  RefsLong.readArray(kdKeys, pin*outKdKey.length, outKdKey);
	}
	
	boolean readKdKeyAndCheck(int pin, long[] keyToMatch, long mask) {
		int pos = pin*keyToMatch.length;
		for (int i = 0; i < keyToMatch.length; i++) {
			if (((kdKeys[pos++] ^ keyToMatch[i]) & mask) != 0) {
				return false;
			}
		}
		return true;
	}
	
	private void switchLhcToAhcAndGrow(int oldEntryCount, int dims, int kdDims) {
		int posOfIndex = getBitPosIndex();
		int posOfData = posToOffsBitsDataAHC(0, posOfIndex, dims);
		setAHC( true );
		long[] bia2 = Bits.arrayCreate(calcArraySizeTotalBits(oldEntryCount+1, dims));
		long[] kdKeys2 = 
		    RefsLong.arrayCreate(calcArraySizeTotalLongs(oldEntryCount+1, dims, kdDims));
		Object [] v2 = Refs.arrayCreate(1<<dims);
		//Copy only bits that are relevant. Otherwise we might mess up the not-null table!
		Bits.copyBitsLeft(ba, 0, bia2, 0, posOfIndex);
		int postLenTotal = dims*postLen; 
		for (int i = 0; i < oldEntryCount; i++) {
			int entryPosLHC = posOfIndex + i*(IK_WIDTH(dims)+postLenTotal);
			int p2 = (int)Bits.readArray(ba, entryPosLHC, IK_WIDTH(dims));
			Bits.copyBitsLeft(ba, entryPosLHC+IK_WIDTH(dims),
					bia2, posOfData + postLenTotal*p2, 
					postLenTotal);
			int kdPos1 = posToKdPosLHC(i, kdDims);
			int kdPos2 = posToKdPosAHC(p2, kdDims);
			RefsLong.writeArray(kdKeys, kdPos1, kdKeys2, kdPos2, kdDims);
			v2[p2] = values[i];
		}
		ba = Bits.arrayReplace(ba, bia2);
		kdKeys = RefsLong.arrayReplace(kdKeys, kdKeys2);
		values = Refs.arrayReplace(values, v2);
	}
	
	
	private Object switchAhcToLhcAndShrink(int oldEntryCount, int dims, int kdDims, 
			long hcPosToRemove) {
		Object oldEntry = null;
		setAHC( false );
		long[] bia2 = Bits.arrayCreate(calcArraySizeTotalBits(oldEntryCount-1, dims));
		long[] kdKeys2 = RefsLong.arrayCreate(calcArraySizeTotalLongs(oldEntryCount-1, dims, kdDims));
		Object[] v2 = Refs.arrayCreate(oldEntryCount-1);
		int oldOffsIndex = getBitPosIndex();
		int oldOffsData = oldOffsIndex + (1<<dims)*INN_HC_WIDTH;
		//Copy only bits that are relevant. Otherwise we might mess up the not-null table!
		Bits.copyBitsLeft(ba, 0, bia2, 0, oldOffsIndex);
		int postLenTotal = dims*postLen;
		int n=0;
		for (int i = 0; i < (1L<<dims); i++) {
			if (i == hcPosToRemove) {
				//skip the item that should be deleted.
				oldEntry = values[i];
				continue;
			}
			if (values[i] != null) {
				v2[n] = values[i];
				int entryPosLHC = oldOffsIndex + n*(IK_WIDTH(dims)+postLenTotal);
				Bits.writeArray(bia2, entryPosLHC, IK_WIDTH(dims), i);
				Bits.copyBitsLeft(
						ba, oldOffsData + postLenTotal*i, 
						bia2, entryPosLHC + IK_WIDTH(dims),
						postLenTotal);
				int kdPos1 = posToKdPosAHC(i, kdDims);
				int kdPos2 = posToKdPosLHC(n, kdDims);
				RefsLong.writeArray(kdKeys, kdPos1, kdKeys2, kdPos2, kdDims);
				n++;
			}
		}
		ba = Bits.arrayReplace(ba, bia2);
		kdKeys = RefsLong.arrayReplace(kdKeys, kdKeys2);
		values = Refs.arrayReplace(values, v2);
		return oldEntry;
	}
	
	
	/**
	 * 
	 * @param hcPos
	 * @param pin position in node: ==hcPos for AHC or pos in array for LHC
	 * @param key
	 */
	void addEntryPIN(long hcPos, int negPin, long key, long[] kdKey, Object value, int dims) {
		final int bufEntryCnt = getEntryCount();
		final int kdDims = kdKey.length;
		//decide here whether to use hyper-cube or linear representation
		//--> Initially, the linear version is always smaller, because the cube has at least
		//    two entries, even for a single dimension. (unless DIM > 2*REF=2*32 bit 
		//    For one dimension, both need one additional bit to indicate either
		//    null/not-null (hypercube, actually two bit) or to indicate the index. 

		//switch representation (HC <-> Linear)?
		if (!isAHC() && shouldSwitchToAHC(bufEntryCnt + 1, dims, kdKey.length)) {
			switchLhcToAhcAndGrow(bufEntryCnt, dims, kdKey.length);
		}

		incEntryCount();

		int offsIndex = getBitPosIndex();
		if (isAHC()) {
			//hyper-cube
			int offsPostKey = posToOffsBitsDataAHC(hcPos, offsIndex, dims);
			Bits.writeArray(ba, offsPostKey, postLen*dims, key);
			int kdPos = posToKdPosAHC(hcPos, kdDims);
			RefsLong.writeArray(kdKey, kdKeys, kdPos);
			values[(int) hcPos] = value;
		} else {
			//get position
			int pin = -(negPin+1);

			//resize array
			ba = Bits.arrayEnsureSize(ba, calcArraySizeTotalBits(bufEntryCnt+1, dims));
			long[] ia = ba;
			int offs = pinToOffsBitsLHC(pin, offsIndex, dims);
			Bits.insertBits(ia, offs, IK_WIDTH(dims) + dims*postLen);
			//insert key
			Bits.writeArray(ia, offs, IK_WIDTH(dims), hcPos);
			//insert kdKey
			int kdPos = posToKdPosLHC(pin, kdDims);
			kdKeys = RefsLong.insertArray(kdKeys, kdKey, kdPos);
			//insert value:
			offs += IK_WIDTH(dims);
			Bits.writeArray(ia, offs, postLen*dims, key);
      values = Refs.insertSpaceAtPos(values, pin, bufEntryCnt+1);
      values[pin] = value;
		}
	}

	void getPostPIN(int posInNode, long hcPos, long[] key) {
		long[] ia = ba;
		int offs = pinToOffsBitsData(posInNode, hcPos, key.length);
		final long mask = (~0L)<<postLen;
		for (int i = 0; i < key.length; i++) {
			key[i] &= mask;
			key[i] |= Bits.readArray(ia, offs, postLen);
			offs += postLen;
		}
	}

	Object removeValue(long hcPos, int posInNode, final int kdDims, final int dims) {
		final int bufEntryCnt = getEntryCount();

		//switch representation (HC <-> Linear)?
		if (isAHC() && shouldSwitchToLHC(bufEntryCnt, dims, kdDims)) {
			//revert to linearized representation, if applicable
			Object oldVal = switchAhcToLhcAndShrink(bufEntryCnt, dims, kdDims, hcPos);
			decEntryCount();
			return oldVal;
		}			

		Object oldVal;
		int offsIndex = getBitPosIndex();
		if (isAHC()) {
			//hyper-cube
			oldVal = values[(int) hcPos]; 
			values[(int) hcPos] = null;
			//Nothing else to do, values can just stay where they are
		} else {
			//linearized cube:
			//remove key and value
			int posBit = pinToOffsBitsLHC(posInNode, offsIndex, dims);
			Bits.removeBits(ba, posBit, IK_WIDTH(dims) + dims*postLen);
			//shrink array
			ba = Bits.arrayTrim(ba, calcArraySizeTotalBits(bufEntryCnt-1, dims));
			//values:
			oldVal = values[posInNode]; 
      values = Refs.removeSpaceAtPos(values, posInNode, bufEntryCnt-1);
			//kdKey
			int kdPos = posToKdPosLHC(posInNode, kdDims);
			kdKeys = RefsLong.arrayRemove(kdKeys, kdPos, kdDims);
		}

		decEntryCount();

		return oldVal;
	}


	/**
	 * @return True if the post-fixes are stored as array hyper-cube
	 */
	boolean isAHC() {
		return isAHC;
	}


	/**
	 * Set whether the post-fixes are stored as array hyper-cube.
	 */
	void setAHC(boolean b) {
		isAHC = b;
	}

	/**
	 * @return entry counter
	 */
	int getEntryCount() {
		return entryCnt;
	}


	private void decEntryCount() {
		--entryCnt;
	}


	private void incEntryCount() {
		entryCnt++;
	}


	int getBitPosIndex() {
		return getBitPosInfix();
	}

	int getBitPosInfix() {
		return HC_BITS;
	}


	private int posToOffsBitsDataAHC(long hcPos, int offsIndex, int dims) {
		return offsIndex + INN_HC_WIDTH * (1<<dims) + postLen * dims * (int)hcPos;
	}
	
	int pinToOffsBitsLHC(int pin, int offsIndex, int dims) {
		return offsIndex + (IK_WIDTH(dims) + postLen * dims) * pin;
	}
	
	int pinToOffsBitsData(int pin, long hcPos, int dims) {
		int offsIndex = getBitPosIndex();
		if (isAHC()) {
			return posToOffsBitsDataAHC(hcPos, offsIndex, dims);
		} else {
			return pinToOffsBitsLHC(pin, offsIndex, dims) + IK_WIDTH(dims);
		}
	}
	
	private int posToKdPosAHC(long hcPos, int kdDims) {
		return (int) (hcPos * kdDims);
	}
	
	private int posToKdPosLHC(int pin, int kdDims) {
		return pin * kdDims;
	}
	
	private int pinToKdPos(int pin, int kdDims) {
		return pin * kdDims;
	}
	
	/**
	 * 
	 * @param pos
	 * @param dims
	 * @return The position of the entry, for example as in the value[]. 
	 */
	int getPosition(long hcPos, final int dims) {
		int offsInd = getBitPosIndex();
		if (isAHC()) {
			//hyper-cube
			int posInt = (int) hcPos;  //Hypercube can not be larger than 2^31
			return (values[posInt] != null) ? posInt : -(posInt)-1;
		} else {
			//linearized cube
			return Bits.binarySearch(ba, offsInd, getEntryCount(), hcPos, IK_WIDTH(dims), 
					dims * postLen);
		}
	}

	/**
	 * Find first entry. 
	 * Assumption: There is at least one entry.
	 * @return
	 */
	int findFirstEntry(int dims) {
		if (isAHC()) {
			for (int i = 0; i < (1L<<dims); i++) {
				if (values[i] != null) {
					return i;
				}
			}
			return -1;
		}
		//LHC?
		return 0;
	}

	int getPostLen() {
		return postLen;
	}
	
	Object[] values() {
		return values;
	}
	
	long[] kdKeys() {
		return kdKeys;
	}
	
}
