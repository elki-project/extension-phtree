package ch.ethz.globis.pht.v12;

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

import static ch.ethz.globis.pht.PhTreeHelper.DEBUG;
import static ch.ethz.globis.pht.PhTreeHelper.posInArray;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import ch.ethz.globis.pht.PersistenceProvider;
import ch.ethz.globis.pht.PhTreeHelper;
import ch.ethz.globis.pht.util.RefsByte;
import ch.ethz.globis.pht.util.RefsLong;
import ch.ethz.globis.pht.util.Refs;
import ch.ethz.globis.pht.v12.PhTree12.NodeEntry;
import ch.ethz.globis.pht.v12.nt.NodeTreeV12;
import ch.ethz.globis.pht.v12.nt.NodeTreeV12.NtEntry12;
import ch.ethz.globis.pht.v12.nt.NtIteratorMask;
import ch.ethz.globis.pht.v12.nt.NtIteratorMinMax;
import ch.ethz.globis.pht.v12.nt.NtNode;
import ch.ethz.globis.pht64kd.MaxKTreeI.NtEntry;
import ch.ethz.globis.pht64kd.MaxKTreeI.PhIterator64;


/**
 * Node of the PH-tree.
 * 
 * @author ztilmann
 */
public class Node implements Externalizable {
	
	//size of references in bytes
	private static final int REF_BITS = 4*8;
	private static final int HC_BITS = 0;  //number of bits required for storing current (HC)-representation
	private static final int INN_HC_WIDTH = 0; //Index-NotNull: width of not-null flag for post/infix-hc

	/**
	 * Nodes switch to AHC when space(LHC)*AHC_HC_BIAS > space(AHC). 
	 */
	public static final double AHC_LHC_BIAS = 2.0;
	
	/**
	 * Threshold at which nodes should be turned into NT nodes.
	 */
	public static final int NT_THRESHOLD = 150; 
	
	public static final byte SUBCODE_EMPTY = -1;
	public static final byte SUBCODE_KEY_VALUE = -2;

	private Object[] values;
	
	private int entryCnt = 0;

	/**
	 * Structure of the byte[] and the required bits
	 * AHC:
	 * | kdKey AHC        |
	 * | 2^DIM*(DIM*pLen) |
	 * 
	 * LHC:
	 * | hcPos / kdKeyLHC      |
	 * | pCnt*(DIM + DIM*pLen) |
	 * 
	 * 
	 * pLen = postLen
	 * pCnt = postCount
	 * sCnt = subCount
	 */
	long[] ba = null;
	
	/**
	 * Infix codes. Values:
	 * 0: no entry 
	 * 1..64: == postLen+1
	 * -2: post-FIX 
	 */
	byte[] subCodes = null;

	// |   1st   |   2nd    |   3rd   |    4th   |
	// | isSubHC | isPostHC | isSubNI | isPostNI |
	private boolean isAHC = false;

	private byte postLen = 0;

	//Nested tree index
	private NtNode<Object> ind = null;

	
	/**
	 * @return true if NI should be used. 
	 */
	private static final boolean shouldSwitchToNT(int entryCount) {
		//Maybe just provide a switching threshold? 5-10?
		return entryCount >= NT_THRESHOLD;
	}

	private static final boolean shouldSwitchFromNtToHC(int entryCount) {
		return entryCount <= NT_THRESHOLD-30;
	}

	static final int IK_WIDTH(int dims) { return dims; }; //post index key width 

	/**
	 * DO NOT USE.
	 */
    public Node() {
		// For Externalizable & ZooDB only
	}

	protected Node(Node original) {
        if (original.values != null) {
            this.values = Refs.arrayClone(original.values);
        }
        this.entryCnt = original.entryCnt;
        this.isAHC = original.isAHC;
        this.postLen = original.postLen;
        if (original.ind != null) {
        	//copy NT tree
        	throw new UnsupportedOperationException();
        }
        if (original.ba != null) {
        	this.ba = Bits.arrayClone(original.ba);
        }
        if (original.subCodes != null) {
        	this.subCodes = RefsByte.arrayClone(original.subCodes);
        }
    }

	static Node createEmpty() {
		return new Node();
	}

	private void initNode(int postLen, int dims) {
		this.postLen = (byte) postLen;
		this.entryCnt = 0;
		this.ind = null;
		this.isAHC = false;
		int size = calcArraySizeTotalBits(2, dims);
		this.ba = Bits.arrayCreate(size);
		this.values = Refs.arrayCreate(2);
		this.subCodes = RefsByte.arrayCreate(2);
	}

	static Node createNode(int dims, int postLen) {
		Node n = NodePool.getNode();
		n.initNode(postLen, dims);
		return n;
	}

	static Node createNode(Node original) {
		return new Node(original);
	}

	<T> NodeEntry<T> createNodeEntry(long[] key, byte subCode, T value) {
		return new NodeEntry<>(key, subCode, value);
	}
	
	void discardNode() {
		Bits.arrayReplace(ba, null);
		Refs.arrayReplace(values, null);
		RefsByte.arrayReplace(subCodes, null);
		entryCnt = 0;
		NodePool.offer(this);
	}
	
	int calcArraySizeTotalBits(int entryCount, final int dims) {
		int nBits = getBitPosIndex();
		//post-fixes
		if (isAHC()) {
			//hyper-cube
			nBits += (INN_HC_WIDTH + dims * postLen) * (1 << dims);
		} else if (isNT()) {
			nBits += 0;
		} else {
			//hc-pos index
			nBits += entryCount * (IK_WIDTH(dims) + dims * postLen);
		}
		return nBits;
	}

	private int calcArraySizeTotalBitsNt() {
		return getBitPosIndex();
	}


	/**
	 * 
	 * @param pin
	 * @param hcPos
	 * @param outVal
	 * @return whether the infix length is > 0
	 */
	boolean getInfixOfSub(int pin, long hcPos, long[] outVal, byte subCode) {
		if (!hasSubInfix(subCode)) {
			return false;
		}
		int offs = pinToOffsBitsData(pin, hcPos, outVal.length);
		//To cut of trailing bits
		long mask = (-1L) << postLen;
		for (int i = 0; i < outVal.length; i++) {
			//Replace val with infix (val may be !=0 from traversal)
			outVal[i] = (mask & outVal[i]) | Bits.readArray(ba, offs, postLen);
			offs += postLen;
		}
		return true;
	}

	void getInfixOfSubNt(long[] infix, long[] outKey, byte subCode) {
		if (!hasSubInfix(subCode)) {
			return;
		}
		//To cut of trailing bits
		long mask = (-1L) << postLen;
		for (int i = 0; i < outKey.length; i++) {
			//Replace val with infix (val may be !=0 from traversal)
			outKey[i] = (mask & outKey[i]) | infix[i];
		}
	}

	/**
	 * Returns the value (T or Node) if the entry exists and matches the key.
	 * @param posInNode
	 * @param pos The position of the node when mapped to a vector.
	 * @return The sub node or null.
	 */
	Object doInsertIfMatching(long[] keyToMatch, Object newValueToInsert, PhTree12<?> tree) {
		long hcPos = posInArray(keyToMatch, getPostLen());

		if (isNT()) {
			//ntPut will also increase the node-entry count
			Object v = ntPut(hcPos, keyToMatch, SUBCODE_KEY_VALUE, newValueToInsert, 
					tree.getPersistenceProvider());
			//null means: Did not exist, or we had to do a split...
			if (v == null) {
				tree.increaseNrEntries();
			}
			return v;
		}
		
		int pin = getPosition(hcPos, keyToMatch.length);
		//check whether hcPos is valid
		if (pin < 0) {
			tree.increaseNrEntries();
			addPostPIN(hcPos, pin, keyToMatch, newValueToInsert, tree.getPersistenceProvider());
			return null;
		}
		
		byte subCode = getSubCode(pin);
		if (isSubNode(subCode)) {
			if (hasSubInfix(subCode)) {
				long mask = calcInfixMaskFromSC(subCode);
				return insertSplit(keyToMatch, newValueToInsert, subCode, pin, hcPos, tree, mask);
			}
			return tree.getPersistenceProvider().resolveObject(getValue(pin));
		} else {
			if (postLen > 0) {
				long mask = calcPostfixMask();
				return insertSplit(keyToMatch, newValueToInsert, subCode, pin, hcPos, tree, mask);
			}
			Object v = getValue(pin);
			//perfect match -> replace value
			setValue(pin, newValueToInsert, SUBCODE_KEY_VALUE);
			return v;
		}
	}

	/**
	 * Returns the value (T or Node) if the entry exists and matches the key.
	 * @param keyToMatch The key of the entry
	 * @param getOnly True if we only get the value. False if we want to delete it.
	 * @param parent
	 * @param newKey
	 * @param insertRequired
	 * @param tree
	 * @return The sub node or null.
	 */
	Object doIfMatching(long[] keyToMatch, boolean getOnly, Node parent,
			long[] newKey, int[] insertRequired, PhTree12<?> tree) {

		long hcPos = posInArray(keyToMatch, getPostLen());
		
		if (isNT()) {
			if (getOnly) {
				return ntGetEntryIfMatches(hcPos, keyToMatch, tree.getPersistenceProvider());
			}			
			Object v = ntRemoveEntry(hcPos, keyToMatch, newKey, insertRequired, 
					tree.getPersistenceProvider());
			if (v != null && !(v instanceof Node)) {
				//Found and removed entry.
				//TODO update counter?
				tree.decreaseNrEntries();
				if (getEntryCount() == 1) {
					mergeIntoParentNt(keyToMatch, parent, tree.getPersistenceProvider());
				}
				return v;
			} 
			//No need to resolve persistent objects, they should already be resolved at this point.
			return v;
		}
		
		int pin; 
		int offs;
		int dims = keyToMatch.length;
		byte subCode;
		//TODO remove this part!
		//TODO remove this part!
		//TODO remove this part!
		//TODO remove this part!
		//TODO remove this part!
//		pin = getPosition(hcPos, dims);
//		if (pin < 0) {
//			return null;
//		}
//		subCode = getSubCode(pin);
//		if (isSubEmpty(subCode)) {
//			return null;
//		}
		//TODO remove this part!
		//TODO remove this part! -> use subCode!
		if (isAHC()) {
			subCode = getSubCode((int) hcPos);
			if (isSubEmpty(subCode)) {
				//not found
				return null;
			}
			pin = (int) hcPos;
			offs = posToOffsBitsDataAHC(hcPos, getBitPosIndex(), dims);
		} else {
			pin = getPosition(hcPos, keyToMatch.length);
			if (pin < 0) {
				//not found
				return null;
			}
			offs = pinToOffsBitsDataLHC(pin, getBitPosIndex(), dims);
			subCode = getSubCode(pin);
		}
		Object v = getValue(pin);
		if (isSubNode(subCode)) {
			if (hasSubInfix(subCode)) {
				final long mask = calcInfixMaskFromSC(subCode);
				if (!readAndCheckKdKey(offs, keyToMatch, mask)) {
					return null;
				}
			}
			return tree.getPersistenceProvider().resolveObject(v);
		} else {
			final long mask = calcPostfixMask();
			if (!readAndCheckKdKey(offs, keyToMatch, mask)) {
				return null;
			}
			if (getOnly) {
				return v;
			} else {
				return deleteAndMergeIntoParent(pin, hcPos, keyToMatch, 
							parent, newKey, insertRequired, v, tree);
			}			
		}
	}
	
	private boolean readAndCheckKdKey(int offs, long[] keyToMatch, long mask) {
		for (int i = 0; i < keyToMatch.length; i++) {
			long k = Bits.readArray(ba, offs, postLen);
			if (((k ^ keyToMatch[i]) & mask) != 0) {
				return false;
			}
			offs += postLen;
		}
		return true;
	}

	public long calcPostfixMask() {
		return ~((-1L)<<postLen);
	}
	
	public long calcInfixMaskFromSC(byte subCode) {
		int subPostLen = calcSubPostLen(subCode);
		long mask = ~((-1L)<<(postLen-subPostLen-1));
		return mask << (subPostLen+1);
	}
	

	/**
	 * Splitting occurs if a node with an infix has to be split, because a new value to be inserted
	 * requires a partially different infix.
	 * @param newKey
	 * @param newValue
	 * @param currentKdKey WARNING: For AHC/LHC, this is an empty buffer
	 * @param currentValue
	 * @param node
	 * @param parent
	 * @param posInParent
	 * @return The value
	 */
	private Object insertSplit(long[] newKey, Object newValue, byte subCode,
			int pin, long hcPos, PhTree12<?> tree, long mask) {
        //do the splitting

        //What does 'splitting' mean (we assume there is currently a sub-node, in case of a postfix
        // work similar):
        //The current sub-node has an infix that is not (or only partially) compatible with 
        //the new key.
        //We create a new intermediary sub-node between the parent node and the current sub-node.
        //The new key/value (which we want to add) should end up as post-fix for the new-sub node. 
        //All other current post-fixes and sub-nodes stay in the current sub-node. 

        //How splitting works:
        //We insert a new node between the current and the parent node:
        //  parent -> newNode -> node
        //The parent is then updated with the new sub-node and the current node gets a shorter
        //infix.

		int bitOffs = pinToOffsBitsData(pin, hcPos, newKey.length);
		long[] buffer = new long[newKey.length];
		int maxConflictingBits = calcConflictingBits(newKey, bitOffs, buffer, mask);
		if (maxConflictingBits == 0) {
			Object v = getValue(pin);
			if (subCode == SUBCODE_KEY_VALUE) {
				setValue(pin, newValue, SUBCODE_KEY_VALUE);
				return v;
			} 
			return tree.getPersistenceProvider().resolveObject(v);
		}
		
		//subCode remains the same
		Node newNode = createNode(newKey, SUBCODE_KEY_VALUE, newValue, 
				buffer, subCode, getValue(pin), maxConflictingBits);
		Object newNodeObj = tree.getPersistenceProvider().storeObject(newNode);

        //determine length of infix
        replaceEntryWithSub(pin, hcPos, newKey, calcSubCode(newNode), newNodeObj, false, 
        		tree.getPersistenceProvider());
        tree.increaseNrEntries();
		//entry did not exist
        return null;
    }

    /**
     * 
     * @param key1
     * @param val1
     * @param key2
     * @param val2
     * @param mcb
     * @return A new node or 'null' if there are no conflicting bits
     */
    public Node createNode(
    		long[] key1, byte subCode1, Object val1, 
    		long[] key2, byte subCode2, Object val2,
    		int mcb) {
        int newPostLen = mcb-1;
        Node newNode = createNode(key1.length, newPostLen);

        long posSub1 = posInArray(key1, newPostLen);
        long posSub2 = posInArray(key2, newPostLen);
        if (posSub1 < posSub2) {
        	newNode.writeEntry(0, posSub1, key1, subCode1, val1);
        	newNode.writeEntry(1, posSub2, key2, subCode2, val2);
        } else {
        	newNode.writeEntry(0, posSub2, key2, subCode2, val2);
        	newNode.writeEntry(1, posSub1, key1, subCode1, val1);
        }
        newNode.incEntryCount();
        newNode.incEntryCount();
        return newNode;
    }

    /**
     * @param v1
     * @param v2
     * @param mask bits to consider (1) and to ignore (0)
     * @return the position of the most significant conflicting bit (starting with 1) or
     * 0 in case of no conflicts.
     */
    public static int calcConflictingBits(long[] v1, long[] v2, long mask) {
		//long mask = (1l<<node.getPostLen()) - 1l; // e.g. (0-->0), (1-->1), (8-->127=0x01111111)
     	//write all differences to diff, we just check diff afterwards
		long diff = 0;
		for (int i = 0; i < v1.length; i++) {
			diff |= (v1[i] ^ v2[i]);
		}
    	return Long.SIZE-Long.numberOfLeadingZeros(diff & mask);
    }
    
    /**
     * @param v1
     * @param outV The 2nd kd-key is read into outV
     * @return the position of the most significant conflicting bit (starting with 1) or
     * 0 in case of no conflicts.
     */
    private int calcConflictingBits(long[] v1, int bitOffs, long[] outV, long mask) {
		//long mask = (1l<<node.getPostLen()) - 1l; // e.g. (0-->0), (1-->1), (8-->127=0x01111111)
     	//write all differences to diff, we just check diff afterwards
		long diff = 0;
		long[] ia = ba;
		int offs = bitOffs;
		for (int i = 0; i < v1.length; i++) {
			long k = Bits.readArray(ia, offs, postLen);
			diff |= (v1[i] ^ k);
			outV[i] = k;
			offs += postLen;
		}
    	return Long.SIZE-Long.numberOfLeadingZeros(diff & mask);
    }
    
	private Object deleteAndMergeIntoParent(int pinToDelete, long hcPos, long[] key, 
			Node parent, long[] newKey, int[] insertRequired, Object valueToDelete, 
			PhTree12<?> tree) {
		
		int dims = key.length;

		//Check for update()
		if (newKey != null) {
			//replace
			int bitPosOfDiff = calcConflictingBits(key, newKey, -1L);
			if (bitPosOfDiff <= getPostLen()) {
				//replace
				return replacePost(pinToDelete, hcPos, newKey);
			} else {
				insertRequired[0] = bitPosOfDiff;
			}
		}

		//okay we have something to delete
		tree.decreaseNrEntries();

		//check if merging is necessary (check children count || isRootNode)
		if (parent == null || getEntryCount() > 2) {
			//no merging required
			//value exists --> remove it
			return removeEntry(hcPos, pinToDelete, dims);
		}

		//okay, at his point we have a post that matches and (since it matches) we need to remove
		//the local node because it contains at most one other entry and it is not the root node.
		
		//The pin of the entry that we want to keep
		int pin2 = -1;
		long pos2 = -1;
		Object val2 = null;
		byte subCode2 = -123;
		if (isAHC()) {
			for (int i = 0; i < (1<<key.length); i++) {
				if (getValue(i) != null && i != pinToDelete) {
					pin2 = i;
					pos2 = i;
					subCode2 = getSubCode(i);
					val2 = getValue(i);
					break;
				}
			}
		} else {
			//LHC: we have only pos=0 and pos=1
			pin2 = (pinToDelete == 0) ? 1 : 0;
			int offs = pinToOffsBitsLHC(pin2, getBitPosIndex(), dims);
			pos2 = Bits.readArray(ba, offs, IK_WIDTH(dims));
			subCode2 = getSubCode(pin2);
			val2 = getValue(pin2);
		}

		long[] newPost = new long[dims];
		RefsLong.arraycopy(key, 0, newPost, 0, key.length);

		long posInParent = PhTreeHelper.posInArray(key, parent.getPostLen());
		int pinInParent = parent.getPosition(posInParent, dims);
		if (isSubNode(subCode2)) {
			PhTreeHelper.applyHcPos(pos2, getPostLen(), newPost);
			getInfixOfSub(pin2, pos2, newPost, subCode2);
			byte newSub2Code = calcSubCodeMerge(subCode2);
			//update parent, the position is the same
			//we use newPost as Infix
			parent.replaceEntryWithSub(pinInParent, posInParent, newPost, newSub2Code, val2, true,
					tree.getPersistenceProvider());
		} else {
			//this is also a post
			getEntryByPIN(pin2, pos2, newPost);
			parent.replaceSubWithPost(pinInParent, posInParent, newPost, val2, 
					tree.getPersistenceProvider());
		}

		discardNode();
		return valueToDelete;
	}

	private void mergeIntoParentNt(long[] key, Node parent, PersistenceProvider pp) {
		int dims = key.length;

		//check if merging is necessary (check children count || isRootNode)
		if (parent == null || getEntryCount() > 2) {
			//no merging required
			//value exists --> remove it
			return;
		}

		//okay, at his point we have a post that matches and (since it matches) we need to remove
		//the local node because it contains at most one other entry and it is not the root node.
		
		//TODO Setting up a whole IteratorPos seems to be a bit over the top...?
		//TODO --> especially since there are only two elements...
		NtIteratorMinMax<Object> iter = ntIterator(dims, pp);
		NtEntry12<Object> nte = iter.nextEntryReuse();

		long posInParent = PhTreeHelper.posInArray(key, parent.getPostLen());
		int pinInParent = parent.getPosition(posInParent, dims);
		byte sub2Code = nte.getKdSubCode();
		if (isSubNode(sub2Code)) {
			//connect sub to parent
			//int newInfixLen = parent.getInfixCode(pin) + 1 + sub2Code;
			byte newSub2Code = calcSubCodeMerge(sub2Code);

			//update parent, the position is the same, we use newPost as Infix
			//For subnodes, we can reuse the subCode.
			parent.replaceEntryWithSub(pinInParent, posInParent, nte.getKdKey(), 
					newSub2Code, nte.getValue(), true, pp);
		} else {
			//this is also a post
			parent.replaceSubWithPost(pinInParent, posInParent, nte.getKdKey(), nte.getValue(), pp);
		}

		discardNode();
	}

	/**
	 * @param posInNode
	 * @param pos The position of the node when mapped to a vector.
	 * @return The sub node or null.
	 */
	Object getEntryByPIN(int posInNode, long hcPos, long[] postBuf) {
		PhTreeHelper.applyHcPos(hcPos, postLen, postBuf);
		byte subCode = getSubCode(posInNode);
		if (isSubNode(subCode)) {
			getInfixOfSub(posInNode, hcPos, postBuf, subCode);
		} else {
			int offsetBit = pinToOffsBitsData(posInNode, hcPos, postBuf.length);
			final long mask = (~0L)<<postLen;
			for (int i = 0; i < postBuf.length; i++) {
				postBuf[i] &= mask;
				postBuf[i] |= Bits.readArray(ba, offsetBit, postLen);
				offsetBit += postLen;
			}
		}
		return getValue(posInNode);
	}


	/**
	 * @param posInNode
	 * @param pos The position of the node when mapped to a vector.
	 * @return The sub node or null.
	 */
	Object getEntry(long hcPos, long[] postBuf, PersistenceProvider pp) {
		int posInNode = getPosition(hcPos, postBuf.length);
		if (posInNode < 0) {
			return null;
		}
		if (isNT()) {
			NodeEntry<?> e = new NodeEntry<>(postBuf, SUBCODE_EMPTY, null);
			ntGetEntry(hcPos, e, pp);
			return isSubNode(e.getSubCode()) ? pp.resolveObject(e.getValue()) : e.getValue();
		}
		Object ret = getEntryByPIN(posInNode, hcPos, postBuf);
		return isSubNode(getSubCode(posInNode)) ? pp.resolveObject(ret) : ret;
	}


	/**
	 * @param posInNode
	 * @param pos The position of the node when mapped to a vector.
	 * @return The sub node or null.
	 */
	Object getEntryPIN(int posInNode, long hcPos, long[] subNodePrefix, long[] outKey) {
		byte subCode = getSubCode(posInNode);
		if (isSubEmpty(subCode)) {
			return null;
		}
		PhTreeHelper.applyHcPos(hcPos, postLen, subNodePrefix);
		if (isSubNode(subCode)) {
			getInfixOfSub(posInNode, hcPos, subNodePrefix, subCode);
		} else {
			int offsetBit = pinToOffsBitsData(posInNode, hcPos, subNodePrefix.length);
			final long mask = (~0L)<<postLen;
			for (int i = 0; i < subNodePrefix.length; i++) {
				outKey[i] = (subNodePrefix[i] & mask) | Bits.readArray(ba, offsetBit, postLen);
				offsetBit += postLen;
			}
		}
		return getValue(posInNode);
	}

	private boolean shouldSwitchToAHC(int entryCount, int dims) {
		return useAHC(entryCount, dims);
	}
	
	private boolean shouldSwitchToLHC(int entryCount, int dims) {
		return !useAHC(entryCount+2, dims);
	}
	
	private boolean useAHC(int entryCount, int dims) {
		//calc post mode.
		//+1 bit for null/not-null flag
		long sizeAHC = (dims * postLen + INN_HC_WIDTH + REF_BITS + 8) * (1L << dims); 
		//+DIM because every index entry needs DIM bits
		long sizeLHC = (dims * postLen + IK_WIDTH(dims) + REF_BITS + 8) * (long)entryCount;
		//Already 1.1 i.o. 1.0 has significant bad impact on perf.
		return PhTree12.AHC_ENABLED && (dims<=31) && (sizeLHC*AHC_LHC_BIAS >= sizeAHC);
	}

	/**
	 * Writes a complete entry.
	 * This should only be used for new nodes.
	 * 
	 * @param pin
	 * @param hcPos
	 * @param newKey
	 * @param value
	 * @param newSubInfixLen -infix len for sub-nodes. This is ignored for post-fixes.
	 */
	private void writeEntry(int pin, long hcPos, long[] newKey, byte subCode, Object value) {
		if (isNT()) {
			ntPut(hcPos, newKey, subCode, value, null);
			return;
		}
		int dims = newKey.length;
		int offsIndex = getBitPosIndex();
		int offsKey;
		if (isAHC()) {
			setValue((int) hcPos, value, subCode);
			offsKey = posToOffsBitsDataAHC(hcPos, offsIndex, dims);
		} else {
			setValue(pin, value, subCode);
			offsKey = pinToOffsBitsLHC(pin, offsIndex, dims);
			Bits.writeArray(ba, offsKey, IK_WIDTH(dims), hcPos);
			offsKey += IK_WIDTH(dims);
		}
		if (postLen > 0) {
			for (int i = 0; i < newKey.length; i++) {
				Bits.writeArray(ba, offsKey, postLen, newKey[i]);
				offsKey += postLen;
			}
		}
	}

	private Object replacePost(int pin, long hcPos, long[] newKey) {
		int offs = pinToOffsBitsData(pin, hcPos, newKey.length);
		for (int i = 0; i < newKey.length; i++) {
			Bits.writeArray(ba, offs, postLen, newKey[i]);
			offs += postLen;
		}
		return getValue(pin);
	}

	void replaceEntryWithSub(int posInNode, long hcPos, long[] infix, byte subCode, 
			Object newSub, boolean writeInfix, PersistenceProvider pp) {
		if (isNT()) {
			//TODO during insert we wounldn't need to rewrite the infix, only the is-infix-0 
			//     would need to be set...
			ntReplaceEntry(hcPos, infix, subCode, newSub, pp);
			return;
		}
		//During insert, we do not need to rewrite the infix, because the previous infix
		//must still be valid.
		//		//We always write it for values (rather than subNodes)
		if (writeInfix && hasSubInfix(subCode)) {// || !Node.isSubNode(subCode)) {
			replacePost(posInNode, hcPos, infix);
		}
		setValue(posInNode, newSub, subCode);
	}

	/**
	 * Replace a sub-node with a postfix, for example if the current sub-node is removed, 
	 * it may have to be replaced with a post-fix.
	 */
	void replaceSubWithPost(int pin, long hcPos, long[] key, Object value, PersistenceProvider pp) {
		if (isNT()) {
			ntReplaceEntry(hcPos, key, SUBCODE_KEY_VALUE, value, pp);
			return;
		}
		setValue(pin, value, SUBCODE_KEY_VALUE);
		replacePost(pin, hcPos, key);
	}

	Object ntReplaceEntry(long hcPos, long[] kdKey, byte subCode, Object value, 
			PersistenceProvider pp) {
		//We use 'null' as parameter to indicate that we want replacement, rather than splitting,
		//if the value exists.
		return NodeTreeV12.addEntry(ind, hcPos, kdKey, subCode, value, null, pp);
	}
	
	/**
	 * General contract:
	 * Returning a value or NULL means: Value was replaced, no change in counters
	 * Returning a Node means: Traversal not finished, no change in counters
	 * Returning null means: Insert successful, please update global entry counter
	 * 
	 * Node entry counters are updated internally by the operation
	 * Node-counting is done by the NodePool.
	 * 
	 * @param hcPos
	 * @param dims
	 * @param tree Providing a 'tree' means that the NT-tree will 'resolve' the value 
	 * if it is a Node.
	 * @return the previous value
	 */
	Object ntPut(long hcPos, long[] kdKey, byte subCode, Object value, PersistenceProvider pp) {
		return NodeTreeV12.addEntry(ind, hcPos, kdKey, subCode, value, this, pp);
	}
	
	/**
	 * General contract:
	 * Returning a value or NULL means: Value was removed, please update global entry counter
	 * Returning a Node means: Traversal not finished, no change in counters
	 * Returning null means: Entry not found, no change in counters
	 * 
	 * Node entry counters are updated internally by the operation
	 * Node-counting is done by the NodePool.
	 * 
	 * @param hcPos
	 * @param dims
	 * @return
	 */
	Object ntRemoveAnything(long hcPos, int dims, PersistenceProvider pp) {
    	return NodeTreeV12.removeEntry(ind, hcPos, dims, null, null, null, null, pp);
	}

	Object ntRemoveEntry(long hcPos, long[] key, long[] newKey, int[] insertRequired, 
			PersistenceProvider pp) {
    	return NodeTreeV12.removeEntry(ind, hcPos, key.length, key, newKey, insertRequired, 
    			this, pp);
	}

	<T> Object ntGetEntry(long hcPos, NodeEntry<T> outKey, PersistenceProvider pp) {
		return NodeTreeV12.getEntry(ind(), hcPos, outKey, null, null, pp);
	}

	Object ntGetEntryIfMatches(long hcPos, long[] keyToMatch, PersistenceProvider pp) {
		return NodeTreeV12.getEntry(ind(), hcPos, null, keyToMatch, this, pp);
	}

	int ntGetSize() {
		return getEntryCount();
	}
	

	private void switchLhcToAhcAndGrow(int oldEntryCount, int dims) {
		int posOfIndex = getBitPosIndex();
		int posOfData = posToOffsBitsDataAHC(0, posOfIndex, dims);
		setAHC( true );
		long[] bia2 = Bits.arrayCreate(calcArraySizeTotalBits(oldEntryCount+1, dims));
		Object [] v2 = Refs.arrayCreate(1<<dims);
		byte[] sc2 = RefsByte.arrayCreate(1<<dims);
		//Copy only bits that are relevant. Otherwise we might mess up the not-null table!
		Bits.copyBitsLeft(ba, 0, bia2, 0, posOfIndex);
		int postLenTotal = dims*postLen; 
		for (int i = 0; i < oldEntryCount; i++) {
			int entryPosLHC = posOfIndex + i*(IK_WIDTH(dims)+postLenTotal);
			int p2 = (int)Bits.readArray(ba, entryPosLHC, IK_WIDTH(dims));
			Bits.copyBitsLeft(ba, entryPosLHC+IK_WIDTH(dims),
					bia2, posOfData + postLenTotal*p2, 
					postLenTotal);
			v2[p2] = values[i];
			sc2[p2] = subCodes[i];
		}
		ba = Bits.arrayReplace(ba, bia2);
		values = Refs.arrayReplace(values, v2);
		subCodes = RefsByte.arrayReplace(subCodes, sc2);
	}
	
	
	private Object switchAhcToLhcAndShrink(int oldEntryCount, int dims, long hcPosToRemove) {
		Object oldEntry = null;
		setAHC( false );
		long[] bia2 = Bits.arrayCreate(calcArraySizeTotalBits(oldEntryCount-1, dims));
		Object[] v2 = Refs.arrayCreate(oldEntryCount-1);
		byte[] sc2 = RefsByte.arrayCreate(oldEntryCount-1);
		int oldOffsIndex = getBitPosIndex();
		int oldOffsData = oldOffsIndex + (1<<dims)*INN_HC_WIDTH;
		//Copy only bits that are relevant. Otherwise we might mess up the not-null table!
		Bits.copyBitsLeft(ba, 0, bia2, 0, oldOffsIndex);
		int postLenTotal = dims*postLen;
		int n=0;
		for (int i = 0; i < (1L<<dims); i++) {
			if (i == hcPosToRemove) {
				//skip the item that should be deleted.
				oldEntry = getValue(i);
				continue;
			}
			if (values[i] != null) {
				v2[n] = values[i];
				sc2[n] = subCodes[i];
				int entryPosLHC = oldOffsIndex + n*(IK_WIDTH(dims)+postLenTotal);
				Bits.writeArray(bia2, entryPosLHC, IK_WIDTH(dims), i);
				Bits.copyBitsLeft(
						ba, oldOffsData + postLenTotal*i, 
						bia2, entryPosLHC + IK_WIDTH(dims),
						postLenTotal);
				n++;
			}
		}
		ba = Bits.arrayReplace(ba, bia2);
		values = Refs.arrayReplace(values, v2);
		subCodes = RefsByte.arrayReplace(subCodes, sc2);
		return oldEntry;
	}
	
	
	/**
	 * 
	 * @param hcPos
	 * @param pin position in node: ==hcPos for AHC or pos in array for LHC
	 * @param key
	 */
	void addPostPIN(long hcPos, int pin, long[] key, Object value, PersistenceProvider pp) {
		final int dims = key.length;
		final int bufEntryCnt = getEntryCount();
		//decide here whether to use hyper-cube or linear representation
		//--> Initially, the linear version is always smaller, because the cube has at least
		//    two entries, even for a single dimension. (unless DIM > 2*REF=2*32 bit 
		//    For one dimension, both need one additional bit to indicate either
		//    null/not-null (hypercube, actually two bit) or to indicate the index. 

		if (!isNT() && shouldSwitchToNT(bufEntryCnt)) {
			ntBuild(bufEntryCnt, dims, key, pp);
		}
		if (isNT()) {
			ntPut(hcPos, key, SUBCODE_KEY_VALUE, value, pp);
			return;
		}

		//switch representation (HC <-> Linear)?
		if (!isAHC() && shouldSwitchToAHC(bufEntryCnt + 1, dims)) {
			switchLhcToAhcAndGrow(bufEntryCnt, dims);
			//no need to update pin now, we are in HC now.
		}

		incEntryCount();

		int offsIndex = getBitPosIndex();
		if (isAHC()) {
			//hyper-cube
			int offsPostKey = posToOffsBitsDataAHC(hcPos, offsIndex, dims);
			for (int i = 0; i < key.length; i++) {
				Bits.writeArray(ba, offsPostKey + postLen * i, postLen, key[i]);
			}
			setValue((int) hcPos, value, SUBCODE_KEY_VALUE);
		} else {
			//get position
			pin = -(pin+1);

			//resize array
			ba = Bits.arrayEnsureSize(ba, calcArraySizeTotalBits(bufEntryCnt+1, dims));
			long[] ia = ba;
			int offs = pinToOffsBitsLHC(pin, offsIndex, dims);
			Bits.insertBits(ia, offs, IK_WIDTH(dims) + dims*postLen);
			//insert key
			Bits.writeArray(ia, offs, IK_WIDTH(dims), hcPos);
			//insert value:
			offs += IK_WIDTH(dims);
			for (int i = 0; i < key.length; i++) {
				Bits.writeArray(ia, offs, postLen, key[i]);
				offs += postLen;
			}
			values = Refs.insertSpaceAtPos(values, pin, bufEntryCnt+1);
			subCodes = RefsByte.insertSpaceAtPos(subCodes, pin, bufEntryCnt+1);
			setValue(pin, value, SUBCODE_KEY_VALUE);
		}
	}

	void postToNI(int startBit, int postLen, long[] outKey, long hcPos, long[] prefix, long mask) {
		for (int d = 0; d < outKey.length; d++) {
			outKey[d] = Bits.readArray(ba, startBit, postLen) | (prefix[d] & mask);
			startBit += postLen;
		}
		PhTreeHelper.applyHcPos(hcPos, postLen, outKey);
	}

	void postFromNI(long[] ia, int startBit, long key[], int postLen) {
		//insert postifx
		for (int d = 0; d < key.length; d++) {
			Bits.writeArray(ia, startBit, postLen, key[d]);
			startBit += postLen;
		}
	}

	/**
	 * WARNING: This is overloaded in subclasses of Node.
	 * @return Index.
	 */
	NtNode<Object> createNiIndex(int dims) {
		return NtNode.createRoot(dims);
	}
	
	private void ntBuild(int bufEntryCnt, int dims, long[] prefix, PersistenceProvider pp) {
		//Migrate node to node-index representation
		if (ind != null || isNT()) {
			throw new IllegalStateException();
		}
		ind = createNiIndex(dims);

		long prefixMask = (-1L) << postLen;
		
		//read posts 
		if (isAHC()) {
			int oldOffsIndex = getBitPosIndex();
			int oldPostBitsVal = posToOffsBitsDataAHC(0, oldOffsIndex, dims);
			int postLenTotal = dims*postLen;
			final long[] buffer = new long[dims];
			for (int i = 0; i < (1L<<dims); i++) {
				Object o = values[i];
				if (o == null) {
					continue;
				} 
				int dataOffs = oldPostBitsVal + i*postLenTotal;
				postToNI(dataOffs, postLen, buffer, i, prefix, prefixMask);
				//We use 'null' as parameter to indicate that we want 
				//to skip checking for splitNode or increment of entryCount
				NodeTreeV12.addEntry(ind, i, buffer, getSubCode(i), o, null, pp);
			}
		} else {
			int offsIndex = getBitPosIndex();
			int dataOffs = pinToOffsBitsLHC(0, offsIndex, dims);
			int postLenTotal = dims*postLen;
			final long[] buffer = new long[dims];
			for (int i = 0; i < bufEntryCnt; i++) {
				long p2 = Bits.readArray(ba, dataOffs, IK_WIDTH(dims));
				dataOffs += IK_WIDTH(dims);
				Object e = values[i];
				postToNI(dataOffs, postLen, buffer, p2, prefix, prefixMask);
				//We use 'null' as parameter to indicate that we want 
				//to skip checking for splitNode or increment of entryCount
				NodeTreeV12.addEntry(ind, p2, buffer, getSubCode(i), e, null, pp);
				dataOffs += postLenTotal;
			}
		}

		setAHC(false);
		ba = Bits.arrayTrim(ba, calcArraySizeTotalBitsNt());
		values = Refs.arrayReplace(values, null); 
		subCodes = RefsByte.arrayReplace(subCodes, null);
	}

	/**
	 * 
	 * @param bufSubCnt
	 * @param bufPostCnt
	 * @param dims
	 * @param posToRemove
	 * @param removeSub Remove sub or post?
	 * @return Previous value if post was removed
	 */
	private Object ntDeconstruct(int dims, long posToRemove, PersistenceProvider pp) {
		//Migrate node to node-index representation
		if (ind == null || !isNT()) {
			throw new IllegalStateException();
		}

		int entryCountNew = ntGetSize() - 1;
		decEntryCount();

		//calc node mode.
		boolean shouldBeAHC = useAHC(entryCountNew, dims);
		setAHC(shouldBeAHC);


		Object oldValue = null;
		int offsIndex = getBitPosIndex();
		long[] bia2 = Bits.arrayCreate(calcArraySizeTotalBits(entryCountNew, dims));
		//Copy only bits that are relevant. Otherwise we might mess up the not-null table!
		Bits.copyBitsLeft(ba, 0, bia2, 0, offsIndex);
		int postLenTotal = dims*postLen;
		if (shouldBeAHC) {
			//HC mode
			Object[] v2 = Refs.arrayCreate(1<<dims);
			byte[] sc2 = RefsByte.arrayCreate(1<<dims);
			int startBitData = posToOffsBitsDataAHC(0, offsIndex, dims);
			NtIteratorMinMax<Object> it = ntIterator(dims, pp);
			while (it.hasNext()) {
				NtEntry12<Object> e = it.nextEntryReuse();
				long pos = e.key();
				if (pos == posToRemove) {
					//skip the item that should be deleted.
					oldValue = e.value();
					v2[(int) pos] = null;
					continue;
				}
				int p2 = (int) pos;
				int offsBitData = startBitData + postLen * dims * p2;
				postFromNI(bia2, offsBitData, e.getKdKey(), postLen);
				v2[p2] = e.value();
				sc2[p2] = e.getKdSubCode();
			}
			ba = Bits.arrayReplace(ba, bia2);
			values = Refs.arrayReplace(values, v2);
			subCodes = RefsByte.arrayReplace(subCodes, sc2);
		} else {
			//LHC mode
			Object[] v2 = Refs.arrayCreate(entryCountNew);
			byte[] sc2 = RefsByte.arrayCreate(entryCountNew);
			int n=0;
			PhIterator64<Object> it = ntIterator(dims, pp);
			int entryPosLHC = offsIndex;
			while (it.hasNext()) {
				NtEntry<Object> e = it.nextEntryReuse();
				long pos = e.key();
				if (pos == posToRemove) {
					//skip the item that should be deleted.
					oldValue = e.value();
					continue;
				}
				//write hc-key
				Bits.writeArray(bia2, entryPosLHC, IK_WIDTH(dims), pos);
				entryPosLHC += IK_WIDTH(dims);
				v2[n] = e.value();
				postFromNI(bia2, entryPosLHC, e.getKdKey(), postLen);
				entryPosLHC += postLenTotal;
				n++;
			}
			ba = Bits.arrayReplace(ba, bia2);
			values = Refs.arrayReplace(values, v2);
			subCodes = RefsByte.arrayReplace(subCodes, sc2);
		}			

		ind = null;
		return oldValue;
	}


	/**
	 * Get post-fix.
	 * @param pin
	 * @param hcPos
	 * @param inOutPrefix Input key with prefix. This may be modified in this method!
	 *              After the method call, this contains the postfix if the postfix matches the
	 * range. Otherwise it contains only part of the postfix.
	 * @param outKey Postfix output if the entry is a postfix
	 * @param rangeMin
	 * @param rangeMax
	 * @return Subnode or value if the postfix matches the range, otherwise NOT_FOUND.
	 */
	Object checkAndGetEntryPIN(int pin, long hcPos, long[] inOutPrefix, long[] outKey,
			long[] rangeMin, long[] rangeMax) {
		byte subCode = getSubCode(pin); 
		if (isSubEmpty(subCode)) {
			return null;
		}
		
		PhTreeHelper.applyHcPos(hcPos, postLen, inOutPrefix);

		if (isSubNode(subCode)) {
			return checkAndApplyInfix(subCode, pin, hcPos, 
					inOutPrefix, rangeMin, rangeMax) ? getValue(pin) : null;
		}
		
		if (checkAndGetPost(pin, hcPos, inOutPrefix, outKey, rangeMin, rangeMax)) {
			return getValue(pin);
		}
		return null;
	}

	private static int N_GOOD = 0;
	private static int N = 0;
	
	private boolean checkAndApplyInfix(byte subCode, int pin, long hcPos, long[] valTemplate, 
			long[] rangeMin, long[] rangeMax) {
		int dims = valTemplate.length;
		//first check if node-prefix allows sub-node to contain any useful values
		int subOffs = pinToOffsBitsData(pin, hcPos, dims);
		
		if (PhTreeHelper.DEBUG) {
			N_GOOD++;
			//Ensure that we never enter this method if the node cannot possibly contain a match.
			long maskClean = (-1L) << postLen;
			for (int dim = 0; dim < valTemplate.length; dim++) {
				if ((valTemplate[dim]&maskClean) > rangeMax[dim] || 
						(valTemplate[dim] | ~maskClean) < rangeMin[dim]) {
					if (getPostLen() < 63) {
						//							System.out.println("N-CAAI-min=" + Bits.toBinary(rangeMin[dim]));
						//							System.out.println("N-CAAI-val=" + Bits.toBinary(valTemplate[dim]));
						//							System.out.println("N-CAAI-max=" + Bits.toBinary(rangeMax[dim]));
						//							System.out.println("N-CAAI-msk=" + Bits.toBinary(maskClean));
						//							System.out.println("pl=" + getPostLen() + "  dim=" + dim);
					  //System.out.println("N-CAAI: " + ++N + " / " + N_GOOD);
						//THis happen for kNN when rangeMin/max are adjusted.
						throw new IllegalStateException("pl=" + getPostLen());
					}
					//ignore, this happens with negative values.
					//return false;
				}
			}
		}
		//	return true;
		//}
		
		if (!hasSubInfix(subCode)) {
			return true;
		}

		//assign infix
		//Shift in two steps in case they add up to 64.
		long maskClean = (-1L) << postLen;

		//first, clean trailing bits
		//Mask for comparing the tempVal with the ranges, except for bit that have not been
		//extracted yet.
		long compMask = (-1L)<<(calcSubPostLen(subCode)+1);
		for (int dim = 0; dim < valTemplate.length; dim++) {
			long in = (valTemplate[dim] & maskClean) | Bits.readArray(ba, subOffs, postLen);
			in &= compMask;
			if (in > rangeMax[dim] || (in | ~compMask) < rangeMin[dim]) {
				return false;
			}
			valTemplate[dim] = in;
			subOffs += postLen;
		}

		return true;
	}

	boolean checkAndApplyInfixNt(byte subCode, long[] postFix, long[] valTemplate, 
			long[] rangeMin, long[] rangeMax) {
		//first check if node-prefix allows sub-node to contain any useful values

		if (PhTreeHelper.DEBUG) {
			N_GOOD++;
			//Ensure that we never enter this method if the node cannot possibly contain a match.
			long maskClean = (-1L) << postLen;
			for (int dim = 0; dim < valTemplate.length; dim++) {
				if ((valTemplate[dim] & maskClean) > rangeMax[dim] || 
						(valTemplate[dim] | ~maskClean) < rangeMin[dim]) {
					if (getPostLen() < 63) {
						System.out.println("N-CAAI: " + ++N + " / " + N_GOOD);
						throw new IllegalStateException();
					}
					//ignore, this happens with negative values.
					//return false;
				}
			}
		}
		
		if (!hasSubInfix(subCode)) {
			return true;
		}

		//assign infix
		//Shift in two steps in case they add up to 64.
		long maskClean = (-1L) << postLen;

		//first, clean trailing bits
		//Mask for comparing the tempVal with the ranges, except for bit that have not been
		//extracted yet.
		long compMask = (-1L)<<(calcSubPostLen(subCode)+1);
		for (int dim = 0; dim < valTemplate.length; dim++) {
			long in = (valTemplate[dim] & maskClean) | postFix[dim];
			in &= compMask;
			if (in > rangeMax[dim] || in < (rangeMin[dim] & compMask)) {
				return false;
			}
			valTemplate[dim] = in;
		}

		return true;
	}

	
	/**
	 * Get post-fix.
	 * @param hcPos
	 * @param in The entry to check. 
	 * @param result This contains the current postfix
	 * @param valTemplate This contains the current prefix. If the entry is a subNode, 
	 * the infix is written to this parameter
	 * @param rangeMin The range minimum
	 * @param rangeMax The range maximum
	 * @return true is the entry matches the ranges, otherwise false
	 */
	@SuppressWarnings("unchecked")
	<T>  boolean checkAndGetEntryNt(long hcPos, Object value, NodeEntry<T> result, 
			long[] valTemplate, long[] rangeMin, long[] rangeMax) {
		byte subCode = result.subCode;
		if (isSubNode(subCode)) {
			PhTreeHelper.applyHcPos(hcPos, postLen, valTemplate);
			if (!checkAndApplyInfixNt(subCode, result.getKey(), valTemplate, 
					rangeMin, rangeMax)) {
				return false;
			}
			result.setNodeKeepKey(subCode, value);
		} else {
			//The result already contains the complete key, no need to apply the prefix.
			long[] inKey = result.getKey();
			for (int i = 0; i < inKey.length; i++) {
				long k = inKey[i];
				if (k < rangeMin[i] || k > rangeMax[i]) {
					return false;
				}
			}
			result.setPost(subCode, (T) value);
		}
		return true;
	}

	private boolean checkAndGetPost(int pin, long hcPos, long[] inPrefix, long[] outKey, 
			long[] rangeMin, long[] rangeMax) {
		long[] ia = ba;
		int offs = pinToOffsBitsData(pin, hcPos, rangeMin.length);
		final long mask = (~0L)<<postLen;
		for (int i = 0; i < outKey.length; i++) {
			long k = (inPrefix[i] & mask) | Bits.readArray(ia, offs, postLen);
			if (k < rangeMin[i] || k > rangeMax[i]) {
				return false;
			}
			outKey[i] = k;
			offs += postLen;
		}
		return true;
	}
	
	Object removeEntryNT(long hcPos, final int dims, PersistenceProvider pp) {
		final int bufEntryCnt = getEntryCount();
		if (shouldSwitchFromNtToHC(bufEntryCnt)) {
			return ntDeconstruct(dims, hcPos, pp);
		}
		Object o = ntRemoveAnything(hcPos, dims, pp);
		decEntryCount();
		return o;
	}
	
	Object removeEntry(long hcPos, int posInNode, final int dims) {
		final int bufEntryCnt = getEntryCount();
		
		//switch representation (HC <-> Linear)?
		if (isAHC() && shouldSwitchToLHC(bufEntryCnt, dims)) {
			//revert to linearized representation, if applicable
			Object oldVal = switchAhcToLhcAndShrink(bufEntryCnt, dims, hcPos);
			decEntryCount();
			return oldVal;
		}			

		int offsIndex = getBitPosIndex();
		Object oldVal;
		if (isAHC()) {
			//hyper-cube
			oldVal = getValue((int) hcPos); 
			setValue((int) hcPos, null, SUBCODE_EMPTY);
			//Nothing else to do, values can just stay where they are
		} else {
			//linearized cube:
			//remove key and value
			int posBit = pinToOffsBitsLHC(posInNode, offsIndex, dims);
			Bits.removeBits(ba, posBit, IK_WIDTH(dims) + dims*postLen);
			//shrink array
			ba = Bits.arrayTrim(ba, calcArraySizeTotalBits(bufEntryCnt-1, dims));
			//values:
			oldVal = getValue(posInNode); 
			values = Refs.removeSpaceAtPos(values, posInNode, bufEntryCnt-1);
			subCodes = RefsByte.removeSpaceAtPos(subCodes, posInNode, bufEntryCnt-1);
		}

		decEntryCount();
		return oldVal;
	}


	/**
	 * @return True if the post-fixes are stored as hyper-cube
	 */
	boolean isAHC() {
		return isAHC;
	}


	/**
	 * Set whether the post-fixes are stored as hyper-cube.
	 */
	void setAHC(boolean b) {
		isAHC = b;
	}


	boolean isNT() {
		return ind != null;
	}


	/**
	 * @return entry counter
	 */
	public int getEntryCount() {
		return entryCnt;
	}


	public void decEntryCount() {
		--entryCnt;
	}


	public void incEntryCount() {
		++entryCnt;
	}


	int getBitPosIndex() {
		return getBitPosInfix();
	}

	private int getBitPosInfix() {
		// isPostHC / isSubHC / postCount / subCount
		return HC_BITS;
	}


	private int posToOffsBitsDataAHC(long hcPos, int offsIndex, int dims) {
		return offsIndex + INN_HC_WIDTH * (1<<dims) + postLen * dims * (int)hcPos;
	}
	
	private int pinToOffsBitsDataLHC(int pin, int offsIndex, int dims) {
		return offsIndex + (IK_WIDTH(dims) + postLen * dims) * pin + IK_WIDTH(dims);
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
	
	/**
	 * 
	 * @param pos
	 * @param dims
	 * @return The position of the entry, for example as in the value[]. 
	 */
	int getPosition(long hcPos, final int dims) {
		if (isAHC()) {
			//hyper-cube
			int posInt = (int) hcPos;  //Hypercube can not be larger than 2^31
			return isSubEmpty(getSubCode(posInt)) ? -posInt-1 : posInt;
		} else {
			if (isNT()) {
				//For NI, this value is not used, because checking for presence is quite 
				//expensive. However, we have to return a positive value to avoid abortion
				//of search (negative indicates that no value exists). It is hack though...
				return Integer.MAX_VALUE;
			} else {
				//linearized cube
				int offsInd = getBitPosIndex();
				return Bits.binarySearch(ba, offsInd, getEntryCount(), hcPos, IK_WIDTH(dims), 
						dims * postLen);
			}
		}
	}

	private Object getValue(int pin) {
		return values[pin];
	}
	
	private void setValue(int pin, Object value, byte subCode) {
		if (DEBUG) {
			if (value instanceof Node && subCode < 0) {
				throw new IllegalStateException();
			}
			if (!(value instanceof Node) && subCode >= 0) {
				throw new IllegalStateException();
			}
		}
		values[pin] = value;
		subCodes[pin] = ++subCode;
	}
	
	byte getSubCode(int pin) {
		byte b = subCodes[pin];
		return --b;
	}
	
	public static boolean isSubNode(byte subCode) {
		return subCode >= 0;
	}
	
	public static boolean isSubEmpty(byte subCode) {
		return subCode == SUBCODE_EMPTY;
	}

	static Node valueToNode(Object value) {
		return (Node) value;
	}
	
	public static byte calcSubCodeMerge(byte subNodeSubCode) {
		return subNodeSubCode;
	}
	
	public static byte calcSubCode(Node subNode) {
		return (byte) subNode.getPostLen();
	}
	
	public static byte calcSubPostLen(byte subCode) {
		return subCode;
	}
	
	public boolean hasSubInfix(byte subCode) {
		return postLen-subCode-1 > 0;
	}

	public int getPostLen() {
		return postLen;
	}
	
	NtNode<Object> ind() {
		return ind;
	}

	NtIteratorMinMax<Object> ntIterator(int dims, PersistenceProvider pp) {
        return new NtIteratorMinMax<>(dims, pp).reset(ind, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    NtIteratorMask<Object> ntIteratorWithMask(int dims, long maskLower, long maskUpper,
    		PersistenceProvider pp) {
		return new NtIteratorMask<>(dims, pp).reset(ind, maskLower, maskUpper);
	}

	Object[] values() {
		return values;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(entryCnt);
		out.writeByte(postLen);
		out.writeBoolean(isAHC);
		//is NT
		boolean isNT = ind != null;
		out.writeBoolean(isNT);
		if (isNT) {
			out.writeObject(ind);
		} else {
			RefsLong.write(ba, out);
			Refs.write(values, out);
			RefsByte.write(subCodes, out);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		entryCnt = in.readInt();
		postLen = in.readByte();
		isAHC = in.readBoolean();
		boolean isNT = in.readBoolean();
		if (isNT) {
			//TODO uses non-Object read!!!!
			ind = (NtNode<Object>) in.readObject();
		} else {
			ba = RefsLong.read(in);
			//TODO uses non-Object read!!!!
			values = Refs.read(in);
			subCodes = RefsByte.read(in);
		}
	}

}
