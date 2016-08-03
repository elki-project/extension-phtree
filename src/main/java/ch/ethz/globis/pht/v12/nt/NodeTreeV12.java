package ch.ethz.globis.pht.v12.nt;

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

import static ch.ethz.globis.pht.PhTreeHelper.align8;

import java.util.List;

import ch.ethz.globis.pht.PersistenceProvider;
import ch.ethz.globis.pht.util.IntVar;
import ch.ethz.globis.pht.util.PhTreeStats;
import ch.ethz.globis.pht.util.StringBuilderLn;
import ch.ethz.globis.pht.v12.Bits;
import ch.ethz.globis.pht.v12.Node;
import ch.ethz.globis.pht.v12.PhTree12.NodeEntry;
import ch.ethz.globis.pht64kd.NodeTree64;

/**
 * NodeTrees are a way to represent Nodes that are to big to be represented as AHC or LHC nodes.
 * 
 * A NodeTree splits a k-dimensional node into a hierarchy of smaller nodes by splitting,
 * for example, the 16-dim key into 2 8-dim keys.
 * 
 * Unlike the normal PH-Tree, NodeTrees do not support infixes.
 * 
 * @author ztilmann
 *
 * @param <T> The value type of the tree 
 */
public class NodeTreeV12<T> implements NodeTree64<T> {

	//Enable HC incrementer / iteration
	static final boolean HCI_ENABLED = true; 
	//Enable AHC mode in nodes
	static final boolean AHC_ENABLED = true; 
	//This needs to be different from PhTree NULL to avoid confusion when extracting values.
	public static final Object NT_NULL = new Object();
	
	protected final IntVar nEntries = new IntVar(0);
	//Number of bit in the global key: [1..64].
	private final int keyBitWidth;
	
	private NtNode<T> root = null;

	private NodeTreeV12(int keyBitWidth) {
		if (keyBitWidth < 1 || keyBitWidth > 64) {
			throw new UnsupportedOperationException("keyBitWidth=" + keyBitWidth);
		}
		this.keyBitWidth = keyBitWidth;
		this.root = NtNode.createRoot(getKeyBitWidth());
	}

	/**
	 * @param keyBitWidth
	 * @return A new NodeTree
	 */
	public static <T> NodeTree64<T> create(int keyBitWidth) {
		return new NodeTreeV12<>(keyBitWidth);
	}
	
	/**
	 * 
	 * @param root The root of this internal tree.
	 * @param hcPos The 'key' in this node tree
	 * @param kdKey The key of the key-value that is stored under the hcPos
	 * @param value The value of the key-value that is stored under the hcPos
	 * @return The previous value at the position, if any.
	 */
	private static <T> T addEntry(NtNode<T> root, long hcPos, 
			long[] kdKey, Object value, IntVar entryCount) {
		T t = addEntry(root, hcPos, kdKey, Node.SUBCODE_KEY_VALUE, value, null, 
				PersistenceProvider.NONE);
		if (t == null) {
			entryCount.inc();
		}
		return t;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T addEntry(NtNode<T> root, long hcPos, 
			long[] kdKey, byte kdSubCode, Object value, Node phNode, PersistenceProvider pp) {
		NtNode<T> currentNode = root;
		while (true) {
			long localHcPos = NtNode.pos2LocalPos(hcPos, currentNode.getPostLen());
			int pin = currentNode.getPosition(localHcPos, NtNode.MAX_DIM);
			if (pin < 0) {
				//insert
				currentNode.localAddEntryPIN(pin, localHcPos, hcPos, kdKey, kdSubCode, 
						Node.SUBCODE_KEY_VALUE, value);
				incCounter(phNode);
				return null;
			}

			byte ntSubCode = currentNode.getNtSubCode(pin);
			long postInFix;
			int conflictingLevels;
			if (NtNode.isNtSubNode(ntSubCode)) {
				//check infix if infixLen > 0
				if (currentNode.hasSubInfix(ntSubCode)) {
					postInFix = currentNode.localReadInfix(pin, localHcPos);
					conflictingLevels = NtNode.getConflictingLevels(hcPos, postInFix, 
							currentNode.getPostLen(), ntSubCode);
				} else {
					postInFix = 0;
					conflictingLevels = 0;
				}
			} else {
				postInFix = currentNode.localReadPostfix(pin, localHcPos);
				long mask = ~((-1L) << (currentNode.getPostLen()*NtNode.MAX_DIM));
				conflictingLevels = NtNode.getMaxConflictingLevelsWithMask(hcPos, postInFix, mask);
			}				

			if (conflictingLevels != 0) {
				Object localVal = currentNode.getValueByPIN(pin);
				byte kdSubCodeLocal = currentNode.getKdSubCode(pin);
				int newPostLen =  conflictingLevels - 1;
				NtNode<T> newNode = NtNode.createNode(newPostLen, kdKey.length);
				currentNode.localReplaceEntryWithSub(pin, localHcPos, hcPos, 
						(byte)-123, NtNode.calcSubCode(newNode), store(pp, newNode));
				long localHcInSubOfNewEntry = NtNode.pos2LocalPos(hcPos, newPostLen);
				long localHcInSubOfPrevEntry = NtNode.pos2LocalPos(postInFix, newPostLen);
				//TODO assume pin=0 for first entry?
				newNode.localAddEntry(localHcInSubOfNewEntry, hcPos, kdKey, 
						kdSubCode, Node.SUBCODE_KEY_VALUE, value);
				long[] localKdKey = new long[kdKey.length];
				currentNode.getKdKeyByPIN(pin, localKdKey);
				newNode.localAddEntry(localHcInSubOfPrevEntry, postInFix, localKdKey, 
						kdSubCodeLocal, ntSubCode, localVal);
				incCounter(phNode);
				return null;
			}
			
			if (NtNode.isNtSubNode(ntSubCode)) {
				//traverse subNode
				Object localVal = currentNode.getValueByPIN(pin);
				currentNode = resolve(pp, localVal);
			} else {
				//identical internal postFixes.
				
				//external postfix is not checked  
				if (phNode == null) {
					return (T) currentNode.localReplaceEntry(pin, kdKey, kdSubCode, 
							Node.SUBCODE_KEY_VALUE, value);
				} else {
					//What do we have to do?
					//We two entries in the same location (local hcPos).
					//Now we need to compare the kdKeys.
					//If they are identical, we either replace the VALUE or return the SUB-NODE
					// (that's actually the same, simply return the VALUE)
					//If the kdKey differs, we have to split, insert a newSubNode and return null.
					
					byte localKdSubCode = currentNode.getKdSubCode(pin);
					if (Node.isSubNode(localKdSubCode)) {
						if (phNode.hasSubInfix(localKdSubCode)) {
							long mask = phNode.calcInfixMaskFromSC(localKdSubCode);
							return (T) insertSplitPH(kdKey, value, pin, 
									mask, localKdSubCode, currentNode, phNode, pp);
						}
						T ret = (T) currentNode.getValueByPIN(pin);
						return (T) pp.resolveObject(ret); 
					} else {
						if (phNode.getPostLen() > 0) {
							long mask = phNode.calcPostfixMask();
							return (T) insertSplitPH(kdKey, value, pin, 
									mask, localKdSubCode, currentNode, phNode, pp);
						}
						//perfect match -> replace value
						currentNode.localReplaceValue(pin, Node.SUBCODE_KEY_VALUE, 
								Node.SUBCODE_KEY_VALUE, value);
						return (T) value;
					}
				}
			}
		}
	}
	
	/**
	 * Increases the entry count of the NtTree. For PhTree nodes,
	 * this means increasing the entry count of the node.
	 */
	private static void incCounter(Node node) {
		if (node != null) {
			node.incEntryCount();
		}
	}
	
	private static Object insertSplitPH(long[] newKey, Object newValue, 
			int pin, long mask, byte currentKdSubCode, NtNode<?> currentNode, Node phNode,
			PersistenceProvider pp) {
		Object currentValue = currentNode.getValueByPIN(pin);
		long[] localKdKey = new long[newKey.length];
		currentNode.getKdKeyByPIN(pin, localKdKey);
		int maxConflictingBits = Node.calcConflictingBits(newKey, localKdKey, mask);
		if (maxConflictingBits == 0) {
			if (Node.isSubNode(currentKdSubCode)) {
				return pp.resolveObject(currentValue);
			}
			currentNode.localReplaceValue(pin, 
					Node.SUBCODE_KEY_VALUE, Node.SUBCODE_KEY_VALUE, newValue);
			return currentValue;
		}
		
		//subCode remains the same
		Node newNode = phNode.createNode(newKey, Node.SUBCODE_KEY_VALUE, newValue, 
						localKdKey, currentKdSubCode, currentValue, maxConflictingBits);
		Object newNodeObj = pp.storeObject(newNode);

		currentNode.localReplaceEntry(pin, newKey, Node.calcSubCode(newNode), 
				Node.SUBCODE_KEY_VALUE, newNodeObj);
		//entry did not exist
        return null;
	}
	
	/**
	 * Remove an entry from the tree.
	 * @param root
	 * @param hcPos
	 * @param outerDims
	 * @param entryCount
	 * @return The value of the removed key or null
	 */
	public static <T> Object removeEntry(
			NtNode<T> root, long hcPos, int outerDims, IntVar entryCount) {
		Object t = removeEntry(root, hcPos, outerDims, null, null, null, null, 
				PersistenceProvider.NONE);
		if (t != null) {
			entryCount.dec();
		}
		return t;
	}
	
	/**
	 * Removes an entry from the tree.
	 * @param root
	 * @param hcPos
	 * @param outerDims
	 * @param keyToMatch
	 * @param newKey
	 * @param insertRequired
	 * @param phNode
	 * @return The value of the removed key or null
	 */
	@SuppressWarnings("unchecked")
	public static <T> Object removeEntry(NtNode<T> root, long hcPos, int outerDims, 
			long[] keyToMatch, long[] newKey, int[] insertRequired, Node phNode, 
			PersistenceProvider pp) {
    	NtNode<T> parentNode = null;
    	int parentPin = -1;
    	long parentHcPos = -1;
    	NtNode<T> currentNode = root;
		while (true) {
			long localHcPos = NtNode.pos2LocalPos(hcPos, currentNode.getPostLen());
			int pin = currentNode.getPosition(localHcPos, NtNode.MAX_DIM);
			if (pin < 0) {
				//Not found
				return null;
			}

			byte ntSubCode = currentNode.getNtSubCode(pin);
			Object localVal = currentNode.getValueByPIN(pin);
			boolean isLocalSubNode = Node.isSubNode(ntSubCode);
			long postInFix;
			int conflictingLevels;
			if (isLocalSubNode) {
				//check infix if infixLen > 0
				if (currentNode.hasSubInfix(ntSubCode)) {
					postInFix = currentNode.localReadInfix(pin, localHcPos);
					conflictingLevels = NtNode.getConflictingLevels(hcPos, postInFix, 
							currentNode.getPostLen(), ntSubCode);
				} else {
					conflictingLevels = 0;
				}
			} else {
				postInFix = currentNode.localReadPostfix(pin, localHcPos);
				long mask = ~((-1L) << (currentNode.getPostLen()*NtNode.MAX_DIM));
				conflictingLevels = NtNode.getMaxConflictingLevelsWithMask(hcPos, postInFix, mask);
			}				

			if (conflictingLevels != 0) {
				//no match
				return null;
			}
			
			if (isLocalSubNode) {
				//traverse local subNode
				parentNode = currentNode;
				parentPin = pin;
				parentHcPos = localHcPos;
				currentNode = resolve(pp, localVal);
			} else {
				//perfect match, now we should remove the value (which can be a normal sub-node!)
				
				if (phNode != null) {
					byte kdSubCode = currentNode.getKdSubCode(pin);
					Object o = phGetIfKdMatches(keyToMatch, currentNode, pin, 
							kdSubCode, localVal, phNode);
					if (o == null) {
						//no match
						return null;
					}
					//compare kdKey!
					if (Node.isSubNode(kdSubCode)) {
						//This is a node, simply return it for further traversal
						return pp.resolveObject(o);
					}
					
					//Check for update()
					if (newKey != null) {
						//replace
						int bitPosOfDiff = Node.calcConflictingBits(keyToMatch, newKey, -1L);
						if (bitPosOfDiff <= phNode.getPostLen()) {
							//replace
							return currentNode.replaceEntry(pin, newKey, 
									kdSubCode, ntSubCode, localVal);
						} else {
							insertRequired[0] = bitPosOfDiff;
						}
					}
					
					//okay, we have a matching postfix, continue...
					phNode.decEntryCount();
				}
				
				//TODO why read T again?
				T ret = (T) currentNode.removeValue(localHcPos, pin, outerDims, NtNode.MAX_DIM);
				//Ignore for n>2 or n==0 (empty root node)
				if (parentNode != null && currentNode.getEntryCount() == 1) {
					//insert remaining entry into parent.
					int pin2 = currentNode.findFirstEntry(NtNode.MAX_DIM);
					long localHcPos2 = currentNode.localReadKey(pin2);
					Object val2 = currentNode.getValueByPIN(pin2);
					byte kdSubCode2 = currentNode.getKdSubCode(pin2);
					byte ntSubCode2 = currentNode.getNtSubCode(pin2);
					int postLen2 = currentNode.getPostLen()*NtNode.MAX_DIM;
					//clean hcPos + postfix/infix 
					long mask2 = (postLen2+NtNode.MAX_DIM==64) ? 0 : (-1L) << (postLen2+NtNode.MAX_DIM);
					//get prefix
					long postInfix2 = hcPos & mask2;
					//get hcPos
					postInfix2 |= localHcPos2 << postLen2;
					//get postFix / infFix
					if (NtNode.isNtSubNode(ntSubCode2)) {
						postInfix2 |= currentNode.localReadInfix(pin2, localHcPos2);
					} else {
						postInfix2 |= currentNode.localReadPostfix(pin2, localHcPos2);
					}
					parentNode.replacePost(parentPin, parentHcPos, postInfix2, NtNode.MAX_DIM);
					long[] kdKey2 = new long[outerDims];
					currentNode.getKdKeyByPIN(pin2, kdKey2);
					parentNode.localReplaceEntry(parentPin, kdKey2, kdSubCode2, ntSubCode2, val2);
					currentNode.discardNode();
				}
				return ret;
			}
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private static <T> NtNode<T> resolve(PersistenceProvider pp, Object o) {
		//return (NtNode<T>) ((pp == null) ? o : pp.resolveObject(o));
		return (NtNode<T>) pp.resolveObject(o);
	}

	private static Object store(PersistenceProvider pp, NtNode<?> o) {
		//return (tree == null) ? o : tree.storeObject(o);
		return pp.storeObject(o);
	}

	private static Object phGetIfKdMatches(long[] keyToMatch,
			NtNode<?> currentNodeNt, int pinNt, byte kdSubCode, 
			Object currentVal, Node phNode) {
		if (Node.isSubNode(kdSubCode)) {
			if (phNode.hasSubInfix(kdSubCode)) {
				final long mask = phNode.calcInfixMaskFromSC(kdSubCode);
				if (!currentNodeNt.readKdKeyAndCheck(pinNt, keyToMatch, mask)) {
					//no match
					return null;
				}
			}
			return currentVal;
		} else {
			final long mask = phNode.calcPostfixMask();
			if (!currentNodeNt.readKdKeyAndCheck(pinNt, keyToMatch, mask)) {
				//no match
				return null;
			}
			
			//So we have a match and an entry to remove
			//We simply remove it an let Node handle the merging, if required.
			return currentVal;
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> Object getEntry(NtNode<T> root, long hcPos, Object outKey, 
			long[] kdKeyToMatch, Node phNode, PersistenceProvider pp) {
		NtNode<T> currentNode = root;
		if (outKey instanceof NodeEntry) {
			((NodeEntry<Object>) outKey).reset(); 
		}
		while (true) {
			long localHcPos = NtNode.pos2LocalPos(hcPos, currentNode.getPostLen());
			int pin = currentNode.getPosition(localHcPos, NtNode.MAX_DIM);
			if (pin < 0) {
				//Not found
				return null;
			}
			
			if (outKey instanceof long[]) {
				currentNode.getKdKeyByPIN(pin, (long[]) outKey);
			}
			
			byte ntSubCode = currentNode.getNtSubCode(pin);
			boolean isLocalSubNode = NtNode.isNtSubNode(ntSubCode);
			long postInFix;
			int conflictingLevels;
			if (isLocalSubNode) {
				//check infix if infixLen > 0
				if (currentNode.hasSubInfix(ntSubCode)) {
					postInFix = currentNode.localReadInfix(pin, localHcPos);
					conflictingLevels = NtNode.getConflictingLevels(hcPos, postInFix, 
							currentNode.getPostLen(), ntSubCode);
				} else {
					conflictingLevels = 0;
				}
			} else {
				postInFix = currentNode.localReadPostfix(pin, localHcPos);
				long mask = ~((-1L) << (currentNode.getPostLen()*NtNode.MAX_DIM));
				conflictingLevels = NtNode.getMaxConflictingLevelsWithMask(hcPos, postInFix, mask);
			}				

			if (conflictingLevels != 0) {
				//no match
				return null;
			}

			Object localVal = currentNode.getValueByPIN(pin);
			if (isLocalSubNode) {
				//traverse local subNode
				currentNode = resolve(pp, localVal);
			} else {
				byte kdSubCode = currentNode.getKdSubCode(pin);
				//identical postFixes, so we return the value (which can be a normal sub-node!)
				//compare kdKey, null indicates 'no match'.
				if (kdKeyToMatch != null && phGetIfKdMatches(
								kdKeyToMatch, currentNode, pin, kdSubCode, localVal, phNode) == null) {
					//no match
					return null;
				}
				if (outKey instanceof NodeEntry) {
					NodeEntry<Object> ne = (NodeEntry<Object>) outKey; 
					currentNode.getKdKeyByPIN(pin, ne.getKey());
					//Here we do NOT resolve the stored node object (if it is one),
					//because resolving it may not be necessary, depending on further checks
					//on the prefix once it is returned (e.g. NodeIteratorRangeCheck).
					ne.setValue(localVal);
					ne.setSubCode(kdSubCode);
					return localVal;
				}
				//In this case we have to resolve it, because the caller can otherwise not
				//see whether it is a nod or not (we don't return the subCode)
				return Node.isSubNode(kdSubCode) ? pp.resolveObject(localVal) : localVal;
			}
		}
	}

	/**
	 * Best HC incrementer ever. 
	 * @param v
	 * @param min
	 * @param max
	 * @return next valid value or min.
	 */
	static long inc(long v, long min, long max) {
		//first, fill all 'invalid' bits with '1' (bits that can have only one value).
		long r = v | (~max);
		//increment. The '1's in the invalid bits will cause bitwise overflow to the next valid bit.
		r++;
		//remove invalid bits.
		return (r & max) | min;

		//return -1 if we exceed 'max' and cause an overflow or return the original value. The
		//latter can happen if there is only one possible value (all filter bits are set).
		//The <= is also owed to the bug tested in testBugDecrease()
		//return (r <= v) ? -1 : r;
	}

	/**
	 * @see NodeTree64#size()
	 */
	@Override
	public int size() {
		return nEntries.get();
	}

	void increaseNrEntries() {
		nEntries.inc();
	}
	
	void decreaseNrEntries() {
		nEntries.dec();
	}

	/**
	 * @see NodeTree64#getKeyBitWidth()
	 */
	@Override
	public int getKeyBitWidth() {
		return keyBitWidth;
	}

	/**
	 * @see NodeTree64#getRoot()
	 */
	@Override
	public NtNode<T> getRoot() {
		return root;
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#put(long, long[], T)
	 */
	@Override
	public T put(long key, long[] kdKey, T value) {
		return NodeTreeV12.addEntry(
				root, key, kdKey, value == null ? NT_NULL : value, nEntries);
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#putB(long, long[])
	 */
	@Override
	public boolean putB(long key, long[] kdKey) {
		return NodeTreeV12.addEntry(
				root, key, kdKey, NT_NULL, nEntries) != null;
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#contains(long, long[])
	 */
	@Override
	public boolean contains(long key, long[] outKdKey) {
		return NodeTreeV12.getEntry(root, key, outKdKey, null, null, 
				PersistenceProvider.NONE) != null;
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#get(long, long[])
	 */
	@Override
	@SuppressWarnings("unchecked")
	public T get(long key, long[] outKdKey) {
		Object ret = NodeTreeV12.getEntry(root, key, outKdKey, null, null, 
				PersistenceProvider.NONE);
		return ret == NT_NULL ? null : (T)ret;
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#remove(long)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public T remove(long key) {
		Object ret = NodeTreeV12.removeEntry(root, key, getKeyBitWidth(), nEntries);
		return ret == NT_NULL ? null : (T)ret;
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#removeB(long)
	 */
	@Override
	public boolean removeB(long key) {
		Object ret = NodeTreeV12.removeEntry(root, key, getKeyBitWidth(), nEntries);
		return ret != null;
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#toStringTree()
	 */
	@Override
	public String toStringTree() {
		StringBuilderLn sb = new StringBuilderLn();
		printTree(sb, root);
		return sb.toString();
	}
	
	@SuppressWarnings("unchecked")
	private void printTree(StringBuilderLn str, NtNode<T> node) {
		int indent = NtNode.calcTreeHeight(getKeyBitWidth()) - node.getPostLen();
		String pre = "";
		for (int i = 0; i < indent; i++) {
			pre += "-";
		}
		str.append(pre + "pl=" + node.getPostLen());
		str.append(";ec=" + node.getEntryCount());
		str.appendLn("; ID=" + node);
		
		long[] kdKey = new long[getKeyBitWidth()];
		for (int i = 0; i < (1<<NtNode.MAX_DIM); i++) {
			int pin = node.getPosition(i, NtNode.MAX_DIM);
			if (pin >= 0) {
				Object v = node.getEntryByPIN(pin, kdKey);
				if (v instanceof NtNode) {
					str.append(pre + i + " ");
					printTree(str, (NtNode<T>) v);
				} else {
					str.appendLn(pre + i + " " + Bits.toBinary(kdKey) + " v=" + v);
				}
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#queryWithMask(long, long)
	 */
	@Override
	public NtIteratorMask<T> queryWithMask(long minMask, long maxMask) {
		NtIteratorMask<T> it = new NtIteratorMask<>(getKeyBitWidth(), PersistenceProvider.NONE);
		it.reset(root, minMask, maxMask);
		return it;
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#query(long, long)
	 */
	@Override
	public PhIterator64<T> query(long min, long max) {
		NtIteratorMinMax<T> it = new NtIteratorMinMax<>(getKeyBitWidth(), PersistenceProvider.NONE);
		it.reset(root, min, max);
		return it;
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#iterator()
	 */
	@Override
	public PhIterator64<T> iterator() {
		NtIteratorMinMax<T> it = new NtIteratorMinMax<>(getKeyBitWidth(), PersistenceProvider.NONE);
		it.reset(root, Long.MIN_VALUE, Long.MAX_VALUE);
		return it;
	}
	
	/* (non-Javadoc)
	 * @see ch.ethz.globis.pht.v12.nt.NodeTree64#checkTree()
	 */
	@Override
	public boolean checkTree() {
		System.err.println("Not implemented: checkTree()");
		return true;
	}

	/**
	 * Collect tree statistics.
	 * @param node
	 * @param stats
	 */
	public static int getStats(NtNode<?> node, PhTreeStats stats, int dims, 
			List<Node> entryBuffer, int currentDepth, PersistenceProvider pp) {
		final int REF = 4;

		//Counter for NtNodes
		stats.nNtNodes++;

		// ba[] + values[] + kdKey[] + postLen + isAHC + entryCount
		stats.size += align8(12 + REF + REF + REF + 1 + 1 + 2);

		int nNodeEntriesFound = 0;
		Object[] data = node.values();
		for (int i = 0; i < data.length; i++) {
			byte ntSubCode = node.getNtSubCode(i);
			if (NtNode.isNtSubEmpty(ntSubCode)) {
				continue;
			}
			nNodeEntriesFound++;
			if (NtNode.isNtSubNode(ntSubCode)) {
				getStats((NtNode<?>) pp.resolveObject(data[i]), stats, dims, entryBuffer, 
						currentDepth, pp);
			} else {
				//subnode entry or postfix entry
				byte kdSubCode = node.getKdSubCode(i);
				if (Node.isSubNode(kdSubCode)) {
					entryBuffer.add((Node) pp.resolveObject(data[i]));
				} else if (!Node.isSubEmpty(kdSubCode)) {
					stats.q_nPostFixN[currentDepth]++;
				}
			}
		}
		if (nNodeEntriesFound != node.getEntryCount()) {
			System.err.println("WARNING: entry count mismatch: found/ntec=" + 
					nNodeEntriesFound + "/" + node.getEntryCount());
		}
		//count children
		//nChildren += node.getEntryCount();
		stats.size += 16 + align8(node.ba.length * 8);
		stats.size += 16 + align8(node.values().length * REF);
		stats.size += 16 + align8(node.kdKeys().length * 8);
		
		if (dims<=31 && node.getEntryCount() > (1L<<dims)) {
			System.err.println("WARNING: Over-populated node found: ntec=" + node.getEntryCount());
		}
		
		//check space
		int baS = node.calcArraySizeTotalBits(node.getEntryCount(), dims);
		baS = Bits.calcArraySize(baS);
		if (baS < node.ba.length) {
			System.err.println("Array too large in NT: " + node.ba.length + " - " + baS + " = " + 
					(node.ba.length - baS));
		}
		return nNodeEntriesFound;
	}
	
	public static class NtEntry12<T> extends NtEntry<T> {
		private byte kdSubCode;
		private byte ntSubCode;
		
		public NtEntry12(long key, long[] kdKey, byte kdSubCode, byte ntSubCode, T value) {
			super(key, kdKey, value);
			this.kdSubCode = kdSubCode;
			this.ntSubCode = ntSubCode;
		}
		
		public NtEntry12(NtEntry12<T> e) {
			super(e);
			this.kdSubCode = e.getKdSubCode();
			this.ntSubCode = e.getNtSubCode();
		}

		public byte getKdSubCode() {
			return kdSubCode;
		}

		public byte getNtSubCode() {
			return ntSubCode;
		}

		@Deprecated
		protected void set(long key, long[] kdKey, T value) {
			throw new UnsupportedOperationException();
		}

		@Deprecated
		public void setValue(T value) {
			throw new UnsupportedOperationException();
		}
		
		protected void set(long key, long[] kdKey, byte kdSubCode, byte ntSubCode, T value) {
			super.set(key, kdKey, value);
			this.kdSubCode = kdSubCode;
			this.ntSubCode = ntSubCode;
		}
		
		public void setValue(byte kdSubCode, byte ntSubCode, T value) {
			super.setValue(value);
			this.kdSubCode = kdSubCode;
			this.ntSubCode = ntSubCode;
		}

		public void reset() {
			setValue(Node.SUBCODE_EMPTY, Node.SUBCODE_EMPTY, null);
		}
	}
	
}
