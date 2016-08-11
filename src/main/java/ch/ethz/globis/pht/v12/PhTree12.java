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

import static ch.ethz.globis.pht.PhTreeHelper.align8;
import static ch.ethz.globis.pht.PhTreeHelper.debugCheck;
import static ch.ethz.globis.pht.PhTreeHelper.posInArray;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import ch.ethz.globis.pht.PersistenceProvider;
import ch.ethz.globis.pht.PhDistance;
import ch.ethz.globis.pht.PhDistanceL;
import ch.ethz.globis.pht.PhEntry;
import ch.ethz.globis.pht.PhFilter;
import ch.ethz.globis.pht.PhFilterDistance;
import ch.ethz.globis.pht.PhRangeQuery;
import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.PhTreeConfig;
import ch.ethz.globis.pht.PhTreeHelper;
import ch.ethz.globis.pht.util.PhMapper;
import ch.ethz.globis.pht.util.PhTreeStats;
import ch.ethz.globis.pht.util.StringBuilderLn;
import ch.ethz.globis.pht.v12.PhResultList.PhEntryFactory;
import ch.ethz.globis.pht.v12.nt.NodeTreeV12;
import ch.ethz.globis.pht.v12.nt.NtNode;

/**
 * n-dimensional index (quad-/oct-/n-tree).
 * 
 * Version 12: Store infixLen in parentNode -> better for I/O and possibly in-memory locality
 * 
 * Version 11: Use of NtTree for Nodes
 *             'null' values are replaced by NULL, this allows removal of AHC-exists bitmap
 *             Removal of recursion (and reimplementation) for get/insert/delete/update 
 * 
 * Version 10b: Moved infix into parent node.
 * 
 * Version 10: Store sub-nodes and postfixes in a common structure (one list/HC of key, one array)
 *             Advantages: much easier iteration through node, replacement of sub/post during 
 *             updates w/o bit shifting, can check infix without accessing sub-node (good for I/O).
 * 
 * Version 8b: Extended array pooling to all arrays
 * 
 * Version 8: Use 64bit depth everywhere. This should simplify a number of methods, especially
 *            regarding negative values.
 * 
 * Version 7: Uses PhEntry to store/return keys.
 *
 * Version 5: moved postCnt/subCnt into node.
 *
 * Version 4: Using long[] instead of int[]
 *
 * Version 3: This includes values for each key.
 *
 * Storage:
 * - classic: One node per combination of bits. Unused nodes can be cut off.
 * - use prefix-truncation -> a node may contain a series of unique bit combinations
 *
 * - To think about: Use optimised tree storage: 00=branch-down 01=branch-up 02=skip 03=ghost (after delete)
 *   -> 0=skip 10=down 11=up?
 *
 *
 * Hypercube: expanded byte array that contains 2^DIM references to sub-nodes (and posts, depending 
 * on implementation)
 * Linearization: Storing Hypercube as paired array of index<->non_null_reference 
 *
 * See also : T. Zaeschke, C. Zimmerli, M.C. Norrie; 
 * "The PH-Tree -- A Space-Efficient Storage Structure and Multi-Dimensional Index", 
 * (SIGMOD 2014)
 *
 * @author ztilmann (Tilmann Zaeschke)
 * 
 * @param <T> The value type of the tree 
 *
 */
public class PhTree12<T> extends PhTree<T> {

	//Enable HC incrementer / iteration
	public static final boolean HCI_ENABLED = true; 
	//Enable AHC mode in nodes
	static final boolean AHC_ENABLED = true; 
	
	//Enable persistence mode, i.e. pages are converted to and from peristent identifiers
	public static final boolean PERS_ENABLED = false;
	private PersistenceProvider pp = 
			PERS_ENABLED ? new PersProviderOStr() : PersistenceProvider.NONE;
	
	//This threshold is used to decide during query iteration whether the first value
	//should be found by binary search or by full scan.
	public static final int LHC_BINARY_SEARCH_THRESHOLD = 50;
	
	static final int DEPTH_64 = 64;
	
	private static final int NO_INSERT_REQUIRED = Integer.MAX_VALUE;


	//Dimension. This is the number of attributes of an entity.
	private final int dims;

	private final AtomicInteger nEntries = new AtomicInteger();
	
	private Object rootId = null;

	
	/**
	 * @param <T>
	 */
	public static class NodeEntry<T> extends PhEntry<T> {
		Object node;
		byte subCode;

		NodeEntry(long[] key, byte subCode, T value) {
			super(key, value);
			this.node = null;
			this.subCode = subCode;
		}

		public void setNodeKeepKey(byte subCode, Object node) {
			set(this.getKey(), null);
			this.node = node;
			this.subCode = subCode;
		}
		
		void setPost(byte subCode, T val) {
			setValue(val);
			this.node = null;
			this.subCode = subCode;
		}
		 
		public void setSubCode(byte subCode) {
			this.subCode = subCode;
		}
		 
		byte getSubCode() {
			return subCode;
		}

		public void reset() {
			subCode = Node.SUBCODE_EMPTY;
			setValue(null);
		}
	}

	Node getRoot() {
		return (Node) pp.loadNode(rootId);
    }

	public PhTree12(int dim) {
		dims = dim;
		debugCheck();
	}

	public PhTree12(PhTreeConfig cfg) {
		dims = cfg.getDimActual();
		pp = cfg.getPersistenceProvider();
		pp.writeTree(this, dims);
		debugCheck();
	}

	public PhTree12(int dims, int nEntries, Object rootId, PersistenceProvider pp) {
		this.dims = dims;
		this.nEntries.set(nEntries);
		this.rootId = rootId;
		this.pp = pp;
	}
	
	void increaseNrEntries() {
		nEntries.incrementAndGet();
		pp.updateTree(this, dims, nEntries.get(), rootId);
	}

	void decreaseNrEntries() {
		nEntries.decrementAndGet();
		pp.updateTree(this, dims, nEntries.get(), rootId);
	}

	@Override
	public int size() {
		return nEntries.get();
	}

	@Override
	public PhTreeStats getStats() {
		return getStats(0, getRoot(), new PhTreeStats(DEPTH_64));
	}

	private PhTreeStats getStats(int currentDepth, Node node, PhTreeStats stats) {
		stats.nNodes++;
		if (node.isAHC()) {
			stats.nAHC++;
		}
		if (node.isNT()) {
			stats.nNT++;
		}
		int infixLen = 63-currentDepth-node.getPostLen();
		stats.infixHist[infixLen]++;
		stats.nodeDepthHist[currentDepth]++;
		int size = node.getEntryCount();
		stats.nodeSizeLogHist[32-Integer.numberOfLeadingZeros(size)]++;
		
		currentDepth += infixLen;
		stats.q_totalDepth += currentDepth;

		if (node.values() != null) {
			Object[] data = node.values();
			for (int i = 0; i < data.length; i++) {
				byte subCode = node.getSubCode(i);
				if (Node.isSubNode(subCode)) {
					getStats(currentDepth + 1, (Node) pp.loadNode(data[i]), stats);
				} else if (!Node.isSubEmpty(subCode)) {
					stats.q_nPostFixN[currentDepth]++;
				}
			}
		} else {
			List<Node> entries = new ArrayList<>();
			int nEntries = NodeTreeV12.getStats(node.ind(), stats, dims, entries, currentDepth, pp);
			for (Object child: entries) {
				getStats(currentDepth + 1, (Node) child, stats);
			}
			if (nEntries != node.ntGetSize()) {
				System.err.println("WARNING: entry count mismatch: a-found/ec=" + 
						nEntries + "/" + node.getEntryCount());
			}
		}
		
		final int REF = 4;//bytes for a reference
		// this +  value[] + ba[] + ind() + isHC + postLen + infLen + nEntries
		stats.size += align8(12 + REF + REF + REF + 1 + 1 + 1 + 4);
		//count children
		int nChildren = node.getEntryCount();
		stats.size += 16 + align8(Bits.arraySizeInByte(node.ba));
		stats.size += node.values() != null ? 16 + align8(node.values().length * REF) : 0;
		if (nChildren == 1 && (node != getRoot()) && nEntries.get() > 1) {
			//This should not happen! Except for a root node if the tree has <2 entries.
			logErr("WARNING: found lonely node...");
		}
		if (nChildren == 0 && (node != getRoot())) {
			//This should not happen! Except for a root node if the tree has <2 entries.
			logErr("WARNING: found ZOMBIE node...");
		}
		if (dims<=31 && node.getEntryCount() > (1L<<dims)) {
			logErr("WARNING: Over-populated node found: ec=" + node.getEntryCount());
		}
		//check space
		int baS = node.calcArraySizeTotalBits(node.getEntryCount(), dims);
		baS = Bits.calcArraySize(baS);
		if (baS < node.ba.length) {
			logErr("Array too large: " + node.ba.length + " - " + baS + " = " + 
					(node.ba.length - baS));
		}
		stats.nTotalChildren += nChildren;
		
		return stats;
	}


	@SuppressWarnings("unchecked")
	@Override
	public T put(long[] key, T value) {
		Object nonNullValue = value == null ? PhTreeHelper.NULL : value;
		if (getRoot() == null) {
			insertRoot(key, nonNullValue);
			return null;
		}

		Object o = getRoot();
			Node currentNode = (Node) o;
		while (o instanceof Node) {
			currentNode = (Node) o;
			o = currentNode.doInsertIfMatching(key, nonNullValue, this);
		}
		pp.updateNode(currentNode);
		return (T) o;
    }

    void insertRoot(long[] key, Object value) {
        Node root = Node.createNode(dims, DEPTH_64-1);
        //calcPostfixes(valueSet, root, 0);
        long pos = posInArray(key, root.getPostLen());
        root.addPostPIN(pos, -1, key, value, getPersistenceProvider());
        rootId = pp.registerNode(root);
        increaseNrEntries();
    }

	@SuppressWarnings("unchecked")
	@Override
	public boolean contains(long... key) {
		Object o = getRoot();
		while (o instanceof Node) {
			Node currentNode = (Node) o;
			o = currentNode.doIfMatching(key, true, null, null, null, this);
		}
		return (T) o != null;
	}


	@SuppressWarnings("unchecked")
	@Override
	public T get(long... key) {
		Object o = getRoot();
		while (o instanceof Node) {
			Node currentNode = (Node) o;
			o = currentNode.doIfMatching(key, true, null, null, null, this);
		}
		return o == PhTreeHelper.NULL ? null : (T) o;
	}


	/**
	 * A value-set is an object with n=DIM values.
	 * @param key
	 * @return true if the value was found
	 */
	@SuppressWarnings("unchecked")
	@Override
	public T remove(long... key) {
		Object o = getRoot();
		Node currentNode = (Node) o;
		Node parentNode = null;
		while (o instanceof Node) {
			currentNode = (Node) o;
			o = currentNode.doIfMatching(key, false, parentNode, null, null, this);
			parentNode = currentNode;
		}
		pp.updateNode(currentNode);
		//TODO update parent node!!!
		//TODO update parent node!!!
		//TODO update parent node!!!
		//TODO update parent node!!!
		//TODO update parent node!!!
		//TODO update parent node!!!
		//TODO update parent node!!!
		//TODO update parent node!!!
		return (T) o;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T update(long[] oldKey, long[] newKey) {
		Node[] stack = new Node[64];
		int stackSize = 0;
		
		Object o = getRoot();
		Node parentNode = null;
		final int[] insertRequired = new int[]{NO_INSERT_REQUIRED};
		while (o instanceof Node) {
			Node currentNode = (Node) o;
			stack[stackSize++] = currentNode;
			o = currentNode.doIfMatching(oldKey, false, parentNode, newKey, insertRequired, this);
			parentNode = currentNode;
		}
		
		Object value = o == PhTreeHelper.NULL ? null : o;

		//traverse the tree from bottom to top
		//this avoids extracting and checking infixes.
		if (insertRequired[0] != NO_INSERT_REQUIRED) {
			//ignore lowest node
			if (stack[stackSize-1].getEntryCount() == 0) {
				//The node may have been deleted
				stackSize--;
			}
			while (stackSize > 0) {
				if (stack[--stackSize].getPostLen()+1 >= insertRequired[0]) {
					o = stack[stackSize];
					while (o instanceof Node) {
						Node currentNode = (Node) o;
						o = currentNode.doInsertIfMatching(newKey, value, this);
					}
					insertRequired[0] = NO_INSERT_REQUIRED;
					break;
				}
			}
		}		
		
		return (T) value;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + 
				" AHC/LHC=" + Node.AHC_LHC_BIAS +  
				" AHC-on=" + AHC_ENABLED +  
				" HCI-on=" + HCI_ENABLED +  
				" NtLimit=" + Node.NT_THRESHOLD +  
				" NtMaxDim=" + NtNode.MAX_DIM +
				" I/O=" + pp.getDescription() +
				" DEBUG=" + PhTreeHelper.DEBUG;
	}

	@Override
	public String toStringPlain() {
		StringBuilderLn sb = new StringBuilderLn();
		if (getRoot() != null) {
			toStringPlain(sb, getRoot(), new long[dims]);
		}
		return sb.toString();
	}

	private void toStringPlain(StringBuilderLn sb, Node node, long[] key) {
		for (int i = 0; i < 1L << dims; i++) {
			Object o = node.getEntry(i, key, this.getPersistenceProvider());
			if (o == null) {
				continue;
			}
			//inner node?
			if (o instanceof Node) {
				toStringPlain(sb, (Node) o, key);
			} else {
				sb.append(Bits.toBinary(key, DEPTH_64));
				sb.appendLn("  v=" + o);
			}
		}
	}


	@Override
	public String toStringTree() {
		StringBuilderLn sb = new StringBuilderLn();
		if (getRoot() != null) {
			toStringTree(sb, 0, getRoot(), new long[dims], true);
		}
		return sb.toString();
	}

	private void toStringTree(StringBuilderLn sb, int currentDepth, Node node, long[] key, 
			boolean printValue) {
		String ind = "*";
		for (int i = 0; i < currentDepth; i++) {
			ind += "-";
		}
		int infixLen = 63-currentDepth-node.getPostLen();
		sb.append( ind + "il=" + infixLen + " pl=" + (node.getPostLen()) + 
				" ec=" + node.getEntryCount() +
				" mode=" + (node.isAHC() ? "AHC" : node.ind() == null ? "LHC" : "NT") + 
				" inf=[");

		//for a leaf node, the existence of a sub just indicates that the value exists.
		if (infixLen > 0) {
			long mask = (-1L) << infixLen;
			mask = ~mask;
			mask <<= node.getPostLen()+1;
			for (int i = 0; i < dims; i++) {
				sb.append(Bits.toBinary(key[i] & mask) + ",");
			}
		}
		currentDepth += infixLen;
		sb.appendLn("] (id=" + System.identityHashCode(node) + ") " + node);

		//To clean previous postfixes.
		for (int i = 0; i < 1L << dims; i++) {
			Object o = node.getEntry(i, key, this.getPersistenceProvider());
			if (o == null) {
				continue;
			}
			if (o instanceof Node) {
				sb.appendLn(ind + "# " + i + "  +");
				toStringTree(sb, currentDepth + 1, (Node) o, key, printValue);
			}  else {
				//post-fix
				sb.append(ind + Bits.toBinary(key, DEPTH_64));
				sb.append("  hcPos=" + i);
				if (printValue) {
					sb.append("  v=" + o);
				}
				sb.appendLn("");
			}
		}
	}


	@Override
	public PhExtent<T> queryExtent() {
		return new PhIteratorFullNoGC<T>(this, null).reset();
	}


	/**
	 * Performs a rectangular window query. The parameters are the min and max keys which 
	 * contain the minimum respectively the maximum keys in every dimension.
	 * @param min Minimum values
	 * @param max Maximum values
	 * @return Result iterator.
	 */
	@Override
	public PhQuery<T> query(long[] min, long[] max) {
		if (min.length != dims || max.length != dims) {
			throw new IllegalArgumentException("Invalid number of arguments: " + min.length +  
					" / " + max.length + "  DIM=" + dims);
		}
		//PhQuery<T> q = new PhIteratorHighK<T>(this, null);
		PhQuery<T> q = new PhIteratorNoGC<>(this, null);
		q.reset(min, max);
		return q;
	}

	/**
	 * Performs a rectangular window query. The parameters are the min and max keys which 
	 * contain the minimum respectively the maximum keys in every dimension.
	 * @param min Minimum values
	 * @param max Maximum values
	 * @return Result list.
	 */
	@Override
	public List<PhEntry<T>> queryAll(long[] min, long[] max) {
		return queryAll(min, max, Integer.MAX_VALUE, null, PVMAPPER);
	}

  private final PhMapper<T, PhEntry<T>> PVMAPPER = new PhMapper<T, PhEntry<T>>() {
    /**  */
    private static final long serialVersionUID = 1L;

    @Override
    public PhEntry<T> map(PhEntry<T> e) {
      return e;
    }
  }; 
	
  private final PhEntryFactory<T> PEFACTORY = new PhEntryFactory<T>() {
    @Override
    public PhEntry<T> create() {
      return new NodeEntry<T>(new long[dims], Node.SUBCODE_EMPTY, null);
    }
  };
  
	/**
	 * Performs a rectangular window query. The parameters are the min and max keys which 
	 * contain the minimum respectively the maximum keys in every dimension.
	 * @param min Minimum values
	 * @param max Maximum values
	 * @return Result list.
	 */
	@Override
	public <R> List<R> queryAll(long[] min, long[] max, int maxResults, 
			PhFilter filter, PhMapper<T, R> mapper) {
		if (min.length != dims || max.length != dims) {
			throw new IllegalArgumentException("Invalid number of arguments: " + min.length +  
					" / " + max.length + "  DIM=" + dims);
		}
		
		if (getRoot() == null) {
			return new ArrayList<>();
		}
		
//		if (filter == null) {
//			filter = null;
//		}
//		if (mapper == null) {
//			mapper = (PhMapper<T, R>) PhMapper.PVENTRY();
//		}
		
		PhResultList<T, R> list = new PhResultList.MappingResultList<>(null, mapper, PEFACTORY);
		
		NodeIteratorListReuse<T, R> it = 
				new NodeIteratorListReuse<>(dims, list, getPersistenceProvider());
		return it.resetAndRun(getRoot(), min, max, maxResults);
	}

	@Override
	public int getDim() {
		return dims;
	}

	@Override
	public int getBitDepth() {
		return PhTree12.DEPTH_64;
	}

	/**
	 * Locate nearest neighbours for a given point in space.
	 * @param nMin number of values to be returned. More values may or may not be returned when 
	 * several have	the same distance.
	 * @param v
	 * @return Result iterator.
	 */
	@Override
	public PhKnnQuery<T> nearestNeighbour(int nMin, long... v) {
		//return new PhQueryKnnMbbPP<T>(this).reset(nMin, PhDistanceL.THIS, v);
		return new PhQueryKnnMbbPPList<T>(this).reset(nMin, PhDistanceL.THIS, v);
	}

	@Override
	public PhKnnQuery<T> nearestNeighbour(int nMin, PhDistance dist,
			PhFilter dimsFilter, long... center) {
		//return new PhQueryKnnMbbPP<T>(this).reset(nMin, dist, center);
		return new PhQueryKnnMbbPPList<T>(this).reset(nMin, dist, center);
	}

	@Override
	public PhRangeQuery<T> rangeQuery(double dist, long... center) {
		return rangeQuery(dist, null, center);
	}

	@Override
	public PhRangeQuery<T> rangeQuery(double dist, PhDistance optionalDist, long...center) {
		PhFilterDistance filter = new PhFilterDistance();
		if (optionalDist == null) {
			optionalDist = PhDistanceL.THIS;
		}
		filter.set(center, optionalDist, dist);
		PhQuery<T> q = new PhIteratorNoGC<>(this, filter);
		PhRangeQuery<T> qr = new PhRangeQuery<>(q, this, optionalDist, filter);
		qr.reset(dist, center);
		return qr;
	}

	/**
	 * Remove all entries from the tree.
	 */
	@Override
	public void clear() {
		rootId = null;
		nEntries.set(0);
		pp.updateTree(this, dims, nEntries.get(), rootId);
	}

	void adjustCounts(int deletedPosts) {
		nEntries.addAndGet(-deletedPosts);
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

	public static <T> void writeExternal(ObjectOutput out, PhTree12<T> tree) throws IOException {
		out.writeInt(tree.nEntries.get());
		out.writeInt(tree.dims);
		out.writeInt((Integer)tree.rootId);
	}

	public static <T> PhTree<T> readExternal(ObjectInput in) throws IOException, 
	ClassNotFoundException {
		int n = in.readInt();
		int dims = in.readInt();
		PhTree12<T> tree = new PhTree12<>(dims);
		tree.nEntries.set(n);
		tree.rootId = in.readInt();
		return tree;
	}
	

	public PersistenceProvider getPersistenceProvider() {
		return pp;
	}
	
	private void logErr(String str) {
		System.err.println(str);
	}
}

