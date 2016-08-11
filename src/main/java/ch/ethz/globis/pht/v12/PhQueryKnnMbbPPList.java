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

import java.util.Arrays;
import java.util.NoSuchElementException;

import ch.ethz.globis.pht.PersistenceProvider;
import ch.ethz.globis.pht.PhDistance;
import ch.ethz.globis.pht.PhEntry;
import ch.ethz.globis.pht.PhFilterDistance;
import ch.ethz.globis.pht.PhTree.PhExtent;
import ch.ethz.globis.pht.PhTree.PhKnnQuery;
import ch.ethz.globis.pht.v12.PhTree12.NodeEntry;

/**
 * kNN query implementation that uses preprocessors and distance functions.
 * 
 * The algorithm works as follows:
 * 
 * First we drill down in the tree to find an entry that is 'close' to
 * desired center of the kNN query. A 'close' entry is one that is in the same node
 * where the center would be, or in one of its sub-nodes. Note that we do not use
 * the center-point itself in case it exists in the tree. The result of the first step is 
 * a guess at the initial search distance (this would be 0 if we used the center itself). 
 * 
 * We then use a combination of rectangle query (center +/- initDistance) and distance-query. 
 * The query traverses only nodes and values that lie in the query rectangle and that satisfy the
 * distance requirement (circular distance when using euclidean space).
 * 
 * While iterating through the query result, we regularly sort the returned entries 
 * to see which distance would suffice to return 'k' result. If the new distance is smaller,
 * we adjust the query rectangle and the distance function before continuing the
 * query. As a result, when the query returns no more entries, we are guaranteed to
 * have all closest neighbours.
 * 
 * The only thing that can go wrong is that we may get less than 'k' neighbours if the
 * initial distance was too small. In that case we multiply the initial distance by 10
 * and run the algorithm again. Not that multiplying the distance by 10 means a 10^k fold
 * increase in the search volume. 
 *   
 *   
 * WARNING:
 * The query rectangle is calculated using the PhDistance.toMBB() method.
 * The implementation of this method may not work with non-euclidean spaces! 
 * 
 * @param <T> 
 */
public class PhQueryKnnMbbPPList<T> implements PhKnnQuery<T> {

	private final int dims;
	private int nMin;
	private PhTree12<T> pht;
	private final PersistenceProvider pp;
	private PhDistance distance;
	private int currentPos = -1;
	private final long[] mbbMin;
	private final long[] mbbMax;
	private final NodeIteratorListReuse<T, DistEntry<T>> iter;
	private final PhFilterDistance checker;
	private final KnnResultList results; 

	/**
	 * Create a new kNN/NNS search instance.
	 * @param pht
	 */
	public PhQueryKnnMbbPPList(PhTree12<T> pht) {
		this.dims = pht.getDim();
		this.mbbMin = new long[dims];
		this.mbbMax = new long[dims];
		this.pht = pht;
		this.pp = pht.getPersistenceProvider();
		this.checker = new PhFilterDistance();
		this.results = new KnnResultList(dims);
		this.iter = new NodeIteratorListReuse<>(dims, results, pp);
	}

	@Override
	public long[] nextKey() {
		return nextEntryReuse().getKey();
	}

	@Override
	public T nextValue() {
		return nextEntryReuse().getValue();
	}

	@Override
	public PhEntry<T> nextEntry() {
		return new PhEntry<>(nextEntryReuse());
	} 

	@Override
	public PhEntry<T> nextEntryReuse() {
		if (currentPos >= results.size()) {
			throw new NoSuchElementException();
		}
		return results.get(currentPos++);
	}

	@Override
	public boolean hasNext() {
		return currentPos < results.size();
	}

	@Override
	public T next() {
		return nextValue();
	}

	@Override
	public PhKnnQuery<T> reset(int nMin, PhDistance dist, long... center) {
		this.distance = dist == null ? this.distance : dist;
		this.nMin = nMin;
		
		if (nMin > 0) {
			results.reset(nMin, center);
			nearestNeighbourBinarySearch(center, nMin);
		} else {
			results.clear();
		}

		currentPos = 0;
		return this;
	}

	private void findKnnCandidate(long[] center, long[] ret) {
		findKnnCandidate(center, pht.getRoot(), ret);
	}

	private long[] findKnnCandidate(long[] key, Node node, long[] ret) {
		Object v = node.doIfMatching(key, true, null, null, null, pht);
		if (v == null) {
			//Okay, there is no perfect match:
			//just perform a query on the current node and return the first value that we find.
			return returnAnyValue(ret, key, node);
		}
		if (v instanceof Node) {
			return findKnnCandidate(key, (Node) v, ret);
		}

		//so we have a perfect match!
		//But we should return it only if nMin=1, otherwise our search area is too small.
		if (nMin == 1) {
			//Never return closest key if we look for nMin>1 keys!
			//now return the key, even if it may not be an exact match (we don't check)
			//TODO why is this necessary? we should have a complete 'ret' at this point...
			System.arraycopy(key, 0, ret, 0, key.length);
			return ret;
		}
		//Okay just perform a query on the current node and return the first value that we find.
		return returnAnyValue(ret, key, node);
	}

	private long[] returnAnyValue(long[] ret, long[] key, Node node) {
		//First, get correct prefix.
		long mask = (-1L) << (node.getPostLen()+1);
		for (int i = 0; i < dims; i++) {
			ret[i] = key[i] & mask;
		}
		
		
		//TODO use the following instead of the iterator below?!!?
		//TODO use the following instead of the iterator below?!!?
		//TODO use the following instead of the iterator below?!!?
		//TODO use the following instead of the iterator below?!!?
		//TODO use the following instead of the iterator below?!!?
		//TODO use the following instead of the iterator below?!!?
		//TODO use the following instead of the iterator below?!!?
		//The pin of the entry that we want to keep
//		int pin2 = -1;
//		long pos2 = -1;
//		Object val2 = null;
//		Object[] values = node.values();
//		if (node.isAHC()) {
//			for (int i = 0; i < (1<<key.length); i++) {
//				if (values[i] != null && i != pinToDelete) {
//					pin2 = i;
//					pos2 = i;
//					val2 = values[i];
//					break;
//				}
//			}
//		} else {
//			//LHC: we have only pos=0 and pos=1
//			pin2 = (pinToDelete == 0) ? 1 : 0;
//			int offs = pinToOffsBitsLHC(pin2, getBitPosIndex(), dims);
//			pos2 = Bits.readArray(node.ba, offs, IK_WIDTH(dims));
//			val2 = values[pin2];
//		}

		
		//TODO reuse
		//TODO reuse
		//TODO reuse
		//TODO reuse
		//TODO reuse
		NodeIteratorFullNoGC<T> ni = new NodeIteratorFullNoGC<>(dims, ret, pp);
		//This allows writing the result directly into 'ret'
		NodeEntry<T> result = new NodeEntry<>(ret, Node.SUBCODE_EMPTY, null);
		ni.init(node, null);
		while (ni.increment(result)) {
			if (result.node != null) {
				//traverse sub node
				ni.init((Node) pp.loadNode(result.node), null);
			} else {
				//Never return closest key if we look for nMin>1 keys!
				if (nMin > 1 && Arrays.equals(key, result.getKey())) {
					//Never return a perfect match if we look for nMin>1 keys!
					//otherwise the distance is too small.
					//This check should be cheap and will not be executed more than once anyway.
					continue;
				}
				return ret;
			}
		}
		throw new IllegalStateException();
	}

	/**
	 * This approach applies binary search to queries.
	 * It start with a query that covers the whole tree. Then whenever it finds an entry (the first)
	 * it discards the query and starts a smaller one with half the distance to the search-point.
	 * This effectively reduces the volume by 2^k.
	 * Once a query returns no result, it uses the previous query to traverse all results
	 * and find the nearest result.
	 * As an intermediate step, it may INCREASE the query size until a non-empty query appears.
	 * Then it could decrease again, like a true binary search.
	 * 
	 * When looking for nMin > 1, one could search for queries with at least nMin results...
	 * 
	 * @param val
	 * @param nMin
	 */
	private void nearestNeighbourBinarySearch(long[] val, int nMin) {
		//special case with minDist = 0
		if (nMin == 1 && pht.contains(val)) {
			DistEntry<T> e = results.getFreeEntry();
			e.set(val, pht.get(val), 0);
			checker.set(val, distance, Double.MAX_VALUE);
			results.phOffer(e);
			return;
		}

		//special case with size() <= nMin
		if (pht.size() <= nMin) {
			PhExtent<T> itEx = pht.queryExtent();
			while (itEx.hasNext()) {
				PhEntry<T> e = itEx.nextEntryReuse();
				DistEntry<T> e2 = results.getFreeEntry();
				e2.set(e, distance.dist(val, e.getKey()));
				checker.set(val, distance, Double.MAX_VALUE);
				results.phOffer(e2);
			}
			return;
		}

		//estimate initial distance
		long[] cand = new long[dims];
		findKnnCandidate(val, cand);
		double currentDist = distance.dist(val, cand);

		while (!findNeighbours(currentDist, nMin, val)) {
			//TODO *= nMin?
			currentDist *= 10;
		}
	}

	private final boolean findNeighbours(double maxDist, int nMin, long[] val) {
		results.maxDistance = maxDist;
		checker.set(val, distance, maxDist);
		distance.toMBB(maxDist, val, mbbMin, mbbMax);
		//TODO remove last parameter???
		iter.resetAndRun(pht.getRoot(), mbbMin, mbbMax, Integer.MAX_VALUE);

		if (results.size() < nMin) {
			//too small, we need a bigger range
			return false;
		}
		return true;
	}


	private static class DistEntry<T> extends NodeEntry<T> {
		double dist;

		DistEntry(long[] key, byte subCode, T value, double dist) {
			super(key, subCode, value);
			this.dist = dist;
		}

		DistEntry(NodeEntry<T> e, double dist) {
			super(e.getKey(), e.getSubCode(), e.getValue());
			this.dist = dist;
		}

		void set(PhEntry<T> e, double dist) {
			super.setValue(e.getValue());
			//TODO avoid arraycopy!?!?!? --> This case happens rarely...
			System.arraycopy(e.getKey(), 0, getKey(), 0, getKey().length);
			this.dist = dist;
		}
		
		void set(long[] key, T value, double dist) {
			setValue(value);
			//TODO avoid arraycopy!?!?!?  --> This case happens rarely...
			System.arraycopy(key, 0, getKey(), 0, getKey().length);
			this.dist = dist;
		}
	}


	private class KnnResultList extends PhResultList<T, DistEntry<T>> {
		private DistEntry<T>[] data;
		private DistEntry<T> free;
		private double[] distData;
		private int size = 0;
		//Maximum value below which new values will be accepted.
		//Rule: maxD=data[max] || maxD=Double.MAX_VALUE
		private double maxDistance = Double.MAX_VALUE;
		private double prevMaxDistance = maxDistance;
		private final int dims;
		private long[] center;
		
		KnnResultList(int dims) {
			this.free = new DistEntry<>(new long[dims], Node.SUBCODE_EMPTY, null, -1);
			this.dims = dims;
		}
		
		private DistEntry<T> createEntry() {
			return new DistEntry<>(new long[dims], Node.SUBCODE_EMPTY, null, 1);
		}
		
		@SuppressWarnings("unchecked")
		void reset(int newSize, long[] center) {
			size = 0;
			this.center = center;
			maxDistance = Double.MAX_VALUE;
			if (data == null) {
				data = new DistEntry[newSize];
				distData = new double[newSize];
				for (int i = 0; i < data.length; i++) {
					data[i] = createEntry();
				}
			}
			if (newSize != data.length) {
				int len = data.length;
				data = Arrays.copyOf(data, newSize);
				distData = new double[newSize];
				for (int i = len; i < newSize; i++) {
					data[i] = createEntry();
				}
			}
		}
		
		DistEntry<T> getFreeEntry() {
			DistEntry<T> ret = free;
			free = null;
			return ret;
		}

		@Override
		void phReturnTemp(PhEntry<T> entry) {
			if (free == null) {
				free = (DistEntry<T>) entry;
			}
		}
		
		@Override
		void phOffer(PhEntry<T> entry) {
			//TODO we don;t really need DistEntry anymore, do we? Maybe for external access of d?
			DistEntry<T> e = (DistEntry<T>) entry;
			double d = distance.dist(center, e.getKey());
			e.dist = d;
			if (d < maxDistance || (d <= maxDistance && size < data.length)) {
				boolean needsAdjustment = internalAdd(e);
				
				if (needsAdjustment) {
					double oldMaxD = maxDistance;
					maxDistance = distData[size-1];
					checker.setMaxDist(maxDistance);
					//This is an optimisation, seem to work for example for 10M/K3/CUBE
					//TODO we should compare with the distance when this was last changed!
					//TODO THIS work best with comparing to the CURRENT previous value, instead
					//     of using the one where we performed the last resize!!!!????
					//TODO 6 is chosen arbitrary, I only tested k3 and k10 with 10M-CUBE
					
					//TODO WHAT!!!?????? For nMin=1 we should not even get here!!!! (special case, see main method)
					if (dims < 6 || data.length > 1 || oldMaxD/maxDistance > 1.1) {
						//adjust minimum bounding box.
						distance.toMBB(maxDistance, center, mbbMin, mbbMax);
						//prevMaxDistance = oldMaxD;
					}
//					if (madXIstNew - maxDistOld > nodeWidth/2) {
//						//recalculate bit-ranges?
//						//-> Seems hardly worth the effort.
//						iter.adjustRanges();
//						iter.adjustMinMax();
//					}
					//Any call to this function is triggered by entry that ended up in the
					//candidate list. 
					//Therefore, none of its parent nodes can be fully excluded by the new MBB.
					//At best, we can exclude part of a parent if the search range slips
					//'below' the center-point of a node in at least one dimension. 
					//We basically need to compare each dimension, in which case we could 
					//as well recalculate the bit-range.
					
					//TODO
					//TODO
					//TODO
					//TODO Is this worth it?
					//TODO Try it out.
					//TODO THink about it.
					//TODO
					//TODO
					//TODO
					
					//We also may have to recalculate the parent node's min/max masks
					
					//This should definitely be worth for HIGH dimensions, because nodes
					//get very large, and excluding half of the quadrants should be worthwhile.
					
					//TODO This should DEFINITELY be done for persistent PH, because I/O may be
					// very expensive, so we should avoid every unnecessary sub-node traversal.
					
					//Trick: recalculate min/max-mask only if we find an entry that fails the 
					//filter? This indicates that some qudarants of the node may be 'wrong'.
					//Also: Only calculate masks AFTER we found at least K results (this is ensured
					//      by 'needsAdjustment').
					
					//Adjustment of masks (and parent mask) may be easier with a fully transposed 
					//tree, or not?
				}
				if (free == e) {
					free = createEntry();
				}
			} else {
				free = e;
			}
		}
		
		private boolean internalAdd(DistEntry<T> e) {
			if (size == 0) {
				free = data[size];
				data[size] = e;
				distData[size] = e.dist;
				size++;
				if (size == data.length) {
					return true;
				}
				return false;
			}
			if (e.dist > distData[size-1] && size == distData.length) {
				//this should never happen.
				throw new UnsupportedOperationException(e.dist + " > " + distData[size-1]);
			}

			if (size == data.length) {
				//We use -1 to allow using the same copy loop when inserting in the beginning
				for (int i = size-1; i >= -1; i--) {
					if (i==-1 || distData[i] < e.dist) {
						//purge and reuse last entry
						free = data[size-1];
						//insert after i
						for (int j = size-2; j >= i+1; j--) {
							data[j+1] = data[j];
							distData[j+1] = distData[j];
						}
						data[i+1] = e;
						distData[i+1] = e.dist;
						return true;
					}
				}
			} else {
				for (int i = size-1; i >= -1; i--) {
					if (i==-1 || distData[i] < e.dist) {
						//purge and reuse entry after last
						free = data[size];
						//insert after i
						for (int j = size-1; j >= i+1; j--) {
							data[j+1] = data[j];
							distData[j+1] = distData[j];
						}
						data[i+1] = e;
						distData[i+1] = e.dist;
						size++;
						if (size == data.length) {
							return true;
						}
						return false;
					}
				}
			}
			
			//This should never happen
			throw new IllegalStateException();
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public boolean isEmpty() {
			return size() == 0;
		}

		@Override
		public void clear() {
			size = 0;
		}

		@Override
		public DistEntry<T> get(int index) {
			if (index < 0 || index >= size) {
				throw new NoSuchElementException();
			}
			return data[index];
		}

		@Override
		DistEntry<T> phGetTempEntry() {
			return free;
		}

		@Override
		boolean phIsPrefixValid(long[] prefix, int bitsToIgnore) {
			long maskMin = (-1L) << bitsToIgnore;
			long maskMax = ~maskMin;
			long[] buf = new long[prefix.length];
			for (int i = 0; i < buf.length; i++) {
				//if v is outside the node, return distance to closest edge,
				//otherwise return v itself (assume possible distance=0)
				long min = prefix[i] & maskMin;
				long max = prefix[i] | maskMax;
				buf[i] = min > center[i] ? min : (max < center[i] ? max : center[i]); 
			}
			//TODO if buf==center -> no need to check distance 
			//TODO return true for dim < 3????
			return distance.dist(center, buf) <= maxDistance;
			//return checker.isValid(bitsToIgnore, prefix);
			//TODO
			//TODO
			//TODO
			//TODO
			//TODO
			//TODO
			//TODO
			//TODO
			//TODO
			//TODO
			//TODO
			//TODO
			//TODO
			//TODO
//			return true;
		}
	}
	
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
