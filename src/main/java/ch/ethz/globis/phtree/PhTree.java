package ch.ethz.globis.phtree;

import java.util.List;

import ch.ethz.globis.phtree.util.PhIteratorBase;
import ch.ethz.globis.phtree.util.PhMapper;
import ch.ethz.globis.phtree.util.PhTreeStats;
import ch.ethz.globis.phtree.v11.PhTree11;
import ch.ethz.globis.phtree.v12.PhTree12;
import ch.ethz.globis.phtree.v8.PhTree8;

/**
 * k-dimensional index (quad-/oct-/n-tree).
 * Supports key/value pairs.
 *
 * See also : T. Zaeschke, C. Zimmerli, M.C. Norrie; 
 * "The PH-Tree -- A Space-Efficient Storage Structure and Multi-Dimensional Index", 
 * (SIGMOD 2014)
 *
 * http://www.phtree.org
 *
 * @author ztilmann (Tilmann Zaeschke)
 * 
 * @param <T> The value type of the tree 
 *
 */
public abstract class PhTree<T> {


	/**
	 * @return The number of entries in the tree
	 */
	public abstract int size();

	/**
	 * @return PH-Tree statistics
	 */
	public abstract PhTreeStats getStats();


  /**
   * Insert an entry associated with a k dimensional key.
	 * This will replace any entry that uses the same key.
   * @param key
   * @param value
   * @return the previously associated value or {@code null} if the key was found
   */
  public abstract T put(long[] key, T value);

	/**
	 * Checks whether a give key exists in the tree.
	 * @param key
	 * @return true if the key exists, otherwise false
	 */
  public abstract boolean contains(long ... key);

	/**
	 * Get an entry associated with a k dimensional key.
	 * @param key
	 * @return the associated value or {@code null} if the key was found
	 */
  public abstract T get(long ... key);


  /**
   * Remove the entry associated with a k dimensional key.
   * @param key
   * @return the associated value or {@code null} if the key was found
   */
  public abstract T remove(long... key);

	/**
	 * @return A string with a list of all entries in the tree.
	 */
  public abstract String toStringPlain();

	/**
	 * @return A string tree view of all entries in the tree.
	 */
  public abstract String toStringTree();

	/**
	 * @return an iterator over all entries in the tree
	 */
	public abstract PhExtent<T> queryExtent();


  /**
	 * Performs a rectangular window query. The parameters are the min and max keys which 
	 * contain the minimum respectively the maximum keys in every dimension.
	 * @param min Minimum values
	 * @param max Maximum values
   * @return Result iterator.
   */
  public abstract PhQuery<T> query(long[] min, long[] max);

	/**
	 * 
	 * @return the number of dimensions of the tree
	 */
	public abstract int getDim();

	/**
	 * 
	 * @return The bit depths for the tree. The latest versions will always return 64. 
	 */
	public abstract int getBitDepth();

  /**
   * Locate nearest neighbours for a given point in space.
	 * @param nMin number of entries to be returned. More entries may or may not be returned if 
	 * several points have the same distance.
   * @param key
   * @return The query iterator.
   */
	public abstract PhKnnQuery<T> nearestNeighbour(int nMin, long... key);

  /**
   * Locate nearest neighbours for a given point in space.
	 * @param nMin number of entries to be returned. More entries may or may not be returned if 
	 * several points have the same distance.
   * @param dist the distance function, can be {@code null}. The default is {@link PhDistanceL}.
   * @param dims the dimension filter, can be {@code null}
   * @param key
   * @return The query iterator.
   */
	public abstract PhKnnQuery<T> nearestNeighbour(int nMin, PhDistance dist, PhFilter dims, 
      long... key);

  /**
   * Find all entries within a given distance from a center point.
   * @param dist Maximum distance
   * @param center Center point
   * @return All entries with at most distance `dist` from `center`.
   */
	public abstract PhRangeQuery<T> rangeQuery(double dist, long... center);

  /**
   * Find all entries within a given distance from a center point.
   * @param dist Maximum distance
   * @param optionalDist Distance function, optional, can be `null`.
   * @param center Center point
   * @return All entries with at most distance `dist` from `center`.
   */
  public abstract PhRangeQuery<T> rangeQuery(double dist, PhDistance optionalDist, long... center);

  /**
   * Update the key of an entry. Update may fail if the old key does not exist, or if the new
   * key already exists.
   * @param oldKey
   * @param newKey
   * @return the value (can be {@code null}) associated with the updated key if the key could be 
   * updated, otherwise {@code null}.
   */
	public abstract T update(long[] oldKey, long[] newKey);

	/**
	 * Same as {@link #query(long[], long[])}, except that it returns a list
	 * instead of an iterator. This may be faster for small result sets. 
	 * @param min
	 * @param max
	 * @return List of query results
	 */
	public abstract List<PhEntry<T>> queryAll(long[] min, long[] max);

  /**
	 * Same as {@link #query(long[], long[])}, except that it returns a list
	 * instead of an iterator. This may be faster for small result sets. 
	 * @param min
	 * @param max
	 * @param maxResults
	 * @param filter
	 * @param mapper
	 * @return List of query results
	 */
	public abstract <R> List<R> queryAll(long[] min, long[] max, int maxResults, 
			PhFilter filter, PhMapper<T, R> mapper);

	/**
   * Create a new tree with the specified number of dimensions.
   * 
   * @param dim number of dimensions
   * @return PhTree
   */
  public static <T> PhTree<T> create(int dim) {
    return new PhTree12<>(dim);
  }

	/**
	 * Create a new tree with a configuration instance.
	 * 
	 * @param cfg configuration instance
	 * @return PhTree
	 */
	public static <T> PhTree<T> create(PhTreeConfig cfg) {
		return new PhTree12<>(cfg);
	}

	/**
	 * Interface for iterators that can reuse entries to avoid garbage collection. 
	 *
	 * @param <T>
	 */
  public static interface PhIterator<T> extends PhIteratorBase<long[], T, PhEntry<T>> {

    /**
     * Special 'next' method that avoids creating new objects internally by reusing Entry objects.
     * Advantage: Should completely avoid any GC effort.
     * Disadvantage: Returned PhEntries are not stable and are only valid until the
     * next call to next(). After that they may change state. Modifying returned entries may
     * invalidate the backing tree.
     * @return The next entry
     */
		@Override
    PhEntry<T> nextEntryReuse();
	}

	/**
	 * Interface for extents (query over all elements). The reset methods allows
	 * reusing the iterator.
	 * 
	 * @param <T>
	 */
	public static interface PhExtent<T> extends PhIterator<T> {

		/**
		 * Reset the extent iterator.
		 * @return the extent itself
		 */
		PhExtent<T> reset();
  }

	/**
	 * Interface for queries. The reset methods allows reusing the query.
	 * 
	 * @param <T>
	 */
  public static interface PhQuery<T> extends PhIterator<T> {

    /**
     * Reset the query with the new 'min' and 'max' boundaries.
     * @param min
     * @param max
     */
    void reset(long[] min, long[] max);
  }

	/**
	 * Interface for k nearest neighbor queries. The reset methods allows reusing the query.
	 * 
	 * @param <T>
	 */
	public static interface PhKnnQuery<T> extends PhIterator<T> {

    /**
     * Reset the query with the new parameters.
     * @param nMin Minimum result count
     * @param dist Distance function
     * @param center The point to find the nearest neighbours for
     * @return the query itself
     */
		PhKnnQuery<T> reset(int nMin, PhDistance dist, long... center);
  }

  /**
   * Clear the tree.
   */
  public abstract void clear();
}

