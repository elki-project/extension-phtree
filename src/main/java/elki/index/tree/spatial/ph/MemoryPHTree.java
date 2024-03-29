package elki.index.tree.spatial.ph;

import ch.ethz.globis.phtree.PhTreeF;
import ch.ethz.globis.phtree.PhTreeF.PhKnnQueryF;
import ch.ethz.globis.phtree.PhTreeF.PhRangeQueryF;
import ch.ethz.globis.phtree.pre.PreProcessorPointF;
import elki.data.NumberVector;
import elki.data.type.TypeInformation;
import elki.data.type.TypeUtil;
import elki.database.ids.DBID;
import elki.database.ids.DBIDIter;
import elki.database.ids.DBIDRef;
import elki.database.ids.DBIDUtil;
import elki.database.ids.DBIDs;
import elki.database.ids.KNNHeap;
import elki.database.ids.KNNList;
import elki.database.ids.ModifiableDoubleDBIDList;
import elki.database.query.distance.DistanceQuery;
import elki.database.query.knn.KNNSearcher;
import elki.database.query.range.RangeSearcher;
import elki.database.relation.Relation;
import elki.database.relation.RelationUtil;
import elki.distance.Distance;
import elki.distance.NumberVectorDistance;
import elki.distance.minkowski.LPNormDistance;
import elki.distance.minkowski.SquaredEuclideanDistance;
import elki.index.DynamicIndex;
import elki.index.IndexFactory;
import elki.index.KNNIndex;
import elki.index.RangeIndex;
import elki.logging.Logging;
import elki.logging.statistics.LongStatistic;
import elki.utilities.Alias;
import elki.utilities.documentation.Reference;
import elki.utilities.optionhandling.Parameterizer;

/**
 * Implementation of an in-memory PH-tree.
 *
 * @author Tilmann Zaeschke, Erich Schubert
 *
 * @apiviz.has PHTreeKNNQuery
 * @apiviz.has PHTreeRangeQuery
 *
 * @param <O> Vector type
 */
@Reference(authors = "T. Zaeschke, C. Zimmerli, M.C. Norrie", //
    title = "The PH-Tree -- A Space-Efficient Storage Structure and Multi-Dimensional Index", //
    booktitle = "Proc. Intl. Conf. on Management of Data (SIGMOD'14), 2014", //
    url = "https://doi.org/10.1145/361002.361007", bibkey = "DBLP:conf/sigmod/ZaschkeZN14")
public class MemoryPHTree<O extends NumberVector> implements DynamicIndex, KNNIndex<O>, RangeIndex<O> {
  /**
   * Class logger
   */
  private static final Logging LOG = Logging.getLogger(MemoryPHTree.class);

  /**
   * Indexed relation
   */
  private Relation<O> relation;

  /**
   * Distance computations performed.
   */
  private long distComputations = 0L;

  /**
   * The PH-Tree instance.
   */
  private final PhTreeF<DBID> tree;

  /**
   * The number of dimensions.
   */
  private int dims = -1;

  /**
   * Constructor.
   *
   * @param relation Relation to index
   */
  public MemoryPHTree(Relation<O> relation) {
    super();
    this.relation = relation;
    dims = RelationUtil.dimensionality(relation);
    // TODO
    // standard preprocessor
    // tree = PhTreeF.create(dims);
    // IntegerPP: about 20% faster, but slightly less accurate
    tree = PhTreeF.create(dims, new PreProcessorPointF.Multiply(100L * 1000L * 1000L));
  }

  @Override
  public void initialize() {
    for(DBIDIter iter = relation.getDBIDs().iter(); iter.valid(); iter.advance()) {
      O o = relation.get(iter);
      double[] v = new double[dims];
      for(int k = 0; k < dims; k++) {
        v[k] = o.doubleValue(k);
      }
      DBID id = DBIDUtil.deref(iter);
      tree.put(v, id);
    }
  }

  @Override
  public void logStatistics() {
    LOG.statistics(new LongStatistic(this.getClass().getName() + ".distance-computations", distComputations));
  }

  @Override
  public KNNSearcher<O> kNNByObject(DistanceQuery<O> distanceQuery, int maxk, int flags) {
    Distance<? super O> df = distanceQuery.getDistance();
    // TODO: if we know this works for other distance functions, add them, too!
    if(df instanceof LPNormDistance) {
      return new PHTreeKNNQuery((LPNormDistance) df);
    }
    if(df instanceof SquaredEuclideanDistance) {
      return new PHTreeKNNQuery((SquaredEuclideanDistance) df);
    }
    // if(df instanceof SparseLPNormDistance) {
    //  return new PHTreeKNNQuery((SparseLPNormDistance) df);
    // }
    return null;
  }

  @Override
  public RangeSearcher<O> rangeByObject(DistanceQuery<O> distanceQuery, double maxrange, int flags) {
    Distance<? super O> df = distanceQuery.getDistance();
    // TODO: if we know this works for other distance functions, add them, too!
    if(df instanceof LPNormDistance) {
      return new PHTreeRangeQuery((LPNormDistance) df);
    }
    if(df instanceof SquaredEuclideanDistance) {
      return new PHTreeRangeQuery((SquaredEuclideanDistance) df);
    }
    //if(df instanceof SparseLPNormDistance) {
    //  return new PHTreeRangeQuery((SparseLPNormDistance) df);
    //}
    return null;
  }

  /**
   * kNN query for the ph-tree.
   *
   * @author Tilmann Zaeschke
   */
  public class PHTreeKNNQuery implements KNNSearcher<O> {
    /**
     * Norm to use.
     */
    private final NumberVectorDistance<?> norm;

    /**
     * Norm wrapper.
     */
    private final PhNorm dist;

    /**
     * Query instance.
     */
    private final PhKnnQueryF<DBID> query;

    /**
     * Center point.
     */
    private final double[] center;

    /**
     * Constructor.
     *
     * @param norm Norm to use
     */
    public PHTreeKNNQuery(NumberVectorDistance<?> norm) {
      super();
      this.norm = norm;
      this.dist = new PhNorm(norm, dims, tree.getPreprocessor());
      this.center = new double[dims];
      // use 'k=0' to avoid executing a query here (center = {0,0,...})
      this.query = tree.nearestNeighbour(0, dist, new double[dims]);
    }

    @Override
    public KNNList getKNN(O obj, int k) {
      final KNNHeap knns = DBIDUtil.newHeap(k);

      oToDouble(obj, center);
      query.reset(k, dist, center);
      while(query.hasNext()) {
        DBID id = query.nextValue();
        O o2 = relation.get(id);
        knns.insert(norm.distance(obj, o2), id);
      }
      distComputations += dist.getAndResetDistanceCounter();
      return knns.toKNNList();
    }
  }

  /**
   * Range query for the ph-tree.
   */
  public class PHTreeRangeQuery implements RangeSearcher<O> {
    /**
     * Norm to use.
     */
    private final NumberVectorDistance<?> norm;

    /**
     * Norm wrapper.
     */
    private final PhNorm dist;

    /**
     * Query instance.
     */
    private PhRangeQueryF<DBID> query;

    /**
     * The query rectangle.
     */
    private final double[] mid;

    /**
     * Constructor.
     *
     * Returns all entries within a given distance of a given center. In
     * euclidean space this is a spherical query.
     *
     * @param norm Norm to use
     */
    public PHTreeRangeQuery(NumberVectorDistance<?> norm) {
      super();
      this.norm = norm;
      this.dist = new PhNorm(norm, dims, tree.getPreprocessor());
      this.mid = new double[dims];
    }

    @Override
    public ModifiableDoubleDBIDList getRange(O obj, double range, ModifiableDoubleDBIDList result) {
      oToDouble(obj, mid);
      range = Math.abs(range);

      long[] longCenter = new long[dims];
      tree.getPreprocessor().pre(mid, longCenter);

      if(query == null) {
        query = tree.rangeQuery(range, dist, mid);
      } else {
        query.reset(range, mid);
      }

      while(query.hasNext()) {
        DBID id = query.nextValue();
        O o2 = relation.get(id);
        double distance = norm.distance(obj, o2);
        result.add(distance, id);
      }
      result.sort();
      return result;
    }
  }

  /**
   * Factory class
   *
   * @author Tilmann Zaeschke
   *
   * @apiviz.stereotype factory
   * @apiviz.has MinimalisticMemoryPHTree
   *
   * @param <O> Vector type
   */
  @Alias({ "miniph", "ph" })
  public static class Factory<O extends NumberVector> implements IndexFactory<O> {
    /**
     * Constructor. Trivial parameterizable.
     */
    public Factory() {
      super();
    }

    @Override
    public MemoryPHTree<O> instantiate(Relation<O> relation) {
      return new MemoryPHTree<>(relation);
    }

    @Override
    public TypeInformation getInputTypeRestriction() {
      return TypeUtil.NUMBER_VECTOR_FIELD;
    }

    public static class Par implements Parameterizer {
      @Override
      public MemoryPHTree.Factory<NumberVector> make() {
        return new MemoryPHTree.Factory<>();
      }
    }
  }

  @Override
  public boolean delete(DBIDRef id) {
    O o = relation.get(id);
    return tree.remove(oToDouble(o, new double[dims])) != null;
  }

  @Override
  public void insert(DBIDRef id) {
    O o = relation.get(id);
    tree.put(oToDouble(o, new double[dims]), DBIDUtil.deref(id));
  }

  @Override
  public void deleteAll(DBIDs ids) {
    DBIDIter iter = ids.iter();
    for(; iter.valid(); iter.advance()) {
      delete(iter);
    }
  }

  @Override
  public void insertAll(DBIDs ids) {
    DBIDIter iter = ids.iter();
    for(; iter.valid(); iter.advance()) {
      insert(iter);
    }
  }

  private double[] oToDouble(O o, double[] v) {
    for(int k = 0; k < dims; k++) {
      v[k] = o.doubleValue(k);
    }
    return v;
  }
}
