PH-Tree add-on for ELKI
=======================

This is an extension for the
[ELKI Data Mining Toolkit](http://elki.dbs.ifi.lmu.de/)
to use the PH-tree index.

* [ELKI on GitHub](https://github.com/elki-project/elki/)
* [PH-Tree on GitHub](https://github.com/tzaeschke/phtree)

Usage
-----

Build the package using Maven `mvn package`, then add the resulting
`target/elki-phtree-1.0.jar` to your ELKI class path.

To use the ph-tree, set the `-db.index` parameter to
`tree.spatial.ph.MemoryPHTree` and use an algorithm using
Euclidean distance (other Minkowski norms and squared Euclidean distance are
also supported), and either radius or k-nearest-neighbor queries.

About the PH-Tree
-----------------

The PH-tree is a multi-dimensional indexing and storage structure.
By default it stores k-dimensional keys (points) consisting of k 64bit-integers. However, it can also be used
to efficiently store floating point values or k-dimensional rectangles.
It supports kNN queries, range queries, window queries and fast update/moving of individual entries.

The PH-tree was developed at ETH Zurich and first published in:
"The PH-Tree: A Space-Efficient Storage Structure and Multi-Dimensional Index" ([PDF](http://globis.ethz.ch/?pubdownload=699)), 
Tilmann Zäschke, Christoph Zimmerli and Moira C. Norrie, 
Proceedings of Intl. Conf. on Management of Data (SIGMOD), 2014

The current version of the PH-tree is discussed in more detail here: ([PDF](https://github.com/tzaeschke/phtree/blob/master/PhTreeRevisited.pdf)) (2015).

Contact:
{zaeschke,zimmerli,norrie)@inf.ethz.ch

