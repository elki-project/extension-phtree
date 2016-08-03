package de.lmu.ifi.dbs.elki.index.tree.spatial.ph;

/*
 This file is part of ELKI:
 Environment for Developing KDD-Applications Supported by Index-Structures

 Copyright (C) 2015
 Ludwig-Maximilians-Universität München
 Lehr- und Forschungseinheit für Datenbanksysteme
 ELKI Development Team

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

import de.lmu.ifi.dbs.elki.index.tree.spatial.SpatialDirectoryEntry;
import de.lmu.ifi.dbs.elki.index.tree.spatial.SpatialEntry;
import de.lmu.ifi.dbs.elki.index.tree.spatial.rstarvariants.AbstractRTreeSettings;
import de.lmu.ifi.dbs.elki.index.tree.spatial.rstarvariants.NonFlatRStarTree;
import de.lmu.ifi.dbs.elki.logging.Logging;
import de.lmu.ifi.dbs.elki.persistent.PageFile;
import de.lmu.ifi.dbs.elki.utilities.documentation.Description;
import de.lmu.ifi.dbs.elki.utilities.documentation.Reference;
import de.lmu.ifi.dbs.elki.utilities.documentation.Title;

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
@Title("PH-Tree")
@Description("Prefix sharing hypercube tree.")
@Reference(authors = "T. Zaeschke, C. Zimmerli, M.C. Norrie", title = "The PH-Tree -- A Space-Efficient Storage Structure and Multi-Dimensional Index", booktitle = "Proc. Intl. Conf. on Management of Data (SIGMOD'14), 2014", url = "http://dx.doi.org/10.1145/361002.361007")
public abstract class PHTree extends NonFlatRStarTree<PHTreeNode, SpatialEntry, AbstractRTreeSettings> {
  /**
   * The logger for this class.
   */
  private static final Logging LOG = Logging.getLogger(PHTree.class);

  /**
   * Constructor.
   *
   * @param pagefile Page file
   * @param settings Settings class
   */
  public PHTree(PageFile<PHTreeNode> pagefile, AbstractRTreeSettings settings) {
    super(pagefile, settings);
  }

  @Override
  protected SpatialEntry createRootEntry() {
    return new SpatialDirectoryEntry(0, null);
  }

  @Override
  protected SpatialEntry createNewDirectoryEntry(PHTreeNode node) {
    return new SpatialDirectoryEntry(node.getPageID(), node.computeMBR());
  }

  /**
   * Creates a new leaf node with the specified capacity.
   *
   * @return a new leaf node
   */
  @Override
  protected PHTreeNode createNewLeafNode() {
    return new PHTreeNode(leafCapacity, true);
  }

  /**
   * Creates a new directory node with the specified capacity.
   *
   * @return a new directory node
   */
  @Override
  protected PHTreeNode createNewDirectoryNode() {
    return new PHTreeNode(dirCapacity, false);
  }

  @Override
  protected Logging getLogger() {
    return LOG;
  }
}