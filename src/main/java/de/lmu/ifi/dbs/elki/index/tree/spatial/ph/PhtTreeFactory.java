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

import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.data.type.TypeInformation;
import de.lmu.ifi.dbs.elki.data.type.TypeUtil;
import de.lmu.ifi.dbs.elki.database.relation.Relation;
import de.lmu.ifi.dbs.elki.index.PagedIndexFactory;
import de.lmu.ifi.dbs.elki.persistent.PageFile;
import de.lmu.ifi.dbs.elki.persistent.PageFileFactory;
import de.lmu.ifi.dbs.elki.utilities.Alias;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.OptionID;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.constraints.CommonConstraints;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.DoubleParameter;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.IntParameter;

/**
 * Factory for regular R*-Trees.
 * 
 * @author Erich Schubert
 * @since 0.4.0
 * 
 * @apiviz.landmark factory
 * @apiviz.uses RStarTreeIndex oneway - - «create»
 * 
 * @param <O> Object type
 */
@Alias({"diskph", "ph"})
public class PhtTreeFactory<O extends NumberVector> extends PagedIndexFactory<O, PhtTree<O>> {
  
  private final PhtSettings settings;
  
  /**
   * Constructor.
   * 
   * @param pageFileFactory Data storage
   * @param settings Tree settings
   */
  public PhtTreeFactory(PageFileFactory<?> pageFileFactory, PhtSettings settings) {
    super(pageFileFactory);
    this.settings = settings;
  }

  @Override
  public PhtTree<O> instantiate(Relation<O> relation) {
    PageFile<PhtNode> pagefile = makePageFile(getNodeClass());
    return new PhtTree<>(relation, pagefile, settings);
  }

  protected Class<PhtNode> getNodeClass() {
    return PhtNode.class;
  }

  /**
   * Parameterization class.
   * 
   * @author Tilmann Zaeschke
   * 
   * @apiviz.exclude
   * 
   * @param <O> Object type
   */
  public static class Parameterizer<O extends NumberVector> extends PagedIndexFactory.Parameterizer<O> {
    /**
     * AHC/LHC bias. Optional.
     */
    public static OptionID AHC_LHC_BIAS_ID = 
        new OptionID("phtree.ahc_lhc_bias", "The AHC/LHC switching bias.");

    /**
     * NT threshold. Optional.
     */
    public static OptionID NT_THRESHOLD_ID = 
        new OptionID("phtree.nt_treshold", "The threshold for switching to NT nodes.");

    /**
     * Tree settings
     */
    protected PhtSettings settings;

    
    @Override
    protected PhtTreeFactory<O> makeInstance() {
      return new PhtTreeFactory<>(pageFileFactory, settings);
    }


    @Override
    protected void makeOptions(Parameterization config) {
      super.makeOptions(config);
      settings = new PhtSettings();
      DoubleParameter ahcLhcBiasP = new DoubleParameter(AHC_LHC_BIAS_ID, 2.0);
      ahcLhcBiasP.addConstraint(CommonConstraints.GREATER_THAN_ZERO_DOUBLE);
      //ahcLhcBiasP.addConstraint(CommonConstraints.LESS_THAN_HALF_DOUBLE);
      if(config.grab(ahcLhcBiasP)) {
        settings.ahcLhcBias = ahcLhcBiasP.getValue();
      }
      IntParameter ntThresholdP = new IntParameter(NT_THRESHOLD_ID, 150);
      //ntThresholdP.addConstraint(CommonConstraints.GREATER_THAN_ZERO_DOUBLE);
      //ntThresholdP.addConstraint(CommonConstraints.LESS_THAN_HALF_DOUBLE);
      if(config.grab(ntThresholdP)) {
        settings.ntThreshold = ntThresholdP.getValue();
      }
    }
  }

  @Override
  public TypeInformation getInputTypeRestriction() {
    return TypeUtil.NUMBER_VECTOR_FIELD;
  }
}