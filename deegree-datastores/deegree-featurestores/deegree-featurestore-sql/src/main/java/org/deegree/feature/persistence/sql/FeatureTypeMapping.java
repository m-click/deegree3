//$HeadURL$
/*----------------------------------------------------------------------------
 This file is part of deegree, http://deegree.org/
 Copyright (C) 2001-2009 by:
 - Department of Geography, University of Bonn -
 and
 - lat/lon GmbH -

 This library is free software; you can redistribute it and/or modify it under
 the terms of the GNU Lesser General Public License as published by the Free
 Software Foundation; either version 2.1 of the License, or (at your option)
 any later version.
 This library is distributed in the hope that it will be useful, but WITHOUT
 ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 details.
 You should have received a copy of the GNU Lesser General Public License
 along with this library; if not, write to the Free Software Foundation, Inc.,
 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

 Contact information:

 lat/lon GmbH
 Aennchenstr. 19, 53177 Bonn
 Germany
 http://lat-lon.de/

 Department of Geography, University of Bonn
 Prof. Dr. Klaus Greve
 Postfach 1147, 53001 Bonn
 Germany
 http://www.geographie.uni-bonn.de/deegree/

 e-mail: info@deegree.org
 ----------------------------------------------------------------------------*/
package org.deegree.feature.persistence.sql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.deegree.commons.jdbc.SQLIdentifier;
import org.deegree.commons.jdbc.TableName;
import org.deegree.commons.tom.gml.property.PropertyType;
import org.deegree.commons.utils.Pair;
import org.deegree.feature.persistence.sql.expressions.TableJoin;
import org.deegree.feature.persistence.sql.id.FIDMapping;
import org.deegree.feature.persistence.sql.rules.CompoundMapping;
import org.deegree.feature.persistence.sql.rules.GeometryMapping;
import org.deegree.feature.persistence.sql.rules.Mapping;
import org.deegree.feature.types.FeatureType;

/**
 * Defines the mapping between a {@link FeatureType} and tables in a relational database.
 * 
 * @author <a href="mailto:schneider@lat-lon.de">Markus Schneider</a>
 * @author last edited by: $Author$
 * 
 * @version $Revision$, $Date$
 */
public class FeatureTypeMapping {

    private final QName ftName;

    private final TableName table;

    private final FIDMapping fidMapping;

    private final Map<QName, Mapping> propToMapping;

    private final List<Mapping> particles = new ArrayList<Mapping>();

    private SQLIdentifier typeColumn;

    /**
     * Creates a new {@link FeatureTypeMapping} instance.
     * 
     * @param ftName
     *            name of the mapped feature type, must not be <code>null</code>
     * @param table
     *            name of the database table that the feature type is mapped to, must not be <code>null</code>
     * @param fidMapping
     *            mapping for the feature id, must not be <code>null</code>
     * @param particleMappings
     *            particle mappings for the feature type, must not be <code>null</code>
     * @param typeColumn
     *            column that stores the name of the feature type, can be <code>null</code> (not stored)
     */
    public FeatureTypeMapping( QName ftName, TableName table, FIDMapping fidMapping, List<Mapping> particleMappings,
                               SQLIdentifier typeColumn ) {
        this.ftName = ftName;
        this.table = table;
        this.fidMapping = fidMapping;
        this.propToMapping = new LinkedHashMap<QName, Mapping>();
        // TODO cope with non-QName XPaths as well
        for ( Mapping mapping : particleMappings ) {
            if ( mapping != null && mapping.getPath().getAsQName() != null ) {
                propToMapping.put( mapping.getPath().getAsQName(), mapping );
            }
        }
        for ( Mapping mapping : particleMappings ) {
            if ( mapping != null ) {
                this.particles.add( mapping );
            }
        }
        this.typeColumn = typeColumn;
    }

    /**
     * Returns the name of the feature type.
     * 
     * @return name of the feature type, never <code>null</code>
     */
    public QName getFeatureType() {
        return ftName;
    }

    /**
     * Returns the identifier of the table that the feature type is mapped to.
     * 
     * @return identifier of the table, never <code>null</code>
     */
    public TableName getFtTable() {
        return table;
    }

    /**
     * Returns the feature id mapping.
     * 
     * @return mapping for the feature id, never <code>null</code>
     */
    public FIDMapping getFidMapping() {
        return fidMapping;
    }

    /**
     * Returns the mapping parameters for the specified property.
     * 
     * @param propName
     *            name of the property, must not be <code>null</code>
     * @return mapping, may be <code>null</code> (if the property is not mapped)
     */
    @Deprecated
    public Mapping getMapping( QName propName ) {
        return propToMapping.get( propName );
    }

    /**
     * Returns the {@link Mapping} particles.
     * 
     * @return mapping particles, may be empty, but never <code>null</code>
     */
    public List<Mapping> getMappings() {
        return particles;
    }

    /**
     * Returns the default (i.e. the first) {@link GeometryMapping}.
     * 
     * @return default geometry mapping, may be <code>null</code> (no geometry mapping defined)
     */
    public Pair<TableName, GeometryMapping> getDefaultGeometryMapping() {
        TableName table = getFtTable();
        for ( Mapping particle : particles ) {
            if ( particle instanceof GeometryMapping ) {
                List<TableJoin> joins = particle.getJoinedTable();
                if ( joins != null && !joins.isEmpty() ) {
                    table = joins.get( joins.size() - 1 ).getToTable();
                }
                return new Pair<TableName, GeometryMapping>( table, (GeometryMapping) particle );
            }
        }
        for ( Mapping particle : particles ) {
            TableName propTable = table;
            if ( particle instanceof CompoundMapping ) {
                List<TableJoin> joins = particle.getJoinedTable();
                if ( joins != null && !joins.isEmpty() ) {
                    propTable = joins.get( joins.size() - 1 ).getToTable();
                }
                Pair<TableName, GeometryMapping> gm = getDefaultGeometryMapping( propTable, (CompoundMapping) particle );
                if ( gm != null ) {
                    return gm;
                }
            }
        }
        return null;
    }

    /**
     * Returns the column that stores the name of the feature type.
     *
     * @return column that stores the name of the feature type, can be <code>null</code> (not stored)
     */
    public SQLIdentifier getTypeColumn() {
        return typeColumn;
    }

    private Pair<TableName, GeometryMapping> getDefaultGeometryMapping( TableName table, CompoundMapping complex ) {
        for ( Mapping particle : complex.getParticles() ) {
            if ( particle instanceof GeometryMapping ) {
                List<TableJoin> joins = particle.getJoinedTable();
                if ( joins != null && !joins.isEmpty() ) {
                    table = joins.get( joins.size() - 1 ).getToTable();
                }
                return new Pair<TableName, GeometryMapping>( table, (GeometryMapping) particle );
            }
        }
        for ( Mapping particle : complex.getParticles() ) {
            TableName propTable = table;
            if ( particle instanceof CompoundMapping ) {
                List<TableJoin> joins = particle.getJoinedTable();
                if ( joins != null && !joins.isEmpty() ) {
                    propTable = joins.get( joins.size() - 1 ).getToTable();
                }
                Pair<TableName, GeometryMapping> gm = getDefaultGeometryMapping( propTable, (CompoundMapping) particle );
                if ( gm != null ) {
                    return gm;
                }
            }
        }
        return null;
    }

    /**
     * Merges the given {@link FeatureTypeMapping} into this one.
     * 
     * @param other
     *            feature type mapping, must target the same feature type
     * @param ft
     *            feature type, must not be <code>null</code>
     */
    public void merge( final FeatureTypeMapping other, final FeatureType ft ) {
        if ( !ftName.equals( other.ftName ) ) {
            final String msg = "Cannot merge feature type mappings, different feature types.";
            throw new IllegalArgumentException( msg );
        }
        if ( !equalsNullSafe( table, other.table ) ) {
            final String msg = "Cannot merge feature type mappings for " + ftName + " and " + other.ftName
                               + ", different tables: " + table + " vs. " + other.table;
            throw new IllegalArgumentException( msg );
        }
        // TODO FIDMapping
        if ( other.typeColumn != null ) {
            typeColumn = other.typeColumn;
        }
        final Map<QName, Mapping> mergedProperties = mergeProperties( other, ft );
        propToMapping.clear();
        propToMapping.putAll( mergedProperties );
        particles.clear();
        particles.addAll( propToMapping.values() );
    }

    private Map<QName, Mapping> mergeProperties( final FeatureTypeMapping other, final FeatureType ft ) {
        final Map<QName, Mapping> propToMapping = new LinkedHashMap<QName, Mapping>();
        for ( final PropertyType pt : ft.getPropertyDeclarations() ) {
            final Mapping mapping1 = this.propToMapping.get( pt.getName() );
            final Mapping mapping2 = other.propToMapping.get( pt.getName() );
            if ( mapping1 != null && mapping2 != null ) {
                final String msg = "Cannot merge feature type mappings for " + ftName + " and " + other.ftName
                                   + ", property is re-mapped: " + pt.getName();
                throw new IllegalArgumentException( msg );
            }
            if ( mapping1 != null ) {
                propToMapping.put( pt.getName(), mapping1 );
            }
            if ( mapping2 != null ) {
                propToMapping.put( pt.getName(), mapping2 );
            }
        }
        return propToMapping;
    }

    private boolean equalsNullSafe( final Object o1, final Object o2 ) {
        if ( o1 == null && o2 == null ) {
            return true;
        } else if ( o1 == null && o2 != null ) {
            return false;
        } else if ( o2 == null && o1 != null ) {
            return false;
        }
        return o1.equals( o2 );
    }

}