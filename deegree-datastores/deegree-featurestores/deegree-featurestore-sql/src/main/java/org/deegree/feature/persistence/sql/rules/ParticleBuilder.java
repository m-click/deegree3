/*----------------------------------------------------------------------------
 This file is part of deegree, http://deegree.org/
 Copyright (C) 2001-2016 by:
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
package org.deegree.feature.persistence.sql.rules;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.deegree.commons.utils.JDBCUtils.close;
import static org.deegree.commons.xml.CommonNamespaces.XSINS;
import static org.deegree.commons.xml.CommonNamespaces.XSI_PREFIX;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.xerces.xs.XSAttributeDeclaration;
import org.apache.xerces.xs.XSAttributeUse;
import org.apache.xerces.xs.XSComplexTypeDefinition;
import org.apache.xerces.xs.XSElementDeclaration;
import org.apache.xerces.xs.XSObjectList;
import org.deegree.commons.jdbc.SQLIdentifier;
import org.deegree.commons.tom.TypedObjectNode;
import org.deegree.commons.tom.genericxml.GenericXMLElement;
import org.deegree.commons.tom.gml.GMLObjectType;
import org.deegree.commons.tom.gml.property.Property;
import org.deegree.commons.tom.gml.property.PropertyType;
import org.deegree.commons.tom.primitive.PrimitiveValue;
import org.deegree.commons.tom.sql.ParticleConverter;
import org.deegree.commons.utils.Pair;
import org.deegree.commons.xml.NamespaceBindings;
import org.deegree.feature.persistence.sql.FeatureTypeMapping;
import org.deegree.feature.persistence.sql.SQLFeatureStore;
import org.deegree.feature.persistence.sql.expressions.TableJoin;
import org.deegree.feature.property.GenericProperty;
import org.deegree.feature.types.AppSchemaGeometryHierarchy;
import org.deegree.filter.expression.ValueReference;
import org.deegree.geometry.Geometry;
import org.deegree.geometry.GeometryFactory;
import org.deegree.geometry.primitive.LineString;
import org.deegree.geometry.primitive.Polygon;
import org.deegree.geometry.primitive.patches.SurfacePatch;
import org.deegree.geometry.primitive.segments.CurveSegment;
import org.deegree.sqldialect.filter.TableAliasManager;
import org.jaxen.expr.Expr;
import org.jaxen.expr.LocationPath;
import org.jaxen.expr.NameStep;
import org.jaxen.expr.NumberExpr;
import org.jaxen.expr.Predicate;
import org.jaxen.expr.Step;
import org.jaxen.saxpath.Axis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds {@link TypedObjectNode} instances from SQL result set rows (relational mode).
 * 
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 * 
 * @since 3.4
 */
class ParticleBuilder {

    private static final Logger LOG = LoggerFactory.getLogger( ParticleBuilder.class );

    private final SQLFeatureStore fs;

    private final Connection conn;

    private final TableAliasManager initialAliasManager;

    private final NamespaceBindings nsBindings;

    private final boolean nullEscalation;

    /**
     * Creates a new {@link ParticleBuilder} instance.
     * 
     * @param fs
     *            feature store, must not be <code>null</code>
     * @param ftMapping
     *            feature type mapping, must not be <code>null</code>
     * @param conn
     *            JDBC connection (used for performing subsequent SELECTs), must not be <code>null</code>
     * @param aliasManager
     * @param escalationPolicy
     *            the void escalation policy, must not be <code>null</code>
     */
    public ParticleBuilder( SQLFeatureStore fs, FeatureTypeMapping ftMapping, Connection conn,
                            TableAliasManager aliasManager, boolean nullEscalation ) {
        this.fs = fs;
        this.conn = conn;
        this.initialAliasManager = aliasManager;
        this.nullEscalation = nullEscalation;
        this.nsBindings = new NamespaceBindings();
        for ( String prefix : fs.getNamespaceContext().keySet() ) {
            String ns = fs.getNamespaceContext().get( prefix );
            nsBindings.addNamespace( prefix, ns );
        }
    }

    List<TypedObjectNode> build( Mapping mapping, ResultSet rs, LinkedHashMap<String, Integer> colToRsIdx,
                                 String idPrefix )
                            throws SQLException {
        if ( mapping.isSkipOnReconstruct() ) {
            return Collections.emptyList();
        }
        if ( !( mapping instanceof FeatureMapping ) && mapping.getJoinedTable() != null ) {
            return buildFromJoinedTable( mapping, rs, colToRsIdx, idPrefix );
        }
        final TypedObjectNode particle = buildParticle( mapping, rs, colToRsIdx, idPrefix );
        if ( particle != null ) {
            return singletonList( particle );
        }
        return emptyList();
    }

    private List<TypedObjectNode> buildFromJoinedTable( Mapping mapping, ResultSet rs,
                                                        LinkedHashMap<String, Integer> colToRsIdx, String idPrefix )
                            throws SQLException {
        final List<TypedObjectNode> values = new ArrayList<TypedObjectNode>();
        ResultSet rs2 = null;
        try {
            final TableJoin join = mapping.getJoinedTable().get( 0 );
            Pair<ResultSet, LinkedHashMap<String, Integer>> p = getJoinedResultSet( join, mapping, rs, colToRsIdx );
            rs2 = p.first;
            int i = 0;
            while ( rs2.next() ) {
                final TypedObjectNode particle = buildParticle( mapping, rs2, p.second, idPrefix + "_" + ( i++ ) );
                if ( particle != null ) {
                    values.add( particle );
                }
            }
        } finally {
            if ( rs2 != null ) {
                rs2.getStatement().close();
                rs2.close();
            }
        }
        return values;
    }

    private TypedObjectNode buildParticle( Mapping mapping, ResultSet rs, LinkedHashMap<String, Integer> colToRsIdx,
                                           String idPrefix )
                            throws SQLException {

        LOG.debug( "Trying to build particle with path {}.", mapping.getPath() );

        TypedObjectNode particle = null;
        ParticleConverter<?> converter = fs.getConverter( mapping );
        final String tableAlias = getRootTableAlias();

        if ( converter != null ) {
            // particle fully defined by available result set columns
            final String col = converter.getSelectSnippet( tableAlias );
            final int colIndex = colToRsIdx.get( col );
            particle = converter.toParticle( rs, colIndex );
            if ( particle instanceof Geometry ) {
                ( (Geometry) particle ).setId( idPrefix );
            }
        } else if ( mapping instanceof CompoundMapping ) {
            particle = buildCompoundParticle( (CompoundMapping) mapping, rs, colToRsIdx, idPrefix );
        }

        if ( particle == null ) {
            LOG.debug( "Building of particle with path {} resulted in NULL.", mapping.getPath() );
        } else {
            LOG.debug( "Built particle with path {}.", mapping.getPath() );
        }
        return particle;
    }

    private TypedObjectNode buildCompoundParticle( final CompoundMapping cm, final ResultSet rs,
                                                   final LinkedHashMap<String, Integer> colToRsIdx,
                                                   final String idPrefix )
                            throws SQLException {

        final Map<QName, PrimitiveValue> attrs = new HashMap<QName, PrimitiveValue>();
        final List<TypedObjectNode> children = new ArrayList<TypedObjectNode>();
        boolean escalateVoid = false;

        // collect attributes and children, determine if particle is void (missing or incomplete)
        for ( final Mapping childMapping : cm.getParticles() ) {
            final List<TypedObjectNode> childValues = build( childMapping, rs, colToRsIdx, idPrefix );
            if ( nullEscalation && !escalateVoid ) {
                escalateVoid = isChildRequiredButMissing( childMapping, childValues );
            }
            addChildParticles( attrs, children, childMapping, childValues );
        }

        return buildCompoundParticle( cm, attrs, children, escalateVoid );
    }

    private boolean isChildRequiredButMissing( final Mapping childMapping, final List<TypedObjectNode> childValues ) {
        if ( childMapping.isVoidable() ) {
            return false;
        }
        for ( final TypedObjectNode childValue : childValues ) {
            if ( childValue != null ) {
                return false;
            }
        }
        return true;
    }

    private void addChildParticles( final Map<QName, PrimitiveValue> attrs, final List<TypedObjectNode> children,
                                    final Mapping particleMapping, final List<TypedObjectNode> particleValues ) {
        final Expr xpath = particleMapping.getPath().getAsXPath();
        assertLocationPath( xpath );
        final LocationPath lp = (LocationPath) xpath;
        assertRelativeWithSingleStep( lp );
        final Step step = (Step) lp.getSteps().get( 0 );
        assertUnpredicatedOrNumbered( step );
        if ( step instanceof NameStep ) {
            final NameStep ns = (NameStep) step;
            final QName name = getQName( ns );
            if ( step.getAxis() == Axis.ATTRIBUTE ) {
                addAttributes( attrs, particleValues, name );
            } else if ( step.getAxis() == Axis.CHILD ) {
                addElements( children, particleValues, name );
            } else {
                final String msg = "Only attribute or child paths are allowed.";
                throw new IllegalArgumentException( msg );
            }
        } else {
            children.addAll( particleValues );
        }
    }

    private TypedObjectNode buildCompoundParticle( final CompoundMapping cm, final Map<QName, PrimitiveValue> attrs,
                                                   final List<TypedObjectNode> children, boolean escalateVoid ) {
        final QName elName = getName( cm.getPath() );
        GenericXMLElement particle = null;
        // if xsi:nil attribute is present, return empty element with xsi:nil (but keep attributes)
        if ( isNilled( attrs ) ) {
            particle = new GenericXMLElement( elName, cm.getElementDecl(), attrs, null );
        } else if ( escalateVoid ) {
            if ( cm.isVoidable() ) {
                LOG.debug( "Materializing void by omitting particle for path {}.", cm.getPath() );
                return null;
            }
            if ( cm.getElementDecl() != null && cm.getElementDecl().getNillable() ) {
                LOG.debug( "Materializing void by nilling particle for path {}.", cm.getPath() );
                // required attributes must still be present even if element is nilled...
                final Map<QName, PrimitiveValue> nilAttrs = new HashMap<QName, PrimitiveValue>();
                if ( cm.getElementDecl().getTypeDefinition() instanceof XSComplexTypeDefinition ) {
                    XSComplexTypeDefinition complexType = (XSComplexTypeDefinition) cm.getElementDecl().getTypeDefinition();
                    XSObjectList attrUses = complexType.getAttributeUses();
                    for ( int i = 0; i < attrUses.getLength(); i++ ) {
                        XSAttributeUse attrUse = (XSAttributeUse) attrUses.item( i );
                        if ( attrUse.getRequired() ) {
                            QName attrName = null;
                            XSAttributeDeclaration attrDecl = attrUse.getAttrDeclaration();
                            if ( attrDecl.getNamespace() == null || attrDecl.getNamespace().isEmpty() ) {
                                attrName = new QName( attrDecl.getName() );
                            } else {
                                attrName = new QName( attrDecl.getNamespace(), attrDecl.getName() );
                            }
                            PrimitiveValue attrValue = attrs.get( attrName );
                            if ( attrValue == null ) {
                                LOG.debug( "Required attribute " + attrName
                                           + "not present. Cannot void using xsi:nil. Escalating void value." );
                                return null;
                            }
                            nilAttrs.put( attrName, attrValue );
                        }
                    }
                }
                nilAttrs.put( new QName( XSINS, "nil", XSI_PREFIX ), new PrimitiveValue( TRUE ) );
                particle = new GenericXMLElement( elName, cm.getElementDecl(), nilAttrs, null );
            }
        } else {
            if ( ( !attrs.isEmpty() ) || !children.isEmpty() ) {
                particle = new GenericXMLElement( elName, cm.getElementDecl(), attrs, children );
            }
        }

        // special case: geometry element mapped via compound mapping and child geometry mapping uses self axis path (.)
        if ( fs.getSchema().getGeometryType( elName ) != null ) {
            return unwrapCustomGeometry( particle );
        }
        return particle;
    }

    private boolean isNilled( final Map<QName, PrimitiveValue> attrs ) {
        final PrimitiveValue nilled = attrs.get( new QName( XSINS, "nil" ) );
        return nilled != null && nilled.getValue().equals( TRUE );
    }

    private void assertLocationPath( final Expr xpath ) {
        if ( !( xpath instanceof LocationPath ) ) {
            final String msg = "XPath expression '" + xpath + "' invalid: Only location paths are allowed.";
            throw new IllegalArgumentException( msg );
        }
    }

    private void assertRelativeWithSingleStep( final LocationPath lp ) {
        if ( lp.getSteps().size() != 1 ) {
            final String msg = "Location '" + lp + "' invalid: Only single step paths are allowed.";
            throw new IllegalArgumentException( msg );
        }
        if ( lp.isAbsolute() ) {
            final String msg = "Location '" + lp + "' invalid: Only relative paths are allowed.";
            throw new IllegalArgumentException( msg );
        }
    }

    private void assertUnpredicatedOrNumbered( final Step step ) {
        if ( !step.getPredicates().isEmpty() ) {
            final List<?> predicates = step.getPredicates();
            if ( predicates.size() == 1 ) {
                final Expr predicate = ( (Predicate) predicates.get( 0 ) ).getExpr();
                if ( predicate instanceof NumberExpr ) {
                    LOG.debug( "Number predicate. Assuming natural ordering." );
                } else {
                    final String msg = "Step '" + step + "' invalid: Only number predicates are allowed.";
                    throw new IllegalArgumentException( msg );
                }
            } else {
                final String msg = "Step '" + step + "' invalid: Only a single number predicate is allowed.";
                throw new IllegalArgumentException( msg );
            }
        }
    }

    private void addAttributes( Map<QName, PrimitiveValue> attrs, List<TypedObjectNode> particleValues, final QName name ) {
        for ( final TypedObjectNode particleValue : particleValues ) {
            if ( particleValue instanceof PrimitiveValue ) {
                attrs.put( name, (PrimitiveValue) particleValue );
            } else {
                LOG.warn( "Value not suitable for attribute." );
            }
        }
    }

    private void addElements( List<TypedObjectNode> children, List<TypedObjectNode> particleValues, final QName name ) {
        for ( TypedObjectNode particleValue : particleValues ) {
            if ( particleValue instanceof PrimitiveValue ) {
                // TODO
                XSElementDeclaration childType = null;
                GenericXMLElement child = new GenericXMLElement( name, childType,
                                                                 Collections.<QName, PrimitiveValue> emptyMap(),
                                                                 singletonList( particleValue ) );
                children.add( child );
            } else if ( particleValue != null ) {
                children.add( particleValue );
            }
        }
    }

    private void addColumn( LinkedHashMap<String, Integer> colToRsIdx, String column ) {
        if ( !colToRsIdx.containsKey( column ) ) {
            colToRsIdx.put( column, colToRsIdx.size() + 1 );
        }
    }

    private LinkedHashMap<String, Integer> getSubsequentSelectColumns( Mapping mapping ) {
        LinkedHashMap<String, Integer> colToRsIdx = new LinkedHashMap<String, Integer>();
        addSelectColumns( mapping, colToRsIdx, false );
        return colToRsIdx;
    }

    private void addSelectColumns( Mapping mapping, LinkedHashMap<String, Integer> colToRsIdx, boolean initial ) {
        if ( mapping.isSkipOnReconstruct() ) {
            return;
        }
        List<TableJoin> jc = mapping.getJoinedTable();
        if ( jc != null && initial ) {
            if ( mapping instanceof FeatureMapping ) {
                ParticleConverter<?> particleConverter = fs.getConverter( mapping );
                if ( particleConverter != null ) {
                    addColumn( colToRsIdx, particleConverter.getSelectSnippet( getRootTableAlias() ) );
                } else {
                    LOG.info( "Omitting mapping '" + mapping + "' from SELECT list. Not mapped to column.'" );
                }
            } else {
                for ( SQLIdentifier column : jc.get( 0 ).getFromColumns() ) {
                    addColumn( colToRsIdx, qualifyRootColumn( column.getName() ) );
                }
            }
        } else {
            ParticleConverter<?> particleConverter = fs.getConverter( mapping );
            if ( mapping instanceof PrimitiveMapping ) {
                if ( particleConverter != null ) {
                    addColumn( colToRsIdx, particleConverter.getSelectSnippet( getRootTableAlias() ) );
                } else {
                    LOG.info( "Omitting mapping '" + mapping + "' from SELECT list. Not mapped to column.'" );
                }
            } else if ( mapping instanceof GeometryMapping ) {
                if ( particleConverter != null ) {
                    addColumn( colToRsIdx, particleConverter.getSelectSnippet( getRootTableAlias() ) );
                } else {
                    LOG.info( "Omitting mapping '" + mapping + "' from SELECT list. Not mapped to column.'" );
                }
            } else if ( mapping instanceof FeatureMapping ) {
                if ( particleConverter != null ) {
                    addColumn( colToRsIdx, particleConverter.getSelectSnippet( getRootTableAlias() ) );
                } else {
                    LOG.info( "Omitting mapping '" + mapping + "' from SELECT list. Not mapped to column.'" );
                }
            } else if ( mapping instanceof BlobParticleMapping ) {
                if ( particleConverter != null ) {
                    addColumn( colToRsIdx, particleConverter.getSelectSnippet( getRootTableAlias() ) );
                } else {
                    LOG.info( "Omitting mapping '" + mapping + "' from SELECT list. Not mapped to column.'" );
                }
            } else if ( mapping instanceof CompoundMapping ) {
                CompoundMapping cm = (CompoundMapping) mapping;
                for ( Mapping particle : cm.getParticles() ) {
                    addSelectColumns( particle, colToRsIdx, true );
                }
            } else if ( mapping instanceof SqlExpressionMapping<?> ) {
                // nothing to do
            } else {
                LOG.warn( "Mappings of type '" + mapping.getClass() + "' are not handled yet." );
            }
        }
    }

    // TODO where should this happen in the end?
    private TypedObjectNode unwrapCustomGeometry( GenericXMLElement particle ) {

        GMLObjectType ot = fs.getSchema().getGeometryType( particle.getName() );
        Geometry geom = null;
        List<Property> props = new ArrayList<Property>();
        for ( TypedObjectNode child : particle.getChildren() ) {
            if ( child instanceof Geometry ) {
                geom = (Geometry) child;
            } else if ( child instanceof GenericXMLElement ) {
                GenericXMLElement xmlEl = (GenericXMLElement) child;
                PropertyType pt = ot.getPropertyDeclaration( xmlEl.getName() );
                props.add( new GenericProperty( pt, xmlEl.getName(), null, xmlEl.getAttributes(), xmlEl.getChildren() ) );
            } else {
                LOG.warn( "Unhandled particle: " + child );
            }
        }
        if ( geom == null ) {
            return null;
        }
        AppSchemaGeometryHierarchy hierarchy = fs.getSchema().getGeometryHierarchy();

        if ( hierarchy != null ) {
            if ( hierarchy.getSurfaceSubstitutions().contains( particle.getName() ) && geom instanceof Polygon ) {
                // constructed as Polygon, but needs to become a Surface
                Polygon p = (Polygon) geom;
                GeometryFactory geomFac = new GeometryFactory();
                List<SurfacePatch> patches = new ArrayList<SurfacePatch>();
                patches.add( geomFac.createPolygonPatch( p.getExteriorRing(), p.getInteriorRings() ) );
                geom = geomFac.createSurface( geom.getId(), patches, geom.getCoordinateSystem() );
            } else if ( hierarchy.getCurveSubstitutions().contains( particle.getName() ) && geom instanceof LineString ) {
                // constructed as LineString, but needs to become a Curve
                LineString p = (LineString) geom;
                GeometryFactory geomFac = new GeometryFactory();
                CurveSegment[] segments = new CurveSegment[1];
                segments[0] = geomFac.createLineStringSegment( p.getControlPoints() );
                geom = geomFac.createCurve( geom.getId(), geom.getCoordinateSystem(), segments );
            }
            geom.setType( fs.getSchema().getGeometryType( particle.getName() ) );
            geom.setProperties( props );
        }
        return geom;
    }

    private QName getName( ValueReference path ) {
        if ( path.getAsQName() != null ) {
            return path.getAsQName();
        }
        Expr xpath = path.getAsXPath();
        if ( xpath instanceof LocationPath ) {
            LocationPath lp = (LocationPath) xpath;
            if ( lp.getSteps().size() == 1 && !lp.isAbsolute() ) {
                Step step = (Step) lp.getSteps().get( 0 );
                if ( step instanceof NameStep ) {
                    return getQName( (NameStep) step );
                }
            }
        }
        return null;
    }

    private Pair<ResultSet, LinkedHashMap<String, Integer>> getJoinedResultSet( TableJoin jc,
                                                                                Mapping mapping,
                                                                                ResultSet rs,
                                                                                LinkedHashMap<String, Integer> colToRsIdx )
                            throws SQLException {

        LinkedHashMap<String, Integer> rsToIdx = getSubsequentSelectColumns( mapping );
        final String tableAlias = "X1";
        StringBuilder sql = new StringBuilder( "SELECT " );
        boolean first = true;
        for ( String column : rsToIdx.keySet() ) {
            if ( !first ) {
                sql.append( ',' );
            }
            sql.append( column );
            first = false;
        }
        sql.append( " FROM " );
        sql.append( jc.getToTable() );
        sql.append( ' ' );
        sql.append( tableAlias );
        sql.append( " WHERE " );
        first = true;
        for ( SQLIdentifier keyColumn : jc.getToColumns() ) {
            if ( !first ) {
                sql.append( " AND " );
            }
            sql.append( keyColumn );
            sql.append( " = ?" );
            first = false;
        }
        if ( jc.getOrderColumns() != null && !jc.getOrderColumns().isEmpty() ) {
            sql.append( " ORDER BY " );
            first = true;
            for ( SQLIdentifier orderColumn : jc.getOrderColumns() ) {
                if ( !first ) {
                    sql.append( "," );
                }
                if ( orderColumn.toString().endsWith( "-" ) ) {
                    sql.append( orderColumn.toString().substring( 0, orderColumn.toString().length() - 1 ) );
                    sql.append( " DESC" );
                } else {
                    sql.append( orderColumn );
                }
                first = false;
            }
        }
        LOG.debug( "SQL: {}", sql );

        PreparedStatement stmt = null;
        ResultSet rs2 = null;
        try {
            long begin = System.currentTimeMillis();
            stmt = conn.prepareStatement( sql.toString() );

            LOG.debug( "Preparing subsequent SELECT took {} [ms] ", System.currentTimeMillis() - begin );
            int i = 1;
            for ( SQLIdentifier keyColumn : jc.getFromColumns() ) {
                Object key = rs.getObject( colToRsIdx.get( tableAlias + "." + keyColumn ) );
                LOG.debug( "? = '{}' ({})", key, keyColumn );
                stmt.setObject( i++, key );
            }
            begin = System.currentTimeMillis();
            rs2 = stmt.executeQuery();
            LOG.debug( "Executing SELECT took {} [ms] ", System.currentTimeMillis() - begin );
        } catch ( Throwable t ) {
            close( rs2, stmt, null, LOG );
            String msg = "Error performing subsequent SELECT: " + t.getMessage();
            LOG.error( msg, t );
            throw new SQLException( msg, t );
        }
        return new Pair<ResultSet, LinkedHashMap<String, Integer>>( rs2, rsToIdx );
    }

    private QName getQName( NameStep step ) {
        String prefix = step.getPrefix();
        QName qName;
        if ( prefix.isEmpty() ) {
            qName = new QName( step.getLocalName() );
        } else {
            String ns = nsBindings.translateNamespacePrefixToUri( prefix );
            qName = new QName( ns, step.getLocalName(), prefix );
        }
        return qName;
    }

    private String qualifyRootColumn( final String column ) {
        return initialAliasManager.getRootTableAlias() + '.' + column;
    }

    private String getRootTableAlias() {
        return initialAliasManager.getRootTableAlias();
    }

}
