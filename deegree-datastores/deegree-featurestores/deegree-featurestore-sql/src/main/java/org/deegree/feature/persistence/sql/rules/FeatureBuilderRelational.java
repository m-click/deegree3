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
package org.deegree.feature.persistence.sql.rules;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.singletonMap;
import static org.deegree.commons.xml.CommonNamespaces.XSINS;
import static org.jaxen.saxpath.Axis.CHILD;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.deegree.commons.jdbc.SQLIdentifier;
import org.deegree.commons.tom.TypedObjectNode;
import org.deegree.commons.tom.genericxml.GenericXMLElement;
import org.deegree.commons.tom.gml.GMLObject;
import org.deegree.commons.tom.gml.property.Property;
import org.deegree.commons.tom.gml.property.PropertyType;
import org.deegree.commons.tom.primitive.BaseType;
import org.deegree.commons.tom.primitive.PrimitiveValue;
import org.deegree.commons.utils.Pair;
import org.deegree.commons.xml.NamespaceBindings;
import org.deegree.commons.xml.stax.XMLStreamReaderWrapper;
import org.deegree.feature.Feature;
import org.deegree.feature.persistence.sql.FeatureBuilder;
import org.deegree.feature.persistence.sql.FeatureTypeMapping;
import org.deegree.feature.persistence.sql.SQLFeatureStore;
import org.deegree.feature.property.GenericProperty;
import org.deegree.feature.types.FeatureType;
import org.deegree.feature.types.property.ObjectPropertyType;
import org.deegree.filter.expression.ValueReference;
import org.deegree.gml.GMLInputFactory;
import org.deegree.gml.GMLOutputFactory;
import org.deegree.gml.GMLStreamReader;
import org.deegree.gml.GMLStreamWriter;
import org.deegree.gml.GMLVersion;
import org.deegree.gml.reference.GmlXlinkOptions;
import org.deegree.gml.schema.GMLSchemaInfoSet;
import org.deegree.sqldialect.filter.TableAliasManager;
import org.jaxen.expr.Expr;
import org.jaxen.expr.LocationPath;
import org.jaxen.expr.NameStep;
import org.jaxen.expr.NumberExpr;
import org.jaxen.expr.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds {@link Feature} instances from SQL result set rows (relational mode).
 * 
 * @author <a href="mailto:schneider@lat-lon.de">Markus Schneider</a>
 * @author last edited by: $Author$
 * 
 * @version $Revision$, $Date$
 */
public class FeatureBuilderRelational implements FeatureBuilder {

    private static final Logger LOG = LoggerFactory.getLogger( FeatureBuilderRelational.class );

    private final SQLFeatureStore fs;

    private final FeatureTypeMapping ftMapping;

    private final Connection conn;

    private final TableAliasManager aliasManager;

    private final NamespaceBindings nsBindings;

    private final SelectManager selectManager;

    private final boolean nullEscalation;

    /**
     * Creates a new {@link FeatureBuilderRelational} instance.
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
    public FeatureBuilderRelational( SQLFeatureStore fs, FeatureTypeMapping ftMapping, Connection conn,
                                     TableAliasManager aliasManager, boolean nullEscalation ) {
        this.fs = fs;
        this.ftMapping = ftMapping;
        this.conn = conn;
        this.aliasManager = aliasManager;
        this.nullEscalation = nullEscalation;
        this.nsBindings = new NamespaceBindings();
        for ( String prefix : fs.getNamespaceContext().keySet() ) {
            String ns = fs.getNamespaceContext().get( prefix );
            nsBindings.addNamespace( prefix, ns );
        }
        this.selectManager = new SelectManager( ftMapping, aliasManager, fs );
    }

    @Override
    public List<String> getInitialSelectList() {
        LOG.debug( "Initial select columns: " + selectManager.selectTerms );
        return new ArrayList<String>( selectManager.selectTerms );
    }

    @Override
    public Feature buildFeature( final ResultSet rs, final FeatureType queryFt )
                            throws SQLException {

        Feature feature = null;
        try {
            final String gmlId = buildGmlId( rs );
            if ( fs.getCache() != null ) {
                feature = (Feature) fs.getCache().get( gmlId );
            }
            if ( feature == null ) {
                LOG.debug( "Recreating feature '" + gmlId + "' from db (relational mode)." );
                final FeatureType ft = disambiguateFeatureType( rs, queryFt );
                final List<Property> props = buildProperties( rs, gmlId, ft );
                feature = ft.newFeature( gmlId, props, null );
                if ( fs.getCache() != null ) {
                    fs.getCache().add( feature );
                }
            } else {
                LOG.debug( "Cache hit." );
            }
        } catch ( Throwable t ) {
            LOG.error( t.getMessage(), t );
            throw new SQLException( t.getMessage(), t );
        }
        return feature;
    }

    private String buildGmlId( final ResultSet rs )
                            throws SQLException {
        String gmlId = ftMapping.getFidMapping().getPrefix();
        List<Pair<SQLIdentifier, BaseType>> fidColumns = ftMapping.getFidMapping().getColumns();
        int resultSetIdx = selectManager.getResultSetIndex( ftMapping.getFidMapping() );
        gmlId += rs.getObject( resultSetIdx++ );
        for ( int i = 1; i < fidColumns.size(); i++ ) {
            gmlId += ftMapping.getFidMapping().getDelimiter() + rs.getObject( resultSetIdx++ );
        }
        return gmlId;
    }

    private FeatureType disambiguateFeatureType( final ResultSet rs, final FeatureType ft )
                            throws SQLException {
        if ( ftMapping.getTypeColumn() != null ) {
            final String ftName = rs.getString( selectManager.getResultSetIndex( qualifyRootColumn( ftMapping.getTypeColumn().getName() ) ) );
            LOG.debug( "Ambigous feature type mapping: Using feature type from type column: " + ftName );
            return fs.getSchema().getFeatureType( QName.valueOf( ftName ) );
        }
        return ft;
    }

    private List<Property> buildProperties( final ResultSet rs, final String gmlId, final FeatureType ft )
                            throws SQLException {
        List<Property> props = new ArrayList<Property>();
        for ( Mapping mapping : ftMapping.getMappings() ) {
            if ( mapping.isSkipOnReconstruct() ) {
                continue;
            }
            ValueReference propName = mapping.getPath();
            QName childEl = getChildElementStepAsQName( propName );
            if ( childEl != null ) {
                PropertyType pt = ft.getPropertyDeclaration( childEl );
                String idPrefix = gmlId + "_" + toIdPrefix( propName );
                addProperties( props, pt, mapping, rs, idPrefix );
            } else {
                LOG.warn( "Omitting mapping '" + mapping
                          + "'. Only single child element steps (optionally with number predicate)"
                          + " are currently supported." );
            }
        }
        return props;
    }

    private String toIdPrefix( ValueReference propName ) {
        String s = propName.getAsText();
        s = s.replace( "/", "_" );
        s = s.replace( ":", "_" );
        s = s.replace( "[", "_" );
        s = s.replace( "]", "_" );
        s = s.toUpperCase();
        return s;
    }

    private void addProperties( List<Property> props, PropertyType pt, Mapping propMapping, ResultSet rs,
                                String idPrefix )
                            throws SQLException {
        if ( propMapping.isSkipOnReconstruct() ) {
            return;
        }
        final ParticleBuilder particleBuilder = new ParticleBuilder( fs, ftMapping, conn, nullEscalation );
        List<TypedObjectNode> particles = particleBuilder.build( propMapping, rs, selectManager, idPrefix );
        if ( particles.isEmpty() && pt.getMinOccurs() > 0 ) {
            if ( pt.isNillable() ) {
                final Map<QName, PrimitiveValue> attrs = singletonMap( new QName( XSINS, "nil" ),
                                                                       new PrimitiveValue( TRUE ) );
                props.add( new GenericProperty( pt, propMapping.getPath().getAsQName(), null, attrs,
                                                Collections.<TypedObjectNode> emptyList() ) );
            } else {
                LOG.warn( "Unable to map NULL value for mapping '" + propMapping.getPath().getAsText()
                          + "' to output. This will result in schema violations." );
            }
        }
        for ( final TypedObjectNode particle : particles ) {
            if ( particle instanceof Property ) {
                props.add( (Property) particle );
            } else if ( particle instanceof GenericXMLElement ) {
                props.add( convertToProperty( (GenericXMLElement) particle, pt ) );
            } else {
                props.add( new GenericProperty( pt, pt.getName(), particle ) );
            }
        }
    }

    private Property convertToProperty( final GenericXMLElement particle, final PropertyType pt ) {
        if ( pt instanceof ObjectPropertyType && !isChildElementGmlObject( particle ) ) {
            LOG.warn( "Recreating GML object-valued property: "
                      + ( (ObjectPropertyType) pt ).getAllowedRepresentation() );
            return recreatePropertyFromGml( pt, particle );
        }
        return new GenericProperty( pt, particle.getName(), null, particle.getAttributes(), particle.getChildren() );
    }

    private boolean isChildElementGmlObject( final GenericXMLElement particle ) {
        if ( particle.getChildren().size() != 1 ) {
            return false;
        }
        final TypedObjectNode child = particle.getChildren().get( 0 );
        if ( child instanceof GMLObject ) {
            return true;
        }
        return false;
    }

    private Property recreatePropertyFromGml( final PropertyType pt, final GenericXMLElement particle ) {
        try {
            final GMLSchemaInfoSet gmlSchema = fs.getSchema().getGMLSchema();
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter( bos );
            final GMLVersion version = fs.getSchema().getGMLSchema().getVersion();
            final GMLStreamWriter gmlWriter = GMLOutputFactory.createGMLStreamWriter( version, xmlWriter );
            gmlWriter.setNamespaceBindings( gmlSchema.getNamespacePrefixes() );
            final GmlXlinkOptions resolveState = new GmlXlinkOptions();
            gmlWriter.getFeatureWriter().export( particle, resolveState );
            gmlWriter.close();
            xmlWriter.close();
            bos.close();
            final InputStream is = new ByteArrayInputStream( bos.toByteArray() );
            final XMLStreamReader xmlReader = XMLInputFactory.newInstance().createXMLStreamReader( is );
            final GMLStreamReader gmlReader = GMLInputFactory.createGMLStreamReader( version, xmlReader );
            gmlReader.setApplicationSchema( fs.getSchema() );
            gmlReader.setLaxMode( true );
            final Property property = gmlReader.getFeatureReader().parseProperty( new XMLStreamReaderWrapper(
                                                                                                              xmlReader,
                                                                                                              null ),
                                                                                  pt, null );
            return property;
        } catch ( final Exception e ) {
            LOG.error( e.getMessage(), e );
        }
        return new GenericProperty( pt, particle.getName(), null, particle.getAttributes(), particle.getChildren() );
    }

    private QName getChildElementStepAsQName( ValueReference ref ) {
        QName qName = null;
        Expr xpath = ref.getAsXPath();
        if ( xpath instanceof LocationPath ) {
            LocationPath lpath = (LocationPath) xpath;
            if ( lpath.getSteps().size() == 1 ) {
                if ( lpath.getSteps().get( 0 ) instanceof NameStep ) {
                    NameStep step = (NameStep) lpath.getSteps().get( 0 );
                    if ( isChildElementStepWithoutPredicateOrWithNumberPredicate( step ) ) {
                        String prefix = step.getPrefix();
                        if ( prefix.isEmpty() ) {
                            qName = new QName( step.getLocalName() );
                        } else {
                            String ns = ref.getNsContext().translateNamespacePrefixToUri( prefix );
                            qName = new QName( ns, step.getLocalName(), prefix );
                        }
                        LOG.debug( "QName: " + qName );
                    }
                }
            }
        }
        return qName;
    }

    private boolean isChildElementStepWithoutPredicateOrWithNumberPredicate( NameStep step ) {
        if ( step.getAxis() == CHILD && !step.getLocalName().equals( "*" ) ) {
            if ( step.getPredicates().isEmpty() ) {
                return true;
            } else if ( step.getPredicates().size() == 1 ) {
                Predicate predicate = (Predicate) step.getPredicates().get( 0 );
                Expr expr = predicate.getExpr();
                if ( expr instanceof NumberExpr ) {
                    return true;
                }
            }
        }
        return false;
    }

    private String qualifyRootColumn( final String column ) {
        return aliasManager.getRootTableAlias() + '.' + column;
    }

}
