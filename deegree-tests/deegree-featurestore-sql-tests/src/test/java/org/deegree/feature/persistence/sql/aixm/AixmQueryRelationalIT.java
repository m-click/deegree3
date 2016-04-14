package org.deegree.feature.persistence.sql.aixm;

import static org.deegree.protocol.wfs.transaction.action.IDGenMode.USE_EXISTING;

import javax.xml.namespace.QName;

import org.deegree.feature.Feature;
import org.deegree.feature.FeatureCollection;
import org.deegree.feature.persistence.query.Query;
import org.deegree.feature.persistence.sql.SQLFeatureStore;
import org.deegree.feature.persistence.sql.SQLFeatureStoreTestCase;
import org.deegree.filter.Filter;
import org.deegree.filter.Operator;
import org.deegree.filter.OperatorFilter;
import org.deegree.filter.spatial.BBOX;
import org.deegree.geometry.Envelope;
import org.deegree.geometry.GeometryFactory;

/**
 * Tests the query behaviour of the {@link SQLFeatureStore} for an AIXM configuration (relational mode).
 *
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 *
 * @since 3.4
 */
public class AixmQueryRelationalIT extends SQLFeatureStoreTestCase {

    private static final QName AIRSPACE_NAME = new QName( AIXM_NS, "Airspace" );

    private static final QName VERTICAL_STRUCTURE_NAME = new QName( AIXM_NS, "VerticalStructure" );

    private SQLFeatureStore fs;

    @Override
    public void setUp()
                            throws Exception {
        fs = setUpFeatureStore( "aixm-relational", "aixm/workspace" );
        importGml( fs, "aixm/data/Donlon.xml", USE_EXISTING );
    }

    public void testQueryVerticalStructure()
                            throws Exception {
        final Query query = new Query( VERTICAL_STRUCTURE_NAME, null, -1, -1, -1 );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 20, fc.size() );
    }

    public void testQueryVerticalStructureCrane5()
                            throws Exception {
        final Query query = buildGmlIdentifierQuery( "8c755520-b42b-11e3-a5e2-0800500c9a66", VERTICAL_STRUCTURE_NAME );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 1, fc.size() );
    }

    public void testQueryByGmlIdentifier()
                            throws Exception {
        final Query query = buildGmlIdentifierQuery( "010d8451-d751-4abb-9c71-f48ad024045b", AIRSPACE_NAME );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 1, fc.size() );
    }

    public void testQueryFilterOnHybridProperty()
                            throws Exception {
        final Query query = buildFilterQuery( "aixm/filter/vertical_structure_timeslice_beginposition.xml",
                                              VERTICAL_STRUCTURE_NAME );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 8, fc.size() );
    }

    public void testQueryFilterOnHybridPropertyNoMatch()
                            throws Exception {
        final Query query = buildFilterQuery( "aixm/filter/vertical_structure_timeslice_beginposition_no_match.xml",
                                              VERTICAL_STRUCTURE_NAME );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 0, fc.size() );
    }

    public void testQueryByBbox()
                            throws Exception {
        final Envelope env = new GeometryFactory().createEnvelope( 52.453333, -5.981667, 53.893333, -5.755, null );
        final Operator op = new BBOX( env );
        final Filter filter = new OperatorFilter( op );
        final Query query = new Query( AIRSPACE_NAME, filter, -1, -1, -1.0 );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 1, fc.size() );
    }

}
