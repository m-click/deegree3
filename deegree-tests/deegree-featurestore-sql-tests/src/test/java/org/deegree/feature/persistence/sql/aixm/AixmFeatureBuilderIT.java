package org.deegree.feature.persistence.sql.aixm;

import static org.deegree.protocol.wfs.transaction.action.IDGenMode.GENERATE_NEW;
import static org.deegree.protocol.wfs.transaction.action.IDGenMode.USE_EXISTING;

import javax.xml.namespace.QName;

import org.deegree.feature.FeatureCollection;
import org.deegree.feature.persistence.query.Query;
import org.deegree.feature.persistence.sql.SQLFeatureStore;
import org.deegree.feature.persistence.sql.SQLFeatureStoreTestCase;
import org.deegree.filter.Filter;
import org.deegree.filter.IdFilter;
import org.deegree.protocol.wfs.transaction.action.IDGenMode;

/**
 * Tests the reconstruction of features for AIXM configurations (relational + hybrid)..
 *
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 *
 * @since 3.4
 */
public class AixmFeatureBuilderIT extends SQLFeatureStoreTestCase {

    private static final QName VERTICAL_STRUCTURE_NAME = new QName( AIXM_NS, "VerticalStructure" );

    private SQLFeatureStore fs;

    public void testBuildFeatureHybrid()
                            throws Exception {
        fs = setUpFeatureStore( "aixm-hybrid", "aixm/workspace" );
        importGml( fs, "aixm/data/Donlon.xml", USE_EXISTING );
        final Query query = buildGmlIdentifierQuery( "8c755520-b42b-11e3-a5e2-0800500c9a66", VERTICAL_STRUCTURE_NAME );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 1, fc.size() );
        assertGmlEquals( fc.iterator().next(), "aixm/expected/crane_5.xml" );
    }

    public void testBuildFeatureRelational()
                            throws Exception {
        fs = setUpFeatureStore( "aixm-relational", "aixm/workspace" );
        importGml( fs, "aixm/data/Donlon.xml", USE_EXISTING );
        final Query query = buildGmlIdentifierQuery( "8c755520-b42b-11e3-a5e2-0800500c9a66", VERTICAL_STRUCTURE_NAME );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 1, fc.size() );
        assertGmlEquals( fc.iterator().next(), "aixm/expected/crane_5_relational.xml" );
    }

    public void testBuildFeatureBlob()
                            throws Exception {
        fs = setUpFeatureStore( "aixm-blob", "aixm/workspace" );
        importGml( fs, "aixm/data/Donlon.xml", USE_EXISTING );
        final Query query = buildGmlIdentifierQuery( "8c755520-b42b-11e3-a5e2-0800500c9a66", VERTICAL_STRUCTURE_NAME );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 1, fc.size() );
        assertGmlEquals( fc.iterator().next(), "aixm/expected/crane_5.xml" );
    }

    public void testBuildFeatureDisambiguationOperatorQuery()
                            throws Exception {
        fs = setUpFeatureStore( "aixm-hybrid", "aixm/workspace" );
        importGml( fs, "aixm/data/Donlon.xml", USE_EXISTING );
        final Query query = buildGmlIdentifierQuery( "8c755520-b42b-11e3-a5e2-0800500c9a66", null );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 1, fc.size() );
        assertGmlEquals( fc.iterator().next(), "aixm/expected/crane_5.xml" );
    }

    public void testBuildFeatureDisambiguationIdQuery()
                            throws Exception {
        fs = setUpFeatureStore( "aixm-hybrid", "aixm/workspace" );
        importGml( fs, "aixm/data/Donlon.xml", USE_EXISTING );
        final Filter filter = new IdFilter( "uuid.8c755520-b42b-11e3-a5e2-0800500c9a66" );
        final Query query = new Query( null, filter, -1, -1, -1 );
        final FeatureCollection fc = fs.query( query ).toCollection();
        assertEquals( 1, fc.size() );
        assertGmlEquals( fc.iterator().next(), "aixm/expected/crane_5.xml" );
    }

}