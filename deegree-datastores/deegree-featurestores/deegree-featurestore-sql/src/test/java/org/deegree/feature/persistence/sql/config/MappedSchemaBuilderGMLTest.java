package org.deegree.feature.persistence.sql.config;

import static org.deegree.commons.xml.jaxb.JAXBUtils.unmarshall;
import static org.deegree.feature.persistence.sql.SqlFeatureStoreProvider.CONFIG_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;

import org.deegree.feature.persistence.FeatureStoreException;
import org.deegree.feature.persistence.sql.FeatureTypeMapping;
import org.deegree.feature.persistence.sql.MappedAppSchema;
import org.deegree.feature.persistence.sql.jaxb.FeatureTypeMappingJAXB;
import org.deegree.feature.persistence.sql.jaxb.SQLFeatureStoreJAXB;
import org.deegree.feature.persistence.sql.jaxb.SQLFeatureStoreJAXB.BLOBMapping;
import org.deegree.feature.persistence.sql.jaxb.SQLFeatureStoreJAXB.NamespaceHint;
import org.deegree.feature.persistence.sql.jaxb.StorageCRS;
import org.deegree.feature.persistence.sql.rules.BlobParticleMapping;
import org.deegree.feature.persistence.sql.rules.CompoundMapping;
import org.deegree.feature.persistence.sql.rules.Mapping;
import org.junit.Test;

/**
 * Test cases for {@link MappedSchemaBuilderGML}.
 *
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 *
 * @since 3.4
 */
public class MappedSchemaBuilderGMLTest {

    private static final String FEATURE_STORE_HYBRID = "../aixm/datasources/feature/aixm-hybrid.xml";

    private static final String FEATURE_STORE_RELATIONAL = "../aixm/datasources/feature/aixm-relational.xml";

    private static final String CONFIG_JAXB_PACKAGE = "org.deegree.feature.persistence.sql.jaxb";

    private static final String AIXM_NS = "http://www.aixm.aero/schema/5.1";

    private static final QName VERTICAL_STRUCTURE = new QName( AIXM_NS, "VerticalStructure" );

    private MappedSchemaBuilderGML builder;

    @Test
    public void testSkipOnReconstructFlagForRelationalOnlyTimeSliceMapping()
                            throws JAXBException, FeatureStoreException, IOException {
        builder = setup( FEATURE_STORE_RELATIONAL );
        final MappedAppSchema schema = builder.getMappedSchema();
        final FeatureTypeMapping ftMapping = schema.getFtMapping( VERTICAL_STRUCTURE );
        final List<Mapping> mappings = ftMapping.getMappings();
        final CompoundMapping timeSliceMapping = (CompoundMapping) mappings.get( 0 );
        final List<Mapping> timeSliceChildren = timeSliceMapping.getParticles();

        // RelationalMapping of VerticalStructureTimeSlice is flagged for reconstruction
        final CompoundMapping relationalMapping = (CompoundMapping) timeSliceChildren.get( 0 );
        assertEquals( new QName( AIXM_NS, "VerticalStructureTimeSlice" ), relationalMapping.getPath().getAsQName() );
        assertFalse( relationalMapping.isSkipOnReconstruct() );
        // skipReconstruct flag is applied to child particles as well
        assertFalse( relationalMapping.getParticles().get( 0 ).isSkipOnReconstruct() );
    }

    @Test
    public void testSkipOnReconstructFlagForHybridTimeSliceMapping()
                            throws JAXBException, FeatureStoreException, IOException {
        builder = setup( FEATURE_STORE_HYBRID );
        final MappedAppSchema schema = builder.getMappedSchema();
        final FeatureTypeMapping ftMapping = schema.getFtMapping( VERTICAL_STRUCTURE );
        final List<Mapping> mappings = ftMapping.getMappings();
        final CompoundMapping timeSliceMapping = (CompoundMapping) mappings.get( 0 );
        final List<Mapping> timeSliceChildren = timeSliceMapping.getParticles();

        // BlobMapping of VerticalStructureTimeSlice is flagged for reconstruction
        final BlobParticleMapping blobMapping = (BlobParticleMapping) timeSliceChildren.get( 0 );
        assertEquals( new QName( AIXM_NS, "VerticalStructureTimeSlice" ), blobMapping.getPath().getAsQName() );
        assertFalse( blobMapping.isSkipOnReconstruct() );

        // RelationalMapping of VerticalStructureTimeSlice is not flagged for reconstruction
        final CompoundMapping relationalMapping = (CompoundMapping) timeSliceChildren.get( 1 );
        assertEquals( new QName( AIXM_NS, "VerticalStructureTimeSlice" ), relationalMapping.getPath().getAsQName() );
        assertTrue( relationalMapping.isSkipOnReconstruct() );
        // skipReconstruct flag is applied to child particles as well
        assertTrue( relationalMapping.getParticles().get( 0 ).isSkipOnReconstruct() );
    }

    private MappedSchemaBuilderGML setup( final String resourcePath )
                            throws JAXBException, FeatureStoreException, IOException {
        final URL configUrl = MappedSchemaBuilderGMLTest.class.getResource( resourcePath );
        final SQLFeatureStoreJAXB config = unmarshallConfig( configUrl );
        final List<String> gmlSchemas = config.getGMLSchema();
        final StorageCRS storageCRS = config.getStorageCRS();
        final List<NamespaceHint> nsHints = config.getNamespaceHint();
        final BLOBMapping blobConf = config.getBLOBMapping();
        final List<FeatureTypeMappingJAXB> ftMappingConfs = config.getFeatureTypeMapping();
        final boolean deleteCascadingByDB = true;
        return new MappedSchemaBuilderGML( configUrl.toString(), gmlSchemas, storageCRS, nsHints, blobConf,
                                           ftMappingConfs, deleteCascadingByDB );
    }

    private SQLFeatureStoreJAXB unmarshallConfig( final URL configUrl )
                            throws JAXBException, IOException {
        final SQLFeatureStoreJAXB config = (SQLFeatureStoreJAXB) unmarshall( CONFIG_JAXB_PACKAGE, CONFIG_SCHEMA,
                                                                             configUrl.openStream(), null );
        return config;
    }

}
