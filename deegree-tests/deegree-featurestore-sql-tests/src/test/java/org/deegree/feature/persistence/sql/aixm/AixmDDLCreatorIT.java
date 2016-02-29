package org.deegree.feature.persistence.sql.aixm;

import static org.apache.commons.io.IOUtils.readLines;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.deegree.commons.utils.StringUtils;
import org.deegree.feature.persistence.FeatureStoreException;
import org.deegree.feature.persistence.sql.MappedAppSchema;
import org.deegree.feature.persistence.sql.SQLFeatureStore;
import org.deegree.feature.persistence.sql.config.MappedSchemaBuilderGML;
import org.deegree.feature.persistence.sql.ddl.DDLCreator;
import org.deegree.feature.persistence.sql.ddl.PostGISDDLCreator;
import org.deegree.feature.persistence.sql.jaxb.FeatureTypeMappingJAXB;
import org.deegree.feature.persistence.sql.jaxb.SQLFeatureStoreJAXB;
import org.deegree.feature.persistence.sql.jaxb.SQLFeatureStoreJAXB.BLOBMapping;
import org.deegree.feature.persistence.sql.jaxb.SQLFeatureStoreJAXB.NamespaceHint;
import org.deegree.feature.persistence.sql.jaxb.StorageCRS;
import org.deegree.sqldialect.postgis.PostGISDialect;
import org.junit.Test;

/**
 * Tests the DDL generation behaviour of the {@link SQLFeatureStore} for an AIXM configuration.
 *
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 *
 * @since 3.4
 */
public class AixmDDLCreatorIT {

    private static final String CONFIG_JAXB_PACKAGE = "org.deegree.feature.persistence.sql.jaxb";

    private DDLCreator ddlCreator;

    @Test
    public void getDDLForRelationalConfiguration()
                            throws IOException, FeatureStoreException, JAXBException {
        ddlCreator = getDdlCreator( "workspace/datasources/feature/aixm-relational.xml" );
        final String[] actualStmts = ddlCreator.getDDL();
        final List<String> expectedStmts = readLines( AixmDDLCreatorIT.class.getResourceAsStream( "expected/ddl/create-relational.sql" ),
                                                      "UTF-8" );
        final String actual = StringUtils.concat( Arrays.asList( actualStmts ), ";\n" ) + ";";
        final String expected = StringUtils.concat( expectedStmts, "\n" );
        assertEquals( expected, actual );
    }

    @Test
    public void getDDLForBlobConfiguration()
                            throws IOException, FeatureStoreException, JAXBException {
        ddlCreator = getDdlCreator( "workspace/datasources/feature/aixm-blob.xml" );
        final String[] actualStmts = ddlCreator.getDDL();
        final List<String> expectedStmts = readLines( AixmDDLCreatorIT.class.getResourceAsStream( "expected/ddl/create-blob.sql" ),
                                                      "UTF-8" );
        final String actual = StringUtils.concat( Arrays.asList( actualStmts ), ";\n" ) + ";";
        final String expected = StringUtils.concat( expectedStmts, "\n" );
        assertEquals( expected, actual );
    }

    private DDLCreator getDdlCreator( final String resourcePath )
                            throws FeatureStoreException, JAXBException {
        final MappedAppSchema schema = getSchemaBuilder( resourcePath ).getMappedSchema();
        return new PostGISDDLCreator( schema, new PostGISDialect( "2.1" ) );
    }

    private MappedSchemaBuilderGML getSchemaBuilder( final String resourcePath )
                            throws FeatureStoreException, JAXBException {
        final URL url = AixmDDLCreatorIT.class.getResource( resourcePath );
        final SQLFeatureStoreJAXB jaxbConfig = unmarshallConfig( url );
        final List<String> gmlSchemas = jaxbConfig.getGMLSchema();
        final StorageCRS storageCRS = jaxbConfig.getStorageCRS();
        final List<NamespaceHint> nsHints = jaxbConfig.getNamespaceHint();
        final BLOBMapping blobConf = jaxbConfig.getBLOBMapping();
        final List<FeatureTypeMappingJAXB> ftMappingConfs = jaxbConfig.getFeatureTypeMapping();
        final boolean deleteCascadingByDB = false;
        return new MappedSchemaBuilderGML( url.toExternalForm(), gmlSchemas, storageCRS, nsHints, blobConf,
                                           ftMappingConfs, deleteCascadingByDB );
    }

    private SQLFeatureStoreJAXB unmarshallConfig( final URL url )
                            throws JAXBException {
        final JAXBContext jc = JAXBContext.newInstance( CONFIG_JAXB_PACKAGE );
        final Unmarshaller unmarshaller = jc.createUnmarshaller();
        return (SQLFeatureStoreJAXB) unmarshaller.unmarshal( url );
    }

}
