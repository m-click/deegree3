package org.deegree.feature.persistence.sql.ddl;

import static org.deegree.commons.utils.JDBCUtils.close;
import static org.deegree.commons.utils.JDBCUtils.rollbackQuietly;
import static org.slf4j.LoggerFactory.getLogger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.deegree.feature.persistence.sql.MappedAppSchema;
import org.deegree.feature.persistence.sql.SQLFeatureStore;
import org.deegree.feature.persistence.sql.jaxb.SQLFeatureStoreJAXB.TableSetup;
import org.deegree.sqldialect.SQLDialect;
import org.slf4j.Logger;

/**
 * Takes care of the table setup for the {@link SQLFeatureStore},
 *
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 *
 * @since 3.4
 */
public class TableSetupHelper {

    private static final Logger LOG = getLogger( TableSetupHelper.class );

    private final Connection conn;

    /**
     * Creates a new {@link TableSetupHelper} instance.
     *
     * @param conn
     *            connection, must not be <code>null</code>
     * @throws SQLException
     */
    public TableSetupHelper( final Connection conn ) throws SQLException {
        this.conn = conn;
        conn.setAutoCommit( false );
    }

    /**
     * Executes SQL statements to setup up the configured tables.
     *
     * @param tableSetup
     *            configuration, must not be <code>null</code>
     * @param schema
     *            mapped application schema, must not be <code>null</code>
     * @param dialect
     *            SQL dialect, must not be <code>null</code>
     * @throws SQLException
     */
    public void setupTables( final TableSetup tableSetup, final MappedAppSchema schema, final SQLDialect dialect )
                            throws SQLException {
        LOG.info( "TableSetup configured. Checking if setup has been performed already." );
        if ( executeBooleanTestStatement( tableSetup.getTestSql() ) ) {
            LOG.info( "Yes." );
        } else {
            LOG.info( "No. Performing setup of tables." );
            final List<String> ddlStatements = new ArrayList<String>();
            if ( tableSetup.isDeriveTablesFromMapping() ) {
                LOG.info( "Deriving tables from mapping." );
                final DDLCreator ddlCreator = DDLCreator.newInstance( schema, dialect );
                ddlStatements.addAll( Arrays.asList( ddlCreator.getDDL() ) );
            }
            ddlStatements.addAll( tableSetup.getSql() );
            executeDdlStatements( ddlStatements );
            conn.commit();
        }
    }

    private boolean executeBooleanTestStatement( final String testStatement )
                            throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery( testStatement );
            rs.next();
            return rs.getBoolean( 1 );
        } finally {
            close( rs, stmt, null, LOG );
        }
    }

    private void executeDdlStatements( final List<String> ddlStatements )
                            throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            for ( final String sql : ddlStatements ) {
                LOG.info( "Executing: " + sql );
                stmt.execute( sql );
            }
            conn.commit();
        } catch ( SQLException e ) {
            rollbackQuietly( conn );
            throw e;
        } finally {
            close( rs, stmt, null, LOG );
        }
    }

}
