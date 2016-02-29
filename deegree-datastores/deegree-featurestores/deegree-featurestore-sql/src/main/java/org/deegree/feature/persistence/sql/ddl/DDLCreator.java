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
package org.deegree.feature.persistence.sql.ddl;

import static org.deegree.commons.tom.primitive.BaseType.INTEGER;
import static org.deegree.commons.tom.primitive.BaseType.STRING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.deegree.commons.jdbc.SQLIdentifier;
import org.deegree.commons.jdbc.TableName;
import org.deegree.commons.tom.primitive.BaseType;
import org.deegree.commons.utils.Pair;
import org.deegree.feature.persistence.sql.FeatureTypeMapping;
import org.deegree.feature.persistence.sql.MappedAppSchema;
import org.deegree.feature.persistence.sql.expressions.TableJoin;
import org.deegree.feature.persistence.sql.id.AutoIDGenerator;
import org.deegree.feature.persistence.sql.id.FIDMapping;
import org.deegree.feature.persistence.sql.rules.CompoundMapping;
import org.deegree.feature.persistence.sql.rules.FeatureMapping;
import org.deegree.feature.persistence.sql.rules.GeometryMapping;
import org.deegree.feature.persistence.sql.rules.Mapping;
import org.deegree.feature.persistence.sql.rules.PrimitiveMapping;
import org.deegree.feature.persistence.sql.rules.SqlExpressionMapping;
import org.deegree.sqldialect.SQLDialect;
import org.deegree.sqldialect.filter.DBField;
import org.deegree.sqldialect.filter.MappingExpression;
import org.deegree.sqldialect.postgis.PostGISDialect;
import org.deegree.sqldialect.table.GeometryColumnDefinition;
import org.deegree.sqldialect.table.PrimitiveColumnDefinition;
import org.deegree.sqldialect.table.TableDefinition;

/**
 * Creates DDL (DataDefinitionLanguage) scripts from {@link MappedAppSchema} instances.
 * 
 * @author <a href="mailto:schneider@lat-lon.de">Markus Schneider</a>
 * @author last edited by: $Author$
 * 
 * @version $Revision$, $Date$
 */
public abstract class DDLCreator {

    // private static Logger LOG = LoggerFactory.getLogger( DDLCreator.class );

    protected final MappedAppSchema schema;

    private final boolean hasBlobTable;

    private final SQLDialect dialect;

    protected TableName currentFtTable;

    private final Map<TableName, TableDefinition> tableNameToTable = new LinkedHashMap<TableName, TableDefinition>();

    /**
     * Creates a new {@link DDLCreator} instance for the given {@link MappedAppSchema}.
     * 
     * @param schema
     *            mapped application schema, must not be <code>null</code>
     * @param dialect
     *            SQL dialect, must not be <code>null</code>
     */
    protected DDLCreator( MappedAppSchema schema, SQLDialect dialect ) {
        this.schema = schema;
        this.dialect = dialect;
        hasBlobTable = schema.getBlobMapping() != null;
    }

    /**
     * Returns the DDL statements for creating the relational schema required by the {@link MappedAppSchema}.
     * 
     * @return the DDL statements, never <code>null</code>
     */
    public String[] getDDL() {
        List<String> ddl = new ArrayList<String>();
        if ( hasBlobTable ) {
            ddl.addAll( getBLOBCreates() );
        }
        ddl.addAll( getRelationalCreates() );
        return ddl.toArray( new String[ddl.size()] );
    }

    protected abstract List<String> getBLOBCreates();

    private List<String> getRelationalCreates() {
        final List<String> stmts = new ArrayList<String>();
        buildRelationalModel( getFeatureTypeMappings() );
        for ( final TableDefinition table : tableNameToTable.values() ) {
            stmts.addAll( dialect.getCreateTableStatements( table ) );
        }
        return stmts;
    }

    private Collection<FeatureTypeMapping> getFeatureTypeMappings() {
        final List<FeatureTypeMapping> ftMappings = new ArrayList<FeatureTypeMapping>();
        for ( short ftId = 0; ftId < schema.getFts(); ftId++ ) {
            final QName ftName = schema.getFtName( ftId );
            final FeatureTypeMapping ftMapping = schema.getFtMapping( ftName );
            if ( ftMapping != null ) {
                ftMappings.add( ftMapping );
            }
        }
        return ftMappings;
    }

    // TODO get rid of this (DDLCreator should be the only needed implementation)
    public static DDLCreator newInstance( MappedAppSchema appSchema, SQLDialect dialect ) {
        if ( dialect instanceof PostGISDialect ) {
            return new PostGISDDLCreator( appSchema, dialect );
        }
        if ( dialect.getClass().getSimpleName().equals( "OracleDialect" ) ) {
            return new OracleDDLCreator( appSchema, dialect );
        }
        if ( dialect.getClass().getSimpleName().equals( "MSSQLDialect" ) ) {
            return new MSSQLDDLCreator( appSchema, dialect );
        }
        throw new IllegalArgumentException( "Nod DDLCreator for DB type '" + dialect.getClass().getSimpleName()
                                            + "' available." );
    }

    private void buildRelationalModel( final Collection<FeatureTypeMapping> ftMappings ) {
        for ( final FeatureTypeMapping ftMapping : ftMappings ) {
            currentFtTable = ftMapping.getFtTable();
            buildTableDefinitions( ftMapping );
        }
    }

    private void buildTableDefinitions( final FeatureTypeMapping ftMapping ) {
        final TableDefinition ftTable = getTableDefinition( ftMapping.getFtTable() );

        // feature id columns
        final FIDMapping fidMapping = ftMapping.getFidMapping();
        for ( final Pair<SQLIdentifier, BaseType> fidColumnAndType : fidMapping.getColumns() ) {
            final PrimitiveColumnDefinition fidColumn = new PrimitiveColumnDefinition( fidColumnAndType.first,
                                                                                       fidColumnAndType.second );
            fidColumn.setIsPrimaryKey();
            if ( fidMapping.getIdGenerator() instanceof AutoIDGenerator ) {
                fidColumn.setIsAutogenerated();
            }
            ftTable.addColumn( fidColumn );
        }

        // particle mappings
        for ( final Mapping mapping : ftMapping.getMappings() ) {
            buildTableDefinitions( mapping, ftTable );
        }
    }

    private void buildTableDefinitions( final Mapping mapping, TableDefinition table ) {

        if ( !( mapping instanceof FeatureMapping ) && mapping.getJoinedTable() != null ) {
            table = buildJoinedTable( table, mapping.getJoinedTable().get( 0 ) );
        }

        if ( mapping instanceof PrimitiveMapping ) {
            final PrimitiveMapping primitiveMapping = (PrimitiveMapping) mapping;
            final MappingExpression me = primitiveMapping.getMapping();
            if ( me instanceof DBField ) {
                final DBField dbField = (DBField) me;
                table.addColumn( new PrimitiveColumnDefinition( new SQLIdentifier( dbField.getColumn() ),
                                                                primitiveMapping.getType().getBaseType() ) );
            }
        } else if ( mapping instanceof GeometryMapping ) {
            final GeometryMapping geometryMapping = (GeometryMapping) mapping;
            final MappingExpression me = geometryMapping.getMapping();
            if ( me instanceof DBField ) {
                final DBField dbField = (DBField) me;
                table.addColumn( new GeometryColumnDefinition( new SQLIdentifier( dbField.getColumn() ),
                                                               geometryMapping.getType() ) );
            }
        } else if ( mapping instanceof FeatureMapping ) {
            final SQLIdentifier col = mapping.getJoinedTable().get( mapping.getJoinedTable().size() - 1 ).getFromColumns().get( 0 );
            if ( col != null ) {
                table.addColumn( new PrimitiveColumnDefinition( col, STRING ) );
            }
            final MappingExpression hrefMe = ( (FeatureMapping) mapping ).getHrefMapping();
            if ( hrefMe instanceof DBField ) {
                table.addColumn( new PrimitiveColumnDefinition( new SQLIdentifier( ( (DBField) hrefMe ).getColumn() ),
                                                                STRING ) );
            }
        } else if ( mapping instanceof CompoundMapping ) {
            final CompoundMapping compoundMapping = (CompoundMapping) mapping;
            for ( final Mapping childMapping : compoundMapping.getParticles() ) {
                buildTableDefinitions( childMapping, table );
            }
        } else if ( mapping instanceof SqlExpressionMapping ) {
            // skip
        } else {
            throw new RuntimeException( "Internal error. Unhandled mapping type '" + mapping.getClass() + "'" );
        }
    }

    private TableDefinition buildJoinedTable( final TableDefinition fromTable, final TableJoin jc ) {
        final TableDefinition table = getTableDefinition( jc.getToTable() );

        // primary key column
        final PrimitiveColumnDefinition pk = new PrimitiveColumnDefinition( new SQLIdentifier( "id" ), INTEGER ).setIsPrimaryKey().setIsAutogenerated();
        table.addColumn( pk );

        // foreign key to from table
        final List<PrimitiveColumnDefinition> fromPks = fromTable.getPrimaryKeyColumns();
        if ( fromPks.size() != 1 ) {
            throw new UnsupportedOperationException( "Cannot create join table. From table has " + fromPks.size()
                                                     + " pk columns. Only single pk column is supported." );
        }
        final BaseType fkType = fromPks.get( 0 ).getType();
        final PrimitiveColumnDefinition fk = new PrimitiveColumnDefinition( jc.getToColumns().get( 0 ), fkType );
        fk.setForeignKey( fromTable.getName(), true );
        table.addColumn( fk );

        for ( final SQLIdentifier col : jc.getOrderColumns() ) {
            final PrimitiveColumnDefinition orderColumn = new PrimitiveColumnDefinition( col, INTEGER ).setIsNotNull();
            table.addColumn( orderColumn );
        }
        return table;
    }

    private TableDefinition getTableDefinition( final TableName name ) {
        final TableDefinition existingTable = tableNameToTable.get( name );
        if ( existingTable != null ) {
            return existingTable;
        }
        final TableDefinition newTable = new TableDefinition( name );
        tableNameToTable.put( name, newTable );
        return newTable;
    }

}
