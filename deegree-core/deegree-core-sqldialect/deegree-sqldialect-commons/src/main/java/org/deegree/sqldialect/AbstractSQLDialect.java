//$HeadURL$
/*----------------------------------------------------------------------------
 This file is part of deegree, http://deegree.org/
 Copyright (C) 2001-2014 by:
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
package org.deegree.sqldialect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.deegree.commons.jdbc.TableName;
import org.deegree.sqldialect.table.ColumnDefinition;
import org.deegree.sqldialect.table.PrimitiveColumnDefinition;
import org.deegree.sqldialect.table.TableDefinition;

/**
 * Implementations provide the vendor-specific behavior for a spatial DBMS so it can be accessed by deegree.
 *
 * @author <a href="mailto:wanhoff@lat-lon.de">Jeronimo Wanhoff</a>
 * @author last edited by: $Author: wanhoff $
 *
 */
public abstract class AbstractSQLDialect implements SQLDialect{

    private char defaultEscapeChar = Character.UNASSIGNED;

    @Override
    public char getLeadingEscapeChar() {
        return defaultEscapeChar;
    }

    @Override
    public char getTailingEscapeChar() {
        return defaultEscapeChar;
    }

    @Override
    public Collection<String> getCreateTableStatements( final TableDefinition table ) {
        final Collection<String> statements = new ArrayList<String>();
        statements.add( getPrimaryCreateTableStatement( table ) );
        statements.addAll( getAdditionalCreateTableStatements( table ) );
        return statements;
    }

    protected String getPrimaryCreateTableStatement( final TableDefinition table ) {
        final StringBuilder sql = new StringBuilder();
        sql.append( "CREATE TABLE " );
        sql.append( table.getName() );
        sql.append( " (" );
        boolean first = true;
        for ( final ColumnDefinition column : table.getColumns() ) {
            final String columnSqlSnippet = getCreateSnippet( column );
            if ( columnSqlSnippet != null ) {
                if ( !first ) {
                    sql.append( "," );
                } else {
                    first = false;
                }
                sql.append( "\n  " );
                sql.append( columnSqlSnippet );
            }
        }
        final List<PrimitiveColumnDefinition> pkColumns = table.getPrimaryKeyColumns();
        if ( !pkColumns.isEmpty() ) {
            sql.append( ",\n  CONSTRAINT " );
            sql.append( getPkConstraintName( table.getName() ) );
            sql.append( " PRIMARY KEY (" );
            first = true;
            for ( final ColumnDefinition pkColumn : pkColumns ) {
                if ( !first ) {
                    sql.append( "," );
                } else {
                    first = false;
                }
                sql.append( pkColumn.getName() );
            }
            sql.append( ')' );
        }
        sql.append( "\n)" );
        return sql.toString();
    }

    protected Collection<String> getAdditionalCreateTableStatements( final TableDefinition table ) {
        final Collection<String> stmts = new ArrayList<String>();
        for ( final ColumnDefinition column : table.getColumns() ) {
            stmts.addAll( getAdditionalCreateStatements( column, table ) );
        }
        return stmts;
    }

    /**
     * Returns the SQL snippet for creating the given column.
     *
     * @param column
     *            column definition, must not be <code>null</code>
     * @return SQL snippet for creating the given column, can be <code>null</code>
     */
    protected abstract String getCreateSnippet( final ColumnDefinition column );

    /**
     * Returns additional SQL statements for creating the given column.
     *
     * @param column
     *            column definition, must not be <code>null</code>
     * @return SQL statements for creating the given column, can be empty, but never <code>null</code>
     */
    protected abstract Collection<String> getAdditionalCreateStatements( final ColumnDefinition column,
                                                                         final TableDefinition table );

    private String getPkConstraintName( TableName ftTable ) {
        String s = null;
        String table = ftTable.getTable();
        if ( table.endsWith( "\"" ) ) {
            s = table.substring( 0, table.length() - 1 ) + "_pkey\"";
        } else {
            s = table + "_pkey";
        }
        return s;
    }

}
