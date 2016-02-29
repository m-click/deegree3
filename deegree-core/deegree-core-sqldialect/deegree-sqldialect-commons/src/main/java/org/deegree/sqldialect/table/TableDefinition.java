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
package org.deegree.sqldialect.table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.deegree.commons.jdbc.SQLIdentifier;
import org.deegree.commons.jdbc.TableName;

/**
 * Definition of a table in an SQL database.
 * 
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 * 
 * @since 3.4
 */
public class TableDefinition {

    private final TableName name;

    private final Map<SQLIdentifier, ColumnDefinition> columnNameToColumn = new LinkedHashMap<SQLIdentifier, ColumnDefinition>();

    /**
     * Creates a new <code>TableDefinition</code> instance.
     * 
     * @param name
     *            name of the table, must not be <code>null</code>
     */
    public TableDefinition( final TableName name ) {
        this.name = name;
    }

    /**
     * Returns the name of table.
     * 
     * @return name of table, never <code>null</code>
     */
    public TableName getName() {
        return name;
    }

    /**
     * Returns the columns of the table.
     * 
     * @return columns of the table, never <code>null</code>
     */
    public List<ColumnDefinition> getColumns() {
        return new ArrayList<ColumnDefinition>( columnNameToColumn.values() );
    }

    /**
     * Returns the specified column.
     * 
     * @param name
     *            name of the column, must not be <code>null</code>
     * @return specified column, may be <code>null</code> (no such column)
     */
    public ColumnDefinition getColumn( final SQLIdentifier name ) {
        return columnNameToColumn.get( name );
    }

    /**
     * Returns the primary key columns.
     *
     * @return primary key columns, can be empty, but never <code>null</code>
     */
    public List<PrimitiveColumnDefinition> getPrimaryKeyColumns() {
        final List<PrimitiveColumnDefinition> pkColumns = new ArrayList<PrimitiveColumnDefinition>();
        for ( final ColumnDefinition column : columnNameToColumn.values() ) {
            if ( column instanceof PrimitiveColumnDefinition ) {
                final PrimitiveColumnDefinition primitiveColumn = (PrimitiveColumnDefinition) column;
                if ( primitiveColumn.isPrimaryKey() ) {
                    pkColumns.add( primitiveColumn );
                }
            }
        }
        return pkColumns;
    }

    /**
     * Adds a column definition, if it already exists, it is merged with the existing one..
     *
     * @param column
     *            column definition, must not be <code>null</code>
     */
    public void addColumn( final ColumnDefinition column ) {
        final ColumnDefinition existing = columnNameToColumn.get( column.getName() );
        if ( existing != null ) {
            existing.merge( column );
        } else {
            columnNameToColumn.put( column.getName(), column );
        }
    }

}
