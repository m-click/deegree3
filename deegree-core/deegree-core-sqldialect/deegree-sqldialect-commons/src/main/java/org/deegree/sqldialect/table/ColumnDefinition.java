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

import org.deegree.commons.jdbc.SQLIdentifier;

/**
 * Definition of a column of a table in an SQL database.
 * 
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 * 
 * @since 3.4
 */
public abstract class ColumnDefinition {

    private final SQLIdentifier name;

    protected boolean isNotNull;

    /**
     * Creates a new <code>ColumnDefinition</code> instance.
     * 
     * @param name
     *            name of the column, must not be <code>null</code>
     */
    protected ColumnDefinition( final SQLIdentifier name ) {
        this.name = name;
    }

    /**
     * Returns the name of the column.
     * 
     * @return name of the column, never <code>null</code>
     */
    public SQLIdentifier getName() {
        return name;
    }

    /**
     * Returns whether this column must not be NULL.
     * 
     * @return <code>true</code>, if this column must not be NULL, <code>false</code> if it can be NULL
     */
    public boolean getIsNotNull() {
        return isNotNull;
    }

    /**
     * Merges the given {@link ColumnDefinition}.
     *
     * @param other
     *            column definition to be merged, must not be <code>null</code>
     * @throws IllegalArgumentException
     *             if the column definitions are not compatible
     */
    public void merge( final ColumnDefinition other ) {
        final String msg = "Cannot merge column definitions: " + name + " vs. " + other.name;
        throw new IllegalArgumentException( msg );
    }

}
