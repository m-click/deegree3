//$HeadURL: svn+ssh://criador.lat-lon.de/srv/svn/deegree-intern/trunk/latlon-sqldialect-mssql/src/main/java/de/latlon/deegree/sqldialect/mssql/MSSQLDDLCreator.java $
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

import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;

import org.deegree.commons.jdbc.TableName;
import org.deegree.commons.utils.ArrayUtils;
import org.deegree.feature.persistence.sql.MappedAppSchema;
import org.deegree.sqldialect.SQLDialect;

/**
 * Creates PostGIS-DDL (DataDefinitionLanguage) scripts from {@link MappedAppSchema} instances.
 * 
 * @author <a href="mailto:schneider@lat-lon.de">Markus Schneider</a>
 * @author last edited by: $Author: schneider $
 * 
 * @version $Revision: 328 $, $Date: 2011-07-01 11:31:23 +0200 (Fr, 01. Jul 2011) $
 */
public class MSSQLDDLCreator extends DDLCreator {

    /**
     * Creates a new {@link MSSQLDDLCreator} instance for the given {@link MappedAppSchema}.
     * 
     * @param schema
     *            mapped application schema, must not be <code>null</code>
     * @param dialect
     *            SQL dialect, must not be <code>null</code>
     */
    public MSSQLDDLCreator( MappedAppSchema schema, SQLDialect dialect ) {
        super( schema, dialect );
    }

    @Override
    protected List<String> getBLOBCreates() {
        List<String> ddl = new ArrayList<String>();

        // create feature_type table
        TableName ftTable = schema.getBBoxMapping().getTable();
        ddl.add( "CREATE TABLE " + ftTable + " (id integer PRIMARY KEY, qname text NOT NULL, bbox GEOMETRY)" );

        // populate feature_type table
        for ( short ftId = 0; ftId < schema.getFts(); ftId++ ) {
            QName ftName = schema.getFtName( ftId );
            ddl.add( "INSERT INTO " + ftTable + "  (id,qname) VALUES (" + ftId + ",'" + ftName + "')" );
        }

        // create gml_objects table
        TableName blobTable = schema.getBlobMapping().getTable();
        ddl.add( "CREATE TABLE " + blobTable + " (id integer IDENTITY(1,1) PRIMARY KEY, "
                 + "gml_id varchar(2000) NOT NULL, ft_type integer REFERENCES " + ftTable
                 + " , binary_object varbinary(max), gml_bounded_by GEOMETRY)" );

        ddl.add( "ALTER TABLE " + blobTable
                 + " ADD CONSTRAINT gml_objects_geochk CHECK (gml_bounded_by.STIsValid() = 1)" );

        double[] dom = schema.getBlobMapping().getCRS().getValidDomain();

        ddl.add( "CREATE SPATIAL INDEX gml_objects_sidx ON " + blobTable + "(gml_bounded_by) WITH ( BOUNDING_BOX = ( "
                 + ArrayUtils.join( ",", dom ) + " ) )" );
        // ddl.add( "CREATE INDEX gml_objects_sidx ON " + blobTable + "  USING GIST (gml_bounded_by GIST_GEOMETRY_OPS)"
        // );
        // ddl.add( "CREATE TABLE gml_names (gml_object_id integer REFERENCES gml_objects,"
        // + "name text NOT NULL,codespace text,prop_idx smallint NOT NULL)" );
        return ddl;
    }

}
