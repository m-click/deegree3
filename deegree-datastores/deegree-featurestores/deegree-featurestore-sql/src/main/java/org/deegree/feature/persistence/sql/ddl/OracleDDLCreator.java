//$HeadURL: svn+ssh://criador.lat-lon.de/srv/svn/deegree-intern/trunk/latlon-sqldialect-oracle/src/main/java/de/latlon/deegree/sqldialect/oracle/OracleDDLCreator.java $
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
import org.deegree.feature.persistence.sql.MappedAppSchema;
import org.deegree.sqldialect.SQLDialect;

/**
 * Creates Oracle-DDL (DataDefinitionLanguage) scripts from {@link MappedAppSchema} instances.
 * 
 * @author <a href="mailto:schneider@lat-lon.de">Markus Schneider</a>
 * @author last edited by: $Author: schneider $
 * 
 * @version $Revision: 348 $, $Date: 2011-07-01 18:02:24 +0200 (Fr, 01. Jul 2011) $
 */
public class OracleDDLCreator extends DDLCreator {

    /**
     * Creates a new {@link OracleDDLCreator} instance for the given {@link MappedAppSchema}.
     * 
     * @param schema
     *            mapped application schema, must not be <code>null</code>
     * @param dialect
     *            SQL dialect, must not be <code>null</code>
     */
    public OracleDDLCreator( MappedAppSchema schema, SQLDialect dialect ) {
        super( schema, dialect );
    }

    @Override
    protected List<String> getBLOBCreates() {

        List<String> ddl = new ArrayList<String>();

        // create feature_type table
        TableName ftTable = schema.getBBoxMapping().getTable();
        ddl.add( "CREATE TABLE " + ftTable
                 + " (id integer PRIMARY KEY, qname varchar2(4000) NOT NULL, bbox sdo_geometry)" );

        // populate feature_type table
        for ( short ftId = 0; ftId < schema.getFts(); ftId++ ) {
            QName ftName = schema.getFtName( ftId );
            ddl.add( "INSERT INTO " + ftTable + "  (id,qname) VALUES (" + ftId + ",'" + ftName + "')" );
        }

        // create gml_objects table
        TableName blobTable = schema.getBlobMapping().getTable();
        ddl.add( "CREATE TABLE " + blobTable + " (id integer not null, "
                 + "gml_id varchar2(4000) NOT NULL, ft_type integer REFERENCES " + ftTable
                 + " , binary_object blob, gml_bounded_by sdo_GEOMETRY, constraint gml_objects_id_pk primary key(id))" );

        ddl.add( "create sequence " + blobTable + "_id_seq start with 1 increment by 1 nomaxvalue" );

        ddl.add( "create or replace trigger " + blobTable + "_id_trigger before insert on " + blobTable
                 + " for each row begin select " + blobTable + "_id_seq.nextval into :new.id from dual; end;" );

        double[] dom = schema.getBlobMapping().getCRS().getValidDomain();

        ddl.add( "INSERT INTO user_sdo_geom_metadata(TABLE_NAME,COLUMN_NAME,DIMINFO,SRID) VALUES (" + "'" + blobTable
                 + "','gml_bounded_by',SDO_DIM_ARRAY(" + "SDO_DIM_ELEMENT('X', " + dom[0] + ", " + dom[2]
                 + ", 0.00000005), SDO_DIM_ELEMENT('Y', " + dom[1] + ", " + dom[3] + ", 0.00000005)), null)" );

        // TODO validity check, how?
        // ddl.add( "ALTER TABLE " + blobTable
        // + " ADD CONSTRAINT gml_objects_geochk CHECK (" + blobTable + ".gml_bounded_by.st_isvalid()=1)" );

        ddl.add( "CREATE INDEX gml_objects_sidx ON " + blobTable + "(gml_bounded_by) INDEXTYPE IS MDSYS.SPATIAL_INDEX" );
        // ddl.add( "CREATE INDEX gml_objects_sidx ON " + blobTable + "  USING GIST (gml_bounded_by GIST_GEOMETRY_OPS)"
        // );
        // ddl.add( "CREATE TABLE gml_names (gml_object_id integer REFERENCES gml_objects,"
        // + "name text NOT NULL,codespace text,prop_idx smallint NOT NULL)" );
        return ddl;
    }

}
