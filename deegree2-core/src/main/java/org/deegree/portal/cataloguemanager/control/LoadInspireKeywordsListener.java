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
package org.deegree.portal.cataloguemanager.control;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import org.deegree.enterprise.control.ajax.ResponseHandler;
import org.deegree.enterprise.control.ajax.WebEvent;

/**
 * TODO add class documentation here
 * 
 * @author <a href="mailto:name@deegree.org">Andreas Poth</a>
 * @author last edited by: $Author$
 * 
 * @version $Revision$, $Date$
 */
public class LoadInspireKeywordsListener extends AbstractMetadataListener {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.deegree.enterprise.control.ajax.AbstractListener#actionPerformed(org.deegree.enterprise.control.ajax.WebEvent
     * , org.deegree.enterprise.control.ajax.ResponseHandler)
     */
    public void actionPerformed( WebEvent event, ResponseHandler responseHandler )
                            throws IOException {

        String hLevel = (String) event.getParameter().get( "HLEVEL" );
        if ( !hLevel.equals( "service" ) ) {
            hLevel = "dataset";
        }
      
        HttpServletRequest req = (HttpServletRequest) getRequest();
        Locale loc = req.getLocale();
        if ( loc == null ) {
            loc = Locale.getDefault();
        }

        String file = "inspire_" + hLevel + "_keywords_" + loc.getLanguage() + ".properties";

        InputStream is = getClass().getResourceAsStream( "/org/deegree/portal/cataloguemanager/control/resources/" + file );
        Properties p = new Properties();
        p.load( is );
        is.close();
        
        // result page uses UTF-8 encoding
        String charEnc = Charset.defaultCharset().displayName();
        responseHandler.setContentType( "application/json; charset=" + charEnc );        
        responseHandler.writeAndClose( false, p );

    }

}
