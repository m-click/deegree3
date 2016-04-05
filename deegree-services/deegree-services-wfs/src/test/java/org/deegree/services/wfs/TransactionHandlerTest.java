package org.deegree.services.wfs;

import static org.junit.Assert.assertEquals;

import javax.xml.namespace.QName;

import org.deegree.commons.ows.exception.OWSException;
import org.deegree.commons.utils.Pair;
import org.jaxen.BaseXPath;
import org.jaxen.JaxenException;
import org.jaxen.expr.Expr;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link TransactionHandler}.
 *
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 *
 * @since 3.4
 */
public class TransactionHandlerTest {

    private TransactionHandler handler;

    @Before
    public void setup() {
        handler = new TransactionHandler( null, null, null, null );
    }

    @Test
    public void getPropNameAndIndexNumberPredicate()
                            throws OWSException, JaxenException {
        final Expr expr = new BaseXPath( "aixm:timeSlice[1]", null ).getRootExpr();
        final Pair<QName, Integer> propNameAndIndex = handler.getPropNameAndIndex( expr, new QName( "feature" ) );
        assertEquals( 0, propNameAndIndex.second.intValue() );
    }

    @Test
    public void getPropNameAndIndexPositionLast()
                            throws OWSException, JaxenException {
        final Expr expr = new BaseXPath( "aixm:timeSlice[position()=last()]", null ).getRootExpr();
        final Pair<QName, Integer> propNameAndIndex = handler.getPropNameAndIndex( expr, new QName( "feature" ) );
        assertEquals( -1, propNameAndIndex.second.intValue() );
    }

    @Test(expected = OWSException.class)
    public void getPropNameAndIndexPositionLastReversed()
                            throws OWSException, JaxenException {
        final Expr expr = new BaseXPath( "aixm:timeSlice[last()=position()]", null ).getRootExpr();
        handler.getPropNameAndIndex( expr, new QName( "feature" ) );
    }
}
