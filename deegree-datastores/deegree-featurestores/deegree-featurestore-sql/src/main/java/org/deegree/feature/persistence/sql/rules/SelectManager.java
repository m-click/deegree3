package org.deegree.feature.persistence.sql.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.deegree.commons.jdbc.SQLIdentifier;
import org.deegree.commons.tom.TypedObjectNode;
import org.deegree.commons.tom.primitive.BaseType;
import org.deegree.commons.tom.sql.ParticleConverter;
import org.deegree.commons.utils.Pair;
import org.deegree.feature.persistence.sql.FeatureTypeMapping;
import org.deegree.feature.persistence.sql.expressions.TableJoin;
import org.deegree.feature.persistence.sql.id.FIDMapping;
import org.deegree.sqldialect.filter.TableAliasManager;

/**
 * Responsible for building SELECT statements and tracking the structure of the expected result set.
 * 
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 *
 * @since 3.4
 */
class SelectManager {

    final TableAliasManager aliasManager;

    // SQL expressions to be SELECTed, e.g. X1.column, "VALUE" or
    // (CASE WHEN X1.column IS NULL THEN false ELSE true END)
    final Set<String> selectTerms = new LinkedHashSet<String>();

    // qualified with table alias, e.g. X1.id
    final Set<String> orderColumns = new LinkedHashSet<String>();

    // example: "LEFT OUTER JOIN table2 X2 ON X1.id=X2.fk"
    final List<String> joins = new ArrayList<String>();

    // key: mapping particle, value: corresponding table alias
    final Map<Mapping, String> mappingToTableAlias = new HashMap<Mapping, String>();

    private String rootTable;

    SelectManager( final TableAliasManager aliasManager ) {
        this.aliasManager = aliasManager;
    }

    void add( final FeatureTypeMapping ftMapping ) {
        rootTable = ftMapping.getFtTable().toString();
        final String currentTableAlias = aliasManager.getRootTableAlias();
        final FIDMapping fidMapping = ftMapping.getFidMapping();
        for ( final Pair<SQLIdentifier, BaseType> column : fidMapping.getColumns() ) {
            final String sqlTerm = currentTableAlias + "." + column.getFirst().getName();
            selectTerms.add( sqlTerm );
            orderColumns.add( sqlTerm );
        }
        if ( ftMapping.getTypeColumn() != null ) {
            selectTerms.add( currentTableAlias + "." + ftMapping.getTypeColumn().getName() );
        }
    }

    void add( final Mapping mapping, final ParticleConverter<?> particleConverter, final Mapping parent ) {
        if ( mapping.isSkipOnReconstruct() ) {
            return;
        }
        String currentTableAlias = mappingToTableAlias.get( parent );
        if ( parent == null ) {
            currentTableAlias = aliasManager.getRootTableAlias();
        }
        add( mapping, particleConverter, currentTableAlias );
    }

    final String getSelectTerms() {
        return createCsv( selectTerms );
    }

    final String getOrderColumns() {
        return createCsv( orderColumns );
    }

    final String getJoins() {
        return createCsv( joins );
    }

    int getResultSetIndex( final FIDMapping fidMapping ) {
        final String tableAlias = aliasManager.getRootTableAlias();
        return getResultSetIndex( tableAlias + "." + fidMapping.getColumns().get( 0 ).first.getName() );
    }

    int getResultSetIndex( final Mapping mapping, final ParticleConverter<TypedObjectNode> particleConverter ) {
        if ( particleConverter != null ) {
            final String tableAlias = mappingToTableAlias.get( mapping );
            final String selectSnippet = particleConverter.getSelectSnippet( tableAlias );
            if ( selectSnippet != null ) {
                return getResultSetIndex( selectSnippet );
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        final StringBuilder sql = new StringBuilder( "SELECT " );
        sql.append( getSelectTerms() );
        sql.append( " FROM " );
        sql.append( rootTable );
        sql.append( ' ' );
        sql.append( aliasManager.getRootTableAlias() );
        if ( getJoins() != null ) {
            sql.append( ' ' );
            sql.append( getJoins() );
        }
        if ( getOrderColumns() != null ) {
            sql.append( " ORDER BY " );
            sql.append( getOrderColumns() );
        }
        return sql.toString();
    }

    private void add( final Mapping mapping, final ParticleConverter<?> particleConverter, String currentTableAlias ) {
        if ( mapping.getJoinedTable() != null ) {
            currentTableAlias = followJoins( mapping.getJoinedTable(), currentTableAlias );
        }
        mappingToTableAlias.put( mapping, currentTableAlias );
        if ( particleConverter != null ) {
            final String selectSnippet = particleConverter.getSelectSnippet( currentTableAlias );
            if ( selectSnippet != null ) {
                selectTerms.add( selectSnippet );
            }
        }
    }

    private int getResultSetIndex( final String sqlTerm ) {
        int i = 1;
        for ( final String selectColumn : selectTerms ) {
            if ( selectColumn.equals( sqlTerm ) ) {
                return i;
            }
            i++;
        }
        return -1;
    }

    private String followJoins( final List<TableJoin> joins, String currentTableAlias ) {
        for ( final TableJoin join : joins ) {
            currentTableAlias = followJoin( join, currentTableAlias );
        }
        return currentTableAlias;
    }

    private String followJoin( final TableJoin join, final String currentTableAlias ) {
        final String nextTableAlias = aliasManager.generateNew();
        final List<SQLIdentifier> fromColumns = join.getFromColumns();
        final List<SQLIdentifier> toColumns = join.getToColumns();
        final StringBuilder sql = new StringBuilder( "LEFT OUTER JOIN " );
        sql.append( join.getToTable() );
        sql.append( ' ' );
        sql.append( nextTableAlias );
        sql.append( " ON " );
        for ( int i = 0; i < fromColumns.size(); i++ ) {
            if ( i != 0 ) {
                sql.append( " AND " );
            }
            sql.append( currentTableAlias );
            sql.append( '.' );
            sql.append( fromColumns.get( i ) );
            sql.append( '=' );
            sql.append( nextTableAlias );
            sql.append( '.' );
            sql.append( toColumns.get( i ) );
        }
        joins.add( sql.toString() );
        if ( join.getOrderColumns() != null ) {
            for ( final SQLIdentifier orderColumn : join.getOrderColumns() ) {
                orderColumns.add( nextTableAlias + '.' + orderColumn.getName() );
            }
        }
        return nextTableAlias;
    }

    private String createCsv( final Collection<?> values ) {
        final StringBuilder sb = new StringBuilder();
        boolean first = true;
        for ( final Object value : values ) {
            if ( !first ) {
                sb.append( ',' );
            } else {
                first = false;
            }
            sb.append( value );
        }
        return sb.toString();
    }

}
