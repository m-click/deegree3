package org.deegree.feature.persistence.sql.rules;

import static org.deegree.feature.persistence.sql.jaxb.FetchModeType.JOIN;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.deegree.feature.persistence.sql.SQLFeatureStore;
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

    final Map<TableJoin, String> joinToTableAlias = new HashMap<TableJoin, String>();

    private String rootTable;

    /**
     * Creates a new {@link SelectManager} instance for an initial SELECT.
     *
     * @param ftMapping
     * @param aliasManager
     * @param fs
     */
    SelectManager( final FeatureTypeMapping ftMapping, final TableAliasManager aliasManager, final SQLFeatureStore fs ) {
        this.aliasManager = aliasManager;
        final String currentTableAlias = aliasManager.getRootTableAlias();
        rootTable = ftMapping.getFtTable().toString();
        final FIDMapping fidMapping = ftMapping.getFidMapping();
        for ( final Pair<SQLIdentifier, BaseType> column : fidMapping.getColumns() ) {
            final String sqlTerm = currentTableAlias + "." + column.getFirst().getName();
            selectTerms.add( sqlTerm );
            orderColumns.add( sqlTerm );
        }
        if ( ftMapping.getTypeColumn() != null ) {
            selectTerms.add( currentTableAlias + "." + ftMapping.getTypeColumn().getName() );
        }
        for ( final Mapping mapping : ftMapping.getMappings() ) {
            add( mapping, currentTableAlias, fs );
        }
    }

    /**
     * Creates a new {@link SelectManager} instance for a subsequent SELECT.
     *
     * @param ftMapping
     * @param aliasManager
     * @param fs
     */
    SelectManager( final Mapping mapping, final TableJoin join, final SQLFeatureStore fs ) {
        this.aliasManager = new TableAliasManager();
        final String currentTableAlias = aliasManager.getRootTableAlias();
        rootTable = join.getToTable().getName();
        mappingToTableAlias.put( mapping, currentTableAlias );
        addToColumns( join, currentTableAlias );
        final ParticleConverter<?> particleConverter = fs.getConverter( mapping );
        if ( particleConverter != null ) {
            final String selectSnippet = particleConverter.getSelectSnippet( currentTableAlias );
            if ( selectSnippet != null ) {
                selectTerms.add( selectSnippet );
            }
        } else if ( mapping instanceof CompoundMapping ) {
            final CompoundMapping cm = (CompoundMapping) mapping;
            for ( final Mapping childMapping : cm.getParticles() ) {
                add( childMapping, currentTableAlias, fs );
            }
        }
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

    int getResultSetIndex( final String sqlTerm ) {
        int i = 1;
        for ( final String selectColumn : selectTerms ) {
            if ( selectColumn.equals( sqlTerm ) ) {
                return i;
            }
            i++;
        }
        return -1;
    }

    LinkedHashMap<String, Integer> getSelectTermToResultSetIdxMap() {
        final LinkedHashMap<String, Integer> selectTermToResultSet = new LinkedHashMap<String, Integer>();
        int idx = 1;
        for ( final String selectTerm : selectTerms ) {
            selectTermToResultSet.put( selectTerm, idx++ );
        }
        return selectTermToResultSet;
    }

    String getTableAlias( final TableJoin join ) {
        return joinToTableAlias.get( join );
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

    private void add( final Mapping mapping, String currentTableAlias, final SQLFeatureStore fs ) {
        if ( mapping.isSkipOnReconstruct() ) {
            return;
        }
        if ( mapping.getJoinedTable() != null && !( mapping instanceof FeatureMapping ) ) {
            if ( mapping.getJoinedTable().get( 0 ).getFetchMode() == JOIN ) {
                currentTableAlias = followJoins( mapping.getJoinedTable(), currentTableAlias );
            } else {
                addFromColumns( mapping.getJoinedTable(), currentTableAlias );
                mappingToTableAlias.put( mapping, currentTableAlias );
                return;
            }
        }
        mappingToTableAlias.put( mapping, currentTableAlias );
        final ParticleConverter<?> particleConverter = fs.getConverter( mapping );
        if ( particleConverter != null ) {
            final String selectSnippet = particleConverter.getSelectSnippet( currentTableAlias );
            if ( selectSnippet != null ) {
                selectTerms.add( selectSnippet );
            }
        } else if ( mapping instanceof CompoundMapping ) {
            final CompoundMapping cm = (CompoundMapping) mapping;
            for ( final Mapping childMapping : cm.getParticles() ) {
                add( childMapping, currentTableAlias, fs );
            }
        }
    }

    private void addFromColumns( final List<TableJoin> joins, final String currentTableAlias ) {
        for ( final TableJoin join : joins ) {
            for ( final SQLIdentifier fromColumn : join.getFromColumns() ) {
                selectTerms.add( currentTableAlias + "." + fromColumn.getName() );
            }
        }
    }

    private void addToColumns( TableJoin join, final String currentTableAlias ) {
        for ( final SQLIdentifier toColumn : join.getToColumns() ) {
            selectTerms.add( currentTableAlias + "." + toColumn.getName() );
        }
    }

    private String followJoins( final List<TableJoin> joins, String currentTableAlias ) {
        for ( final TableJoin join : joins ) {
            currentTableAlias = followJoin( join, currentTableAlias );
        }
        return currentTableAlias;
    }

    private String followJoin( final TableJoin join, final String currentTableAlias ) {
        final String nextTableAlias = aliasManager.generateNew();
        joinToTableAlias.put( join, nextTableAlias );
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
        // key columns need to be selected to allow deduplication
        for ( final SQLIdentifier keyColumn : join.getKeyColumnToGenerator().keySet() ) {
            selectTerms.add( nextTableAlias + '.' + keyColumn.getName() );
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
