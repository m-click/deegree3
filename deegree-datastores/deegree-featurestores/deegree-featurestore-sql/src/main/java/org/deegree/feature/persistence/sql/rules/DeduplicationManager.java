package org.deegree.feature.persistence.sql.rules;

import java.util.HashSet;
import java.util.Set;

/**
 * Responsible for tracking mappings that have been deduplicated.
 * <p>
 * The need for deduplication arises from join tables that are fetched via JOIN.
 * </p>
 * 
 * @author <a href="mailto:schneider@occamlabs.de">Markus Schneider</a>
 *
 * @since 3.4
 */
class DeduplicationManager {

    private final Set<Mapping> deduplicatedMappings = new HashSet<Mapping>();

    void setDeduplicated( final Mapping mapping ) {
        deduplicatedMappings.add( mapping );
    }

    boolean isDeduplicated( final Mapping mapping ) {
        return deduplicatedMappings.contains( mapping );
    }

}
