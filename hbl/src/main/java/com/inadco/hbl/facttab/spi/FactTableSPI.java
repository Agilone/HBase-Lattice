package com.inadco.hbl.facttab.spi;

import com.inadco.hbl.client.AggregateResultSet;
import com.inadco.hbl.client.HblException;

/**
 * Interface for a 3rd party Fact Table Service Provider.
 * 
 * @author dmitriy
 *
 */
public interface FactTableSPI {

    /**
     * API to execute query over a 3rd party system containing complete fact
     * table when/if suitable cuboid is not available
     * 
     * @param query
     * @throws HblException
     * 
     */
    AggregateResultSet executeQuery(AggregateQuerySPI query) throws HblException;

}
