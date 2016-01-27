
package com.snapdeal.redap.dao;

import org.apache.log4j.Logger;
import java.util.ArrayList;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.snapdeal.redap.base.ErrorClass;

/*
 * Make sure you read this before you try any changes in the code whatsoever
 * 
 * 
 * This DAO is the class that will handle requests to database access. 
 * The DAO calls a Lua udf function that takes as input 3 things.
 * 
 * 1. Filters 		: This applies whatever filters that is required to be imposed on the records.
 * 2. Projections 	: This fields specifies the fields which the end user wishes to see.
 * 3. GroupBy 		: This field specifies the bin on which the groupBy operation is to be performed.
 * 
 * The UDF performs these actions and returns a ResultSet. Each record in the resultset is scanned and
 * cast into an Object type for jackson to convert into json. For some reason, jackson isn't able to convert
 * resultsets into json, hence this step had to be taken.
 * 
 * 
 * Format set for the above three inputs
 * 1. Filters 		: filter_bin1-operation-value_1-filter_bin2-operation-value_2 where
 * 				  	  operations can be one of {eq,leq,geq,g,l}
 * 					EXAMPLE : COURIERNAME|eq|BLUEDART|AVERAGE|g|10
 * 
 * 2. projections	: projections_1|projections_2
 * 					EXAMPLE : COURIERNAME-AVERAGE
 * 
 * 3. groupBy		: groupBy_field|[TIME]
 * 
 * 					EXAMPLE : COURIERNAME      or    TIMESTAMP-86400
 * 					The first example groups the resultset according to the CourierName and the second example groups
 * 					the resultset according to 86400 seconds (i.e 1 day).  
 * 					     	
 */
/**
 * @author Justine Raju Thomas
 *
 */
public class AverageDAO
{
	AerospikeClient	client;
	Logger log = Logger.getLogger(AverageDAO.class);
	
	/*
	 * This following function is where we read value from Aerospike server.
	 * 
	 */
	public ArrayList<Object> getAvgOnDates(AerospikeClient client, String namespaceName, String setName, String startKey,
			String endKey, String groupBy, String filters, String projections)
	{

		log.info("API call has reached the DAO");
		ArrayList<Object> al = new ArrayList<>();
		ResultSet rs = null;
		Statement stmt = new Statement();
		try
		{
			stmt.setNamespace(namespaceName);
			stmt.setSetName(setName);
			stmt.setFilters(
					Filter.range("TIMESTAMP", Long.parseLong(startKey),
							(Long.parseLong(endKey) + Long.valueOf(86400))));
			log.debug("Calling Aggregate Function");
			rs = client.queryAggregate(null, stmt, "genericUdf", "get_results",
					Value.get(filters), Value.get(groupBy),
					Value.get(projections));
			log.debug("Printing the results");
			while (rs.next())
			{
				Object object = rs.getObject();
				al.add(object);
		//		log.debug("Result: " + object);
			}
			//log.info("Returning from DAO with value" + al.toString());
		}
		catch (Throwable e)
		{
			ErrorClass err = new ErrorClass();
			err.status = "Failure";
			err.errorMessage = e.getMessage();
			Object object = (Object) err;
			al.add(object);
			log.error("Aerospike Error Occured : " + e.toString());
		}
		return al;
	}
}
