package com.bridgelabz;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.regions.Regions;

public class CRUDExample {
	
	public static void main(String[] args) throws Exception {

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();

		DynamoDB dynamoDB = new DynamoDB(client);

		Table table = dynamoDB.getTable("Movies");

		int year = 2015;
		String title = "The Big New Movie";

		// ADDING AN ITEM INTO THE LIST
		/*
		 * final Map<String, Object> infoMap = new HashMap<String, Object>();
		 * infoMap.put("plot", "Nothing happens at all."); infoMap.put("rating",
		 * 0);
		 * 
		 * try { System.out.println("Adding a new item..."); PutItemOutcome
		 * outcome = table .putItem(new Item().withPrimaryKey("year", year,
		 * "title", title).withMap("info", infoMap));
		 * 
		 * System.out.println("PutItem succeeded:\n" +
		 * outcome.getPutItemResult());
		 * 
		 * } catch (Exception e) { System.err.println("Unable to add item: " +
		 * year + " " + title); System.err.println(e.getMessage()); }
		 */

		// READING AN ITEM FROM THE LIST
		/*
		 * GetItemSpec spec = new GetItemSpec().withPrimaryKey("year", year,
		 * "title", title);
		 * 
		 * try { System.out.println("Attempting to read the item..."); Item
		 * outcome = table.getItem(spec);
		 * System.out.println("GetItem succeeded: " + outcome);
		 * 
		 * } catch (Exception e) { System.err.println("Unable to read item: " +
		 * year + " " + title); System.err.println(e.getMessage()); }
		 */

		// UPDATING DATA IN THE DATABASE
		/*
		 * UpdateItemSpec updateItemSpec = new UpdateItemSpec()
		 * .withPrimaryKey("year", year, "title", title)
		 * .withUpdateExpression("set info.rating = :r, info.plot=:p, info.actors=:a"
		 * ) .withValueMap(new ValueMap() .withNumber(":r", 5.5)
		 * .withString(":p", "Everything happens all at once.") .withList(":a",
		 * Arrays.asList("Larry","Moe","Curly")))
		 * .withReturnValues(ReturnValue.UPDATED_NEW);
		 * 
		 * try { System.out.println("Updating the item..."); UpdateItemOutcome
		 * outcome = table.updateItem(updateItemSpec);
		 * System.out.println("UpdateItem succeeded:\n" +
		 * outcome.getItem().toJSONPretty());
		 * 
		 * } catch (Exception e) { System.err.println("Unable to update item: "
		 * + year + " " + title); System.err.println(e.getMessage()); }
		 */

		// DELETING AN ITEM FROM DYNAMODB
		DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
				.withPrimaryKey(new PrimaryKey("year", 2015, "title", "The Big New Movie"));

		// Conditional delete (we expect this to fail)

		try {
			System.out.println("Attempting a conditional delete...");
			table.deleteItem(deleteItemSpec);
			System.out.println("DeleteItem succeeded");
		} catch (Exception e) {
			System.err.println("Unable to delete item: " + year + " " + title);
			System.err.println(e.getMessage());
		}

	}
}
