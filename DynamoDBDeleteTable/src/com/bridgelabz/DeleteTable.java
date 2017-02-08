package com.bridgelabz;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.regions.Regions;

public class DeleteTable {

	public static void main(String[] args) throws Exception {

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		DynamoDB dynamoDB = new DynamoDB(client);

		Table table = dynamoDB.getTable("Movies");

		try {
			System.out.println("Attempting to delete table; please wait...");
			table.delete();
			table.waitForDelete();
			System.out.print("Success.");

		} catch (Exception e) {
			System.err.println("Unable to delete table: ");
			System.err.println(e.getMessage());
		}
	}

}
