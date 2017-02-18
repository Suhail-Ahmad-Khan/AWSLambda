package com.bridgelabz;

import java.util.Arrays;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class LambdaFunctionHandler implements RequestHandler<Model, String> {

	@Override
	public String handleRequest(Model input, Context context) {
		context.getLogger().log("Input: " + input);

		// TODO: implement your handler
		return "Data Inserted Successfully";
	}

	public void createTable(DynamoDB dynamoDB, String tableName) {

		try {
			System.out.println("Attempting to create table; please wait...");
			Table table = dynamoDB.createTable(tableName, Arrays.asList(new KeySchemaElement("uniqueId", KeyType.HASH)), // Partition
																															// key
					Arrays.asList(new AttributeDefinition("uniqueId", ScalarAttributeType.N)),
					new ProvisionedThroughput(10L, 10L));
			table.waitForActive();
			System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

		} catch (Exception e) {
			System.err.println("Unable to create table: ");
			System.err.println(e.getMessage());
		}

	}

	public void addItemsInTable(DynamoDB dynamoDB, String tableName) {

		Table table = dynamoDB.getTable(tableName);
		int uniqueId = 11;
		int number1 = 234;
		int number2 = 523;
		int result1 = number1 + number2;

		try {
			Item item = new Item().withPrimaryKey("uniqueId", uniqueId).withNumber("number1", number1)
					.withNumber("number2", number2).withNumber("result1", result1);

			table.putItem(item);
			System.out.println("PutItem succeeded: " + number1 + " " + number2 + " " + result1);

		} catch (Exception e) {
			System.err.println("Unable to add result: " + number1 + " " + number2 + " " + result1);
			System.err.println(e.getMessage());
		}
	}

	public static void main(String[] args) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		DynamoDB dynamoDB = new DynamoDB(client);
		String tableName = "Model";

		LambdaFunctionHandler lambdaFunctionHandler = new LambdaFunctionHandler();
		lambdaFunctionHandler.createTable(dynamoDB, tableName);
		lambdaFunctionHandler.addItemsInTable(dynamoDB, tableName);

	}

}
