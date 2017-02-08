package com.bridgelabz;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CompleteAnalysis {

	public static void main(String[] args) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		DynamoDB dynamoDB = new DynamoDB(client);
		String tableName = "AdditionData";

		int choice;

		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);

		while (true) {
			System.out.println("");
			System.out.println("Understanding the various functions of DynamoDB");
			System.out.println("1.  Create a Table in DynamoDB");
			System.out.println("2.  Load data from a JSON file in the Table");
			System.out.println("3.  Add items in the Table");
			System.out.println("4.  Read items from the Table");
			System.out.println("5.  Update items in the Table");
			System.out.println("6.  Delete items from the Table");
			System.out.println("7.  Delete Table from the DynamoDB");
			System.out.println("8.  Query DynamoDB");
			System.out.println("9.  Scan DynamoDB");
			System.out.println("10. Quit the Program");
			System.out.print("Enter Your Choice: ");
			choice = scanner.nextInt();

			switch (choice) {
			case 1:
				createTable(dynamoDB, tableName);
				break;

			case 2:
				loadDataInTable(dynamoDB, tableName);
				break;

			case 3:
				addItemsInTable(dynamoDB, tableName);
				break;

			case 4:
				readItemFromTable(dynamoDB, tableName);
				break;

			case 5:
				updateItemsInTable(dynamoDB, tableName);
				break;

			case 6:
				deleteItemsFromTable();
				break;

			case 7:
				deleteTable();
				break;

			case 8:
				queryDatabase();
				break;

			case 9:
				scanDatabase();
				break;

			case 10:
				System.out.println("Program Terminated");
				return;

			default:
				System.out.println("Invalid choice");
				break;

			}// end of switch case

		} // end of while loop

	}// end of main method

	private static void createTable(DynamoDB dynamoDB, String tableName) {

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

	private static void loadDataInTable(DynamoDB dynamoDB, String tableName) {

		Table table = dynamoDB.getTable(tableName);
		JsonParser parser = null;
		try {
			parser = new JsonFactory().createParser(new File("/home/bridgeit/Downloads/AddData.json"));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		JsonNode rootNode = null;
		try {
			rootNode = new ObjectMapper().readTree(parser);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Iterator<JsonNode> iter = rootNode.iterator();

		ObjectNode currentNode;

		while (iter.hasNext()) {
			currentNode = (ObjectNode) iter.next();

			int uniqueId = currentNode.path("uniqueId").asInt();
			int number1 = currentNode.path("number1").asInt();
			int number2 = currentNode.path("number2").asInt();
			int result = number1 + number2;

			try {

				Item item = new Item().withPrimaryKey("uniqueId", uniqueId).withNumber("number1", number1)
						.withNumber("number2", number2).withNumber("result", result);

				table.putItem(item);
				System.out.println("PutItem succeeded: " + number1 + " " + number2 + " " + result);

			} catch (Exception e) {
				System.err.println("Unable to add result: " + number1 + " " + number2 + " " + result);
				System.err.println(e.getMessage());
				break;
			}
		}
		try {
			parser.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void addItemsInTable(DynamoDB dynamoDB, String tableName) {

		Table table = dynamoDB.getTable(tableName);
		int uniqueId = 11;
		int number1 = 234;
		int number2 = 523;
		int result = number1 + number2;

		try {
			Item item = new Item().withPrimaryKey("uniqueId", uniqueId).withNumber("number1", number1)
					.withNumber("number2", number2).withNumber("result", result);

			table.putItem(item);
			System.out.println("PutItem succeeded: " + number1 + " " + number2 + " " + result);

		} catch (Exception e) {
			System.err.println("Unable to add result: " + number1 + " " + number2 + " " + result);
			System.err.println(e.getMessage());
		}
	}

	private static void readItemFromTable(DynamoDB dynamoDB, String tableName) {

		Table table = dynamoDB.getTable(tableName);
		int uniqueId = 11;
		GetItemSpec spec = new GetItemSpec().withPrimaryKey("uniqueId", uniqueId);

		try {
			System.out.println("Attempting to read the item...");
			Item outcome = table.getItem(spec);
			System.out.println("GetItem succeeded: " + outcome);

		} catch (Exception e) {
			System.err.println("Unable to read the item: " + uniqueId);
			System.err.println(e.getMessage());
		}

	}

	private static void updateItemsInTable(DynamoDB dynamoDB, String tableName) {
		
		Table table = dynamoDB.getTable(tableName);
		int uniqueId = 11;

		UpdateItemSpec updateItemSpec = new UpdateItemSpec()
				.withPrimaryKey("uniqueId", uniqueId)
				.withNumber(":res", 610)
				.withReturnValues(ReturnValue.UPDATED_NEW);

		try {
			System.out.println("Updating the item...");
			UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
			System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

		} catch (Exception e) {
			System.err.println("Unable to read the item: " + uniqueId);
			System.err.println(e.getMessage());
		}

	}

	private static void deleteItemsFromTable() {
		// TODO Auto-generated method stub

	}

	private static void deleteTable() {
		// TODO Auto-generated method stub

	}

	private static void queryDatabase() {
		// TODO Auto-generated method stub

	}

	private static void scanDatabase() {
		// TODO Auto-generated method stub

	}

}// end of class
