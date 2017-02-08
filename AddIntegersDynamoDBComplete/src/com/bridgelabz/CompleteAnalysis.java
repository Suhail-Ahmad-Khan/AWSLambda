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
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
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
			System.out.println("8.  Scan DynamoDB");
			System.out.println("9.  Quit the Program");
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
				deleteItemsFromTable(dynamoDB, tableName);
				break;

			case 7:
				deleteTable(dynamoDB, tableName);
				break;

			case 8:
				scanDatabase(dynamoDB, tableName);
				break;

			case 9:
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
			int result1 = number1 + number2;

			try {

				Item item = new Item().withPrimaryKey("uniqueId", uniqueId).withNumber("number1", number1)
						.withNumber("number2", number2).withNumber("result1", result1);

				table.putItem(item);
				System.out.println("PutItem succeeded: " + number1 + " " + number2 + " " + result1);

			} catch (Exception e) {
				System.err.println("Unable to add result: " + number1 + " " + number2 + " " + result1);
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

		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("uniqueId", uniqueId)
				.withUpdateExpression("set number1 = :num1, number2 = :num2, result1 = :res")
				.withValueMap(new ValueMap().withNumber(":num1", 55).withNumber(":num2", 55).withNumber(":res", 110))
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

	private static void deleteItemsFromTable(DynamoDB dynamoDB, String tableName) {

		Table table = dynamoDB.getTable(tableName);

		int uniqueId = 0;

		DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(new PrimaryKey("uniqueId", 11));

		try {
			System.out.println("Attempting a conditional delete...");
			table.deleteItem(deleteItemSpec);
			System.out.println("DeleteItem succeeded");
		} catch (Exception e) {
			System.err.println("Unable to delete item: " + uniqueId);
			System.err.println(e.getMessage());
		}

	}

	private static void deleteTable(DynamoDB dynamoDB, String tableName) {

		Table table = dynamoDB.getTable(tableName);

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

	private static void scanDatabase(DynamoDB dynamoDB, String tableName) {

		Table table = dynamoDB.getTable(tableName);

		ScanSpec scanSpec = new ScanSpec().withProjectionExpression("#uniqueId, number1, number2, result1")
				.withFilterExpression("#uniqueId between :start and :end")
				.withNameMap(new NameMap().with("#uniqueId", "uniqueId"))
				.withValueMap(new ValueMap().withNumber(":start", 3).withNumber(":end", 8));

		try {
			ItemCollection<ScanOutcome> items = table.scan(scanSpec);

			Iterator<Item> iter = items.iterator();
			while (iter.hasNext()) {
				Item item = iter.next();
				System.out.println(item.toString());
			}

		} catch (Exception e) {
			System.err.println("Unable to scan the table:");
			System.err.println(e.getMessage());
		}
	}

}// end of class
