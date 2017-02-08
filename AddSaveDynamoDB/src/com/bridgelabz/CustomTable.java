package com.bridgelabz;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CustomTable {

	public static void main(String[] args) throws Exception {

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();

		DynamoDB dynamoDB = new DynamoDB(client);

		String tableName = "AdditionData";

		// CREATING TABLE IN DYNAMODB
		/*
		 * try {
		 * System.out.println("Attempting to create table; please wait...");
		 * Table table = dynamoDB.createTable(tableName, Arrays.asList(new
		 * KeySchemaElement("uniqueId", KeyType.HASH)), // Partition // key
		 * Arrays.asList(new AttributeDefinition("uniqueId",
		 * ScalarAttributeType.N)), new ProvisionedThroughput(10L, 10L));
		 * table.waitForActive(); System.out.println("Success.  Table status: "
		 * + table.getDescription().getTableStatus());
		 * 
		 * } catch (Exception e) {
		 * System.err.println("Unable to create table: ");
		 * System.err.println(e.getMessage()); }
		 */

		// LOADING DATA IN DYNAMODB

		Table table = dynamoDB.getTable("AdditionData");
		JsonParser parser = new JsonFactory().createParser(new File("/home/bridgeit/Downloads/AddData.json"));

		JsonNode rootNode = new ObjectMapper().readTree(parser);
		Iterator<JsonNode> iter = rootNode.iterator();

		ObjectNode currentNode;

		while (iter.hasNext()) {
			currentNode = (ObjectNode) iter.next();

			int uniqueId = currentNode.path("uniqueId").asInt();
			int number1 = currentNode.path("number1").asInt();
			int number2 = currentNode.path("number2").asInt();
			int result = number1 + number2;

			try {

				Item item = new Item()
						.withPrimaryKey("uniqueId", uniqueId)
						.withNumber("number1", number1)
						.withNumber("number2", number2)
						.withNumber("result", result);

				table.putItem(item);
				System.out.println("PutItem succeeded: " + number1 + " " + number2 + " " + result);

			} catch (Exception e) {
				System.err.println("Unable to add result: " + number1 + " " + number2 + " " + result);
				System.err.println(e.getMessage());
				break;
			}
		}
		parser.close();

	}

}
