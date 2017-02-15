package com.bridgelabz;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

public class LambdaFunctionHandler implements RequestHandler<DynamodbEvent, Object> {

	@Override
	public Object handleRequest(DynamodbEvent input, Context context) {
		context.getLogger().log("Input: " + input);

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		DynamoDB dynamoDB = new DynamoDB(client);
		String tableName = "AddNumbers";

		Table table = dynamoDB.getTable(tableName);
		Model model = new Model();

		int uniqueId = model.getUniqueId();
		int number1 = model.getNumber1();
		int number2 = model.getNumber2();
		int result = number1 + number2;

		Item item = new Item().withPrimaryKey("uniqueId", uniqueId).withNumber("number1", number1)
				.withNumber("number2", number2).withNumber("result1", result);

		table.putItem(item);

		return true;
	}

}
