package com.bridgelabz;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class LambdaFunctionHandler implements RequestHandler<Model, Object> {

	@Override
	public Object handleRequest(Model input, Context context) {
		context.getLogger().log("Input: " + input);

		final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		DynamoDB dynamoDB = new DynamoDB(client);
		String tableName = "Model";

		Table table = dynamoDB.getTable(tableName);

		int uniqueId = input.getUniqueId();
		int number1 = input.getNumber1();
		int number2 = input.getNumber2();
		int result = number1 + number2;

		final Item item = new Item()
				.withPrimaryKey("uniqueId", uniqueId)
				.withNumber("number1", number1)
				.withNumber("number2", number2)
				.withNumber("result", result);

		table.putItem(item);

		// TODO: implement your handler
		return null;
	}

}
