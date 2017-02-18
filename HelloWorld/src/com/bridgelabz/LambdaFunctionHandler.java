package com.bridgelabz;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class LambdaFunctionHandler implements RequestHandler<String, String> {

	@Override
	public String handleRequest(String input, Context context) {
		context.getLogger().log("Input: " + input);

		String output = input + " Welcome To Bridgelabz";

		// TODO: implement your handler
		return output;
	}

}
