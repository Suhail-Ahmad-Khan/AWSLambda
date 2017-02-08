package com.bridgelabz;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class LambdaFunctionHandler implements RequestHandler<Model, Integer> {

    @Override
    public Integer handleRequest(Model input, Context context) {
        context.getLogger().log("Input: " + input);
        
        int result = input.getNumber1() + input.getNumber2();

        // TODO: implement your handler
        return result;
    }

}
