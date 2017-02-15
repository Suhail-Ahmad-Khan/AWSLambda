package example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;

public class DDBEventProcessor implements RequestHandler<DynamodbEvent, String> {

	@Override
	public String handleRequest(DynamodbEvent ddbEvent, Context context) {
		
		for (DynamodbStreamRecord record : ddbEvent.getRecords()) {
			System.out.println(record.getEventID());
			System.out.println(record.getEventName());
			System.out.println(record.getDynamodb().toString());

		}
		return "Successfully processed " + ddbEvent.getRecords().size() + " records.";
	}
}
