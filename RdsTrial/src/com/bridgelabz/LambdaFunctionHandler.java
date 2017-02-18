package com.bridgelabz;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.bridgelabz.LambdaFunctionHandler;

public class LambdaFunctionHandler implements RequestHandler<Model, String> {

	Statement statement;
	ResultSet resultSet;

	@Override
	public String handleRequest(Model input, Context context) {

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = DriverManager.getConnection(
					"jdbc:mysql://rdstrialtest.cmgtmjfill7k.us-west-2.rds.amazonaws.com/AddingIntegers", "rdstrialtest",
					"rdstrialtest");
			context.getLogger().log("Test Started");

			int uniqueId = input.getUniqueId();
			int number1 = input.getNumber1();
			int number2 = input.getNumber2();
			int result = number1 + number2;

			String query = " insert into myDatabase (uniqueId, number1, number2, result)" + " values (?, ?, ?, ?)";
			Class.forName("com.mysql.jdbc.Driver");

			PreparedStatement preparedStmt = connection.prepareStatement(query);

			preparedStmt.setInt(1, uniqueId);
			preparedStmt.setInt(2, number1);
			preparedStmt.setInt(3, number2);
			preparedStmt.setInt(4, result);
			preparedStmt.execute();
			System.out.println("Data inserted");
			context.getLogger().log("Data inserted successfully");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}
		return "Number inserted";
	}
}
