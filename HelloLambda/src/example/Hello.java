package example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Hello implements RequestHandler<Model, String> {
	Database database = new Database();
	Statement statement;
	ResultSet resultSet;

	@Override
	public String handleRequest(Model input, Context context) {

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection(
					"jdbc:mysql://testrds1.cmgtmjfill7k.us-west-2.rds.amazonaws.com/TestRds1", "TestRds1", "TestRds1");
			context.getLogger().log("Test Started");
			String query = " insert into timepass (id,name)" + " values (?, ?)";
			Class.forName("com.mysql.jdbc.Driver");

			PreparedStatement preparedStmt = con.prepareStatement(query);

			preparedStmt.setString(1, input.getId());
			preparedStmt.setString(2, input.getName());
			preparedStmt.execute();
			System.out.println("Data inserted");
			context.getLogger().log("Data inserted successfully");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}
		return "hello " + input.getName() + "  inserted";
	}

	public String test(String input) {

		try {

			String query = "select * from timepass where id=" + input;
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection(
					"jdbc:mysql://testrds1.cmgtmjfill7k.us-west-2.rds.amazonaws.com/TestRds1", "TestRds1", "TestRds1");
			System.out.println("Test Started");
			Statement statement = con.createStatement();
			resultSet = statement.executeQuery(query);
			if (resultSet.next()) {
				System.out.println(resultSet.getString("id") + "====" + resultSet.getString("name") + "======");
				return resultSet.getString("name");
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String args[]) {
		Hello hello = new Hello();
		System.out.println("database connection Established");
		String id = "27";
		String abc = hello.test(id);
		System.out.println(abc + "----> found at " + id);
		hello.insert();
		System.out.println("Successfully inserted");
		hello.delete();
		System.out.println("Deleted Successfully");
	}

	public void insert() {
		try {

			String query = " insert into timepass (id,name)" + " values (?, ?)";
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection(
					"jdbc:mysql://testrds1.cmgtmjfill7k.us-west-2.rds.amazonaws.com/TestRds1", "TestRds1", "TestRds1");
			PreparedStatement preparedStmt = con.prepareStatement(query);
			preparedStmt.setString(1, "151");
			preparedStmt.setString(2, "Lea Seydoux");
			preparedStmt.execute();
			System.out.println("Data inserted");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void delete() {
		try {

			String query = "delete from timepass where id = ?";
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection(
					"jdbc:mysql://testrds1.cmgtmjfill7k.us-west-2.rds.amazonaws.com/TestRds1", "TestRds1", "TestRds1");
			PreparedStatement preparedStmt = con.prepareStatement(query);
			preparedStmt.setString(1, "115");
			preparedStmt.execute();
			System.out.println("Data entry deleted");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}