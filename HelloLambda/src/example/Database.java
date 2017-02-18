package example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.activation.DataSource;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;



public class Database {
	private static Database databaseConnection = new Database();
	private Connection connection;
	ResultSet resultSet;
	// Database connection to mysql using connection pull
	public String database(String input){
		try {
			
			/*Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection(
					"jdbc:mysql://employeenew.cl3m2q8sxt0y.us-west-2.rds.amazonaws.com/employeenew", "employeenew",
					"employeenew");
			*/
			MysqlDataSource ds = new MysqlDataSource();
			ds.setURL("jdbc:mysql://employeenew.cl3m2q8sxt0y.us-west-2.rds.amazonaws.com/employeenew");
			ds.setUser("employeenew");
			ds.setPassword("employeenew");
			
			Connection con = ds.getConnection();
			
			// here sonoo is database name, root is username and password
			
			String query = "select * from employeenew where id=" + input;
			Statement statement = con.createStatement();
			resultSet = statement.executeQuery(query);
			if (resultSet.next()) {
				// System.out.println(resultSet.getString("id")+"===="+resultSet.getString("name")+"======");
				//context.getLogger().log(resultSet.getString("id") + "====" + resultSet.getString("name") + "======");
				return resultSet.getString("name");
			}
		} 
		catch (SQLException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null;
	}

	public static Database getInstance() {
		return databaseConnection;
	}

	public Connection getConnection() {
		return connection;
	}

	public void closeConnection() throws SQLException {
		System.out.println("Database Connection Closed");
		this.connection.close();
	}

}


