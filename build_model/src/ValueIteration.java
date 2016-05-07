import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.Connection;


public class ValueIteration
{
	//Configuration variables
	private static String dbaddress = "mysql://localhost";
	private static String dbname = "nhl_final_sequence_tree_no_manpower";
	private static String dbname2 = "NHL_Final_No_Manpower_Value_Iteration";
	private static double convergence_criteria = 0.000001;
	private static int max_number_of_iterations = 200;
	
	//Internal variables
	private static Connection con = null;
	private static double[] last_convergence_value = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
	private static double[] current_convergence_value = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
	private static String[] table_names = { "Expected_Goals",
											"Probability_Next_Home_Goal",
											"Probability_Next_Away_Goal",
											"Expected_Wins",
											"Probability_Home_Win",
											"Probability_Away_Win",
											"Expected_Penalties",
											"Probability_Next_Home_Penalty",
											"Probability_Next_Away_Penalty" };
	
	public static void main( String[] args )
	{
		//Start timer
		long start = System.currentTimeMillis();
		
		//Connect to database
		if ( 0 != connectDB() )
		{
			System.out.println( "Failed to connect to database." );
			return;
		}
		long t1 = System.currentTimeMillis();
		System.out.println( "Time to connect to database: " + ( ( t1 - start ) / 1000.0 ) + "s" );
		System.out.println( "Cumulative running time: " + ( ( t1 - start ) / 1000.0 ) + "s" );
		
		//Create new database
		if ( 0 != createNewDatabase() )
		{
			System.out.println( "Failed to create new database." );
			return;
		}
		
		long t2 = System.currentTimeMillis();
		System.out.println( "Time to create new database: " + ( ( t2 - t1 ) / 1000.0 ) + "s" );
		System.out.println( "Cumulative running time: " + ( ( t2 - start ) / 1000.0 ) + "s" );
		
		//Run value iteration
		if ( 0 != runValueIteration( start ) )
		{
			System.out.println( "Value iteration failed." );
			return;
		}
		
		long t3 = System.currentTimeMillis();
		System.out.println( "Time to run value iteration: " + ( ( t3 - t2 ) / 1000.0 ) + "s" );
		System.out.println( "Cumulative running time: " + ( ( t3 - start ) / 1000.0 ) + "s" );
		
		
		//Disconnect from database
		if ( 0 != disconnectDB() )
		{
			System.out.println( "Failed to disconnect from database." );
			return;
		}

		//End timer
		long end = System.currentTimeMillis();
		
		System.out.println( "Total runtime: " + ( ( end - start ) / 1000.0 ) + "s" );
	}
	
	private static int connectDB()
	{
		String CONN_STR = "jdbc:" + dbaddress + "/";
		try 
		{
			java.lang.Class.forName( "com.mysql.jdbc.Driver" );
		} 
		catch ( Exception e ) 
		{
			System.out.println( "Unable to load MySQL JDBC driver" );
			e.printStackTrace();
			return -1;
		}
		
		try
		{
			con = (Connection) DriverManager.getConnection( CONN_STR, 
															"root", 
															"root" );
		}
		catch (SQLException e)
		{
			System.out.println( "Failed to connect to database." );
			e.printStackTrace();
			return -2;
		}
		return 0;
	}
	
	private static int createNewDatabase()
	{
		try
		{
			Statement st1 = con.createStatement();
			
			System.out.println( "DROP SCHEMA IF EXISTS " + dbname2 + ";" );
			st1.execute(  "DROP SCHEMA IF EXISTS " + dbname2 + ";" );
			
			System.out.println( "CREATE SCHEMA " + dbname2 + ";" );
			st1.execute( "CREATE SCHEMA " + dbname2 + ";" );
			
			System.out.println( "USE " + dbname2 + ";" );
			st1.execute( "USE " + dbname2 + ";" );
			
			//Table creation
			//Nodes
			System.out.println( "CREATE TABLE Nodes AS SELECT * FROM " + dbname + ".Nodes;" );
			st1.execute( "CREATE TABLE Nodes AS SELECT * FROM " + dbname + ".Nodes;" );
			
			System.out.println( "ALTER TABLE Nodes ADD COLUMN Reward_Expected_Penalties INT DEFAULT 0, "
					+ "ADD COLUMN Reward_Probability_Next_Home_Penalty INT DEFAULT 0, "
					+ "ADD COLUMN Reward_Probability_Next_Away_Penalty INT DEFAULT 0;" );
			st1.execute( "ALTER TABLE Nodes ADD COLUMN Reward_Expected_Penalties INT DEFAULT 0, "
					+ "ADD COLUMN Reward_Probability_Next_Home_Penalty INT DEFAULT 0, "
					+ "ADD COLUMN Reward_Probability_Next_Away_Penalty INT DEFAULT 0;" );
			
			System.out.println( "ALTER TABLE Nodes ADD INDEX ( NodeId, Occurrences, Reward_Expected_Goals, Reward_Expected_Win, Reward_Expected_Penalties );" );
			st1.execute( "ALTER TABLE Nodes ADD INDEX ( NodeId, Occurrences, Reward_Expected_Goals, Reward_Expected_Win, Reward_Expected_Penalties );" );
			
			System.out.println( "UPDATE Nodes SET Reward_Expected_Penalties = 1, " + 
								"Reward_Probability_Next_Home_Penalty = 1 " +
								"WHERE NodeName LIKE 'HOME:PENALTY';" );
			st1.execute( "UPDATE Nodes SET Reward_Expected_Penalties = 1, " + 
					"Reward_Probability_Next_Home_Penalty = 1 " +
					"WHERE NodeName LIKE 'HOME:PENALTY';" );
			
			System.out.println( "UPDATE Nodes SET Reward_Expected_Penalties = -1, " + 
					"Reward_Probability_Next_Away_Penalty = 1 " +
					"WHERE NodeName LIKE 'AWAY:PENALTY';" );
			st1.execute( "UPDATE Nodes SET Reward_Expected_Penalties = -1, " + 
					"Reward_Probability_Next_Away_Penalty = 1 " +
					"WHERE NodeName LIKE 'AWAY:PENALTY';" );
			
			System.out.println( "ALTER TABLE Nodes ADD INDEX ( NodeId );" );
			st1.execute( "ALTER TABLE Nodes ADD INDEX ( NodeId );" );
			
			System.out.println( "ALTER TABLE Nodes ADD INDEX ( NodeId, Occurrences );" );
			st1.execute( "ALTER TABLE Nodes ADD INDEX ( NodeId, Occurrences );" );
			
			//Edges
			System.out.println( "CREATE TABLE Edges AS SELECT * FROM " + dbname + ".Edges;" );
			st1.execute( "CREATE TABLE Edges AS SELECT * FROM " + dbname + ".Edges;" );
			
			System.out.println( "ALTER TABLE Edges ADD INDEX ( FromNodeId, ToNodeId, Occurrences );" );
			st1.execute( "ALTER TABLE Edges ADD INDEX ( FromNodeId, ToNodeId, Occurrences );" );
			
			//Expected_Goals
			if ( 0 != createIterationTable( "Expected_Goals" ) )
			{
				System.out.println( "Failed to create table Expected_Goals" );
				return -2;
			}
			
			//Probability_Next_Home_Goal
			if ( 0 != createIterationTable( "Probability_Next_Home_Goal" ) )
			{
				System.out.println( "Failed to create table Probability_Next_Home_Goal" );
				return -3;
			}
			
			//Probability_Next_Away_Goal
			if ( 0 != createIterationTable( "Probability_Next_Away_Goal" ) )
			{
				System.out.println( "Failed to create table Probability_Next_Away_Goal" );
				return -4;
			}
			
			//Expected_Wins
			if ( 0 != createIterationTable( "Expected_Wins" ) )
			{
				System.out.println( "Failed to create table Expected_Wins" );
				return -5;
			}
			
			//Probability_Home_Win
			if ( 0 != createIterationTable( "Probability_Home_Win" ) )
			{
				System.out.println( "Failed to create table Probability_Home_Win" );
				return -6;
			}
			
			//Probability_Away_Win
			if ( 0 != createIterationTable( "Probability_Away_Win" ) )
			{
				System.out.println( "Failed to create table Probability_Away_Win" );
				return -7;
			}
			
			//Expected_Penalties
			if ( 0 != createIterationTable( "Expected_Penalties" ) )
			{
				System.out.println( "Failed to create table Expected_Penalties" );
				return -8;
			}
			
			//Probability_Next_Home_Penalty
			if ( 0 != createIterationTable( "Probability_Next_Home_Penalty" ) )
			{
				System.out.println( "Failedto create table Probability_Next_Home_Penalty" );
				return -9;
			}
			
			//Probability_Next_Away_Penalty
			if ( 0 != createIterationTable( "Probability_Next_Away_Penalty" ) )
			{
				System.out.println( "Failedto create table Probability_Next_Away_Penalty" );
				return -10;
			}
			
			st1.close();
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			return -1;
		}
		
		return 0;
	}
	
	private static int createIterationTable( String name )
	{
		try
		{
			Statement st1 = con.createStatement();
			
			System.out.println( "USE " + dbname2 + ";" );
			st1.execute( "USE " + dbname2 + ";" );
			
			System.out.println( "CREATE TABLE " + name + " AS SELECT NodeId FROM Nodes;" );
			st1.execute( "CREATE TABLE " + name + " AS SELECT NodeId FROM Nodes;" );
			
			System.out.println( "ALTER TABLE " + name + " ADD COLUMN LastValue DOUBLE DEFAULT 0.0, ADD COLUMN CurrentValue DOUBLE DEFAULT 0.0;" );
			st1.execute( "ALTER TABLE " + name + " ADD COLUMN LastValue DOUBLE DEFAULT 0.0, ADD COLUMN CurrentValue DOUBLE DEFAULT 0.0;" );
			
			System.out.println( "ALTER TABLE " + name + " ADD INDEX ( NodeId, LastValue, CurrentValue );" );
			st1.execute( "ALTER TABLE " + name + " ADD INDEX ( NodeId, LastValue, CurrentValue );" );
			
			st1.close();
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	private static int runValueIteration( long startTime )
	{
		Statement st1 = null;
		ResultSet rs1 = null;
		
		long last = System.currentTimeMillis();
		long current = System.currentTimeMillis();
		
		try
		{
			st1 = con.createStatement();
			//System.out.println( "USE " + dbname2 + ";" );
			st1.execute( "USE " + dbname2 + ";" );
			
//			System.out.println( "SELECT NodeId, " + 
//								"NodeName, " + 
//								"Occurrences, " + 
//								"Reward_Expected_Goals, " +
//								"Reward_Expected_Win, " +
//								"Reward_Expected_Penalties " +
//								"FROM Nodes;" );
			rs1 = st1.executeQuery( "SELECT NodeId, " + 
					"NodeName, " + 
					"Occurrences, " + 
					"Reward_Expected_Goals, " +
					"Reward_Expected_Win, " +
					"Reward_Expected_Penalties " +
					"FROM Nodes;" );
			
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			System.out.println( "Failed to get node values." );
			return -1;
		}
		
		boolean converged = false;
		
		for ( int i = 1; i <= max_number_of_iterations; i++ )
		{
			try
			{
				while ( rs1.next() )
				{
					int nodeId = rs1.getInt( "NodeId" );
					int node_occurrences = rs1.getInt( "Occurrences" );
					double reward_expected_goals = rs1.getDouble( "Reward_Expected_Goals" );
					double reward_expected_wins = rs1.getDouble( "Reward_Expected_Win" );
					double reward_expected_penalties = rs1.getDouble( "Reward_Expected_Penalties" );
					
					//Expected_Goals
					if ( 0 != setNewValue( "Expected_Goals", 0, nodeId, reward_expected_goals, node_occurrences ) )
					{
						System.out.println( "Failed to set expected goals value." );
						return -4;
					}
					
					//Probability_Next_Home_Goal
					if ( 0 != setNewValue( "Probability_Next_Home_Goal", 1, nodeId, 0.0, node_occurrences ) )
					{
						System.out.println( "Failed to set Probability_Next_Home_Goal value." );
						return -5;
					}
					
					//Probability_Next_Away_Goal
					if ( 0 != setNewValue( "Probability_Next_Away_Goal", 2, nodeId, 0.0, node_occurrences ) )
					{
						System.out.println( "Failed to set Probability_Next_Away_Goal value." );
						return -6;
					}
					
					//Expected_Wins
					if ( 0 != setNewValue( "Expected_Wins", 3, nodeId, reward_expected_wins, node_occurrences ) )
					{
						System.out.println( "Failed to set expected wins value." );
						return -7;
					}
					
					//Probability_Home_Win
					if ( 0 != setNewValue( "Probability_Home_Win", 4, nodeId, 0.0, node_occurrences ) )
					{
						System.out.println( "Failed to set Probability_Home_Win value." );
						return -8;
					}
					
					//Probability_Away_Win
					if ( 0 != setNewValue( "Probability_Away_Win", 5, nodeId, 0.0, node_occurrences ) )
					{
						System.out.println( "Failed to set Probability_Away_Win value." );
						return -9;
					}
					
					//Expected_Penalties
					if ( 0 != setNewValue( "Expected_Penalties", 6, nodeId, reward_expected_penalties, node_occurrences ) )
					{
						System.out.println( "Failed to set Expected_Penalties value." );
						return -10;
					}
					
					//Probability_Next_Home_Penalty
					if ( 0 != setNewValue( "Probability_Next_Home_Penalty", 7, nodeId, 0.0, node_occurrences ) )
					{
						System.out.println( "Failed to set Probability_Next_Home_Penalty value." );
						return -11;
					}
					
					//Probability_Next_Away_Penalty
					if ( 0 != setNewValue( "Probability_Next_Away_Penalty", 8, nodeId, 0.0, node_occurrences ) )
					{
						System.out.println( "Failed to set Probability_Next_Away_Penalty value." );
						return -12;
					}
				}
				
				rs1.beforeFirst();
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
				System.out.println( "Failed to iteration over result set." );
				return -2;
			}
			
			for ( int j = 0; j < 9; j++ )
			{
				int ret = checkConvergence( table_names[ j ], j, i );
				
				if ( ret < 0 )
				{
					System.out.println( "Failed to calculate convergence." );
					return -4;
				}
				
				if ( ret == 1 )
				{
					converged = true;
				}
				
				if ( 0 != copyLastValue( table_names[j] ) )
				{
					System.out.println( "Failed to copy values." );
					return -5;
				}
			}
			
			current = System.currentTimeMillis();
			System.out.println( "Iteration " + i + " runtime: " + ( ( current - last ) / 1000.0 ) + "s" );
			System.out.println( "Cumulative running time: " + ( ( current - startTime ) / 1000.0 ) + "s" );
			
			last = current;
			
			if ( converged )
			{
				break;
			}
		}
		
		try
		{
			rs1.close();
			st1.close();
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			System.out.println( "Failed to close result set." );
			return -3;
		}
		
		return 0;
	}
	
	private static int setNewValue( String table_name, int table_index, int nodeId, double reward, int occurrences )
	{
		//Expected_X
		//Expected values are calculated using reward + sum( edge_count * child value / node_count )
		if ( table_name.contains( "Expected" ) )
		{
			try
			{
				Statement st1 = con.createStatement();
				
				st1.execute( "USE " + dbname2 + ";" );
				
//				System.out.println( "SELECT A.ToNodeId, A.Occurrences * B.LastValue AS Contribution FROM Edges AS A JOIN " + table_name + " AS B " +
//									"ON A.ToNodeId = B.NodeId WHERE A.FromNodeId = " + nodeId + ";" );
				ResultSet rs1 = st1.executeQuery( "SELECT A.ToNodeId, A.Occurrences * B.LastValue AS Contribution FROM Edges AS A JOIN " + table_name + " AS B " +
						"ON A.ToNodeId = B.NodeId WHERE A.FromNodeId = " + nodeId + ";" );
				
				double sum = 0.0;
				
				while ( rs1.next() )
				{
					sum += rs1.getDouble( "Contribution" );
				}
				
				rs1.close();
				
				double new_value = reward + ( sum / ( ( double ) occurrences ) );
				
				//System.out.println( "UPDATE " + table_name + " SET CurrentValue = " + new_value + " WHERE NodeId = " + nodeId + ";" );
				st1.execute( "UPDATE " + table_name + " SET CurrentValue = " + new_value + " WHERE NodeId = " + nodeId + ";" );
				
				current_convergence_value[ table_index ] += Math.abs( new_value );
				
				st1.close();
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
				System.out.println( "Failed to get children values." );
				return -1;
			}
			return 0;
		}
		
		//Probability_X
		//Probabilistic values are piecewise
		try
		{
			Statement st1 = con.createStatement();
			
			//System.out.println( "USE " + dbname2 + ";" );
			st1.execute( "USE " + dbname2 + ";" );
			
			String eventName = table_name.replace( "Probability_", "").replace("Next_", "").toUpperCase().replace("_", ":");
			
//			System.out.println( "SELECT A.ToNodeId, A.Occurrences * C.LastValue AS Contribution " + 
//								"FROM Edges AS A JOIN (Nodes AS B, " + table_name + " AS C ) " +
//								"ON (A.ToNodeId = C.NodeId AND A.ToNodeId = B.NodeId ) WHERE A.FromNodeId = " + nodeId + 
//								" AND B.NodeName NOT LIKE '" + eventName + "' UNION " +
//								"SELECT A.ToNodeId, 1 AS Contribution " + 
//								"FROM Edges AS A JOIN (Nodes AS B, " + table_name + " AS C ) " +
//								"ON (A.ToNodeId = C.NodeId AND A.ToNodeId = B.NodeId ) WHERE A.FromNodeId = " + nodeId + 
//								" AND B.NodeName LIKE '" + eventName + "';" );
			ResultSet rs1 = st1.executeQuery( "SELECT A.ToNodeId, A.Occurrences * C.LastValue AS Contribution " + 
											  "FROM Edges AS A JOIN (Nodes AS B, " + table_name + " AS C ) " +
											  "ON (A.ToNodeId = C.NodeId AND A.ToNodeId = B.NodeId ) WHERE A.FromNodeId = " + nodeId + 
											  " AND B.NodeName NOT LIKE '" + eventName + "' UNION " +
											  "SELECT A.ToNodeId, 1 AS Contribution " + 
											  "FROM Edges AS A JOIN (Nodes AS B, " + table_name + " AS C ) " +
											  "ON (A.ToNodeId = C.NodeId AND A.ToNodeId = B.NodeId ) WHERE A.FromNodeId = " + nodeId + 
											  " AND B.NodeName LIKE '" + eventName + "';" );
			
			double sum = 0.0;
			
			while ( rs1.next() )
			{
				sum += rs1.getDouble( "Contribution" );
			}
			
			rs1.close();
			
			double new_value = reward + ( sum / ( ( double ) occurrences ) );
			
			//System.out.println( "UPDATE " + table_name + " SET CurrentValue = " + new_value + " WHERE NodeId = " + nodeId + ";" );
			st1.execute( "UPDATE " + table_name + " SET CurrentValue = " + new_value + " WHERE NodeId = " + nodeId + ";" );
			
			st1.close();
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			System.out.println( "Failed to get probability children values." );
			return -2;
		}
		
		
		return 0;
	}
	
	private static int copyLastValue( String table_name )
	{
		try
		{
			Statement st1 = con.createStatement();
			
			System.out.println( "USE " + dbname2 + ";" );
			st1.execute( "USE " + dbname2 + ";" );
			
			System.out.println( "UPDATE " + table_name + " SET LastValue = CurrentValue;" );
			st1.execute( "UPDATE " + table_name + " SET LastValue = CurrentValue;" );
			
			st1.close();
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			System.out.println( "Failed to copy last value for table " + table_name );
			return -1;
		}
		
		return 0;
	}
	
	private static int checkConvergence( String table_name, int table_index, int iteration )
	{
		File f = new File( table_name + "_convergence.csv" );
		FileWriter fw = null;
		if ( 1 == iteration )
		{
			try
			{
				fw = new FileWriter( f, false );
				fw.write( "Iteration,Value,Convergence\n" );
				fw.close();
				return 0;
			}
			catch ( IOException e )
			{
				e.printStackTrace();
				System.out.println( "Couldn't create file " + table_name + "_convergence.csv" );
				return -1;
			}
		}
		else
		{
			try
			{
				fw = new FileWriter( f, true );
			}
			catch ( IOException e )
			{
				e.printStackTrace();
				System.out.println( "Couldn't open file " + table_name + "_convergence.csv" );
				return -2;
			}
		}
		
		double convergence_value = ( ( current_convergence_value[ table_index ] - last_convergence_value[ table_index ] ) / current_convergence_value[ table_index ] );
		
		try
		{
			fw.write( iteration + "," + current_convergence_value[ table_index ] + "," + convergence_value + "\n" );
			fw.flush();
			fw.close();
		}
		catch ( IOException e )
		{
			e.printStackTrace();
			System.out.println( "Failed to write convergence values to " + table_name + "_convergence.csv" );
			return -3;
		}
		
		last_convergence_value[ table_index ] = current_convergence_value[ table_index ];
		
		if ( convergence_value < convergence_criteria )
		{
			return 1;
		}
		
		return 0;
	}
	
	private static int disconnectDB()
	{
		try
		{
			con.close();
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			return -1;
		}
		
		return 0;
	}
}
