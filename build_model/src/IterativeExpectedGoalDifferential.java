import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.mysql.jdbc.Connection;


public class IterativeExpectedGoalDifferential
{
	public static String dbaddress = "mysql://127.0.0.1";
	private static Connection con;
	private static int max_num_iterations = 500;
	private static double discount = 1.0;
	private static double convergence = 0.0001;
	private static boolean expected_goals_converged = false;
	private static boolean probability_home_goal_converged = false;
	private static boolean probability_away_goal_converged = false;
	private static boolean expected_win_converged = false;
	private static boolean probability_home_win_converged = false;
	private static boolean probability_away_win_converged = false;
	private static File f = null;
	private static FileWriter fw = null;
	
	public static void main(String[] args)
	{
		long first = System.currentTimeMillis();
		
		if ( 0 != connectDB() )
		{
			System.out.println( "Failed to connect to database." );
			return;
		}
		
		long t1 = System.currentTimeMillis();
		System.out.println( "Time to connect: " + ( ( t1 - first ) / 1000.0 ) + "s" );
		
		if ( 0 != createWorkingSchema() )
		{
			System.out.println( "Failed to create working schema." );
			return;
		}
		
		long t2 = System.currentTimeMillis();
		System.out.println( "Time to create working schema: " + ( ( t2 - t1 ) / 1000.0 ) + "s" );
		System.out.println( "Current total runtime: " + ( ( t2 - first ) / 1000.0 ) + "s" );
		
		f = new File ( "ComputationConvergence.csv" );
		try 
		{
			fw = new FileWriter( f );
			fw.write( "Iteration,ExpectedGoals,ProbHomeGoal,ProbAwayGoal,ExpectedWin,ProbHomeWin,ProbAwayWin\n" );
		} 
		catch (IOException e) 
		{
			System.out.println( "Failed to open file for writing." );
			e.printStackTrace();
			return;
		}
		
		for ( int i = 1; i <= max_num_iterations; i++ )
		{
			System.out.println( "Iteration " + i );
			if ( 0 != computeExpectedGoals( i ) )
			{
				System.out.println( "Failed to compute expected goals." );
				return;
			}
			
			if ( checkConvergence( i ) )
			{
				break;
			}
			
			System.gc();
		}
		
		try
		{
			fw.close();
		}
		catch ( IOException e )
		{
			System.out.println( "Failed to close file." );
			e.printStackTrace();
			return;
		}
		
		long t3 = System.currentTimeMillis();
		System.out.println( "Time to calculate expected goals: " + ( ( t3 - t2 ) / 1000.0 ) + "s" );
		System.out.println( "Current total runtime: " + ( ( t3 - first ) / 1000.0 ) + "s" );

		if ( 0 != disconnectDB() )
		{
			System.out.println( "Failed to disconnect from database." );
			return;
		}
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
	
	private static int disconnectDB()
	{
		try
		{
			con.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to close connection." );
			return -1;
		}
		return 0;
	}
	
	private static int createWorkingSchema()
	{
		try
		{
			Statement st1 = con.createStatement();
			//st1.execute( "DROP SCHEMA IF EXISTS NHL_Latest_10_Sequence_Tree;" );
			//st1.execute( "CREATE SCHEMA NHL_Latest_10_Sequence_Tree;" );
			st1.execute( "USE NHL_Latest_10_Sequence_Tree;" );
			//st1.execute( "CREATE TABLE Nodes AS SELECT * FROM NHL_Latest_10_Sequence_Tree_Prob_Home_Goal.Nodes;" );
			//st1.execute( "ALTER TABLE Nodes ADD INDEX ( NodeId );" );
			//st1.execute( "CREATE TABLE Edges AS SELECT * FROM NHL_Latest_10_Sequence_Tree_Prob_Home_Goal.Edges;" );
			//st1.execute( "ALTER TABLE Edges ADD INDEX ( FromNodeId );" );
			//st1.execute( "ALTER TABLE Edges ADD INDEX ( ToNodeId );" );
			//st1.execute( "ALTER TABLE Edges ADD INDEX ( FromNodeId, ToNodeId );" );
			
			st1.execute( "DROP TABLE IF EXISTS Nodes_Expected_Goals;" );
			st1.execute( "DROP TABLE IF EXISTS Nodes_Probability_Home_Goal;" );
			st1.execute( "DROP TABLE IF EXISTS Nodes_Probability_Away_Goal;" );
			st1.execute( "DROP TABLE IF EXISTS Nodes_Expected_Win;" );
			st1.execute( "DROP TABLE IF EXISTS Nodes_Probability_Home_Win;" );
			st1.execute( "DROP TABLE IF EXISTS Nodes_Probability_Away_Win;" );
			
			st1.execute( "CREATE TABLE Nodes_Expected_Goals AS SELECT NodeId, Reward_Expected_Goals AS Expectation0 FROM Nodes;" );
			st1.execute( "ALTER TABLE Nodes_Expected_Goals ADD INDEX ( NodeId );" );
			st1.execute( "ALTER TABLE Nodes_Expected_Goals ADD INDEX NodeExpectation0 ( NodeId, Expectation0 );" );
			
			st1.execute( "CREATE TABLE Nodes_Probability_Home_Goal AS SELECT NodeId, Reward_Probability_Home_Goal AS Expectation0 FROM Nodes;" );
			st1.execute( "ALTER TABLE Nodes_Probability_Home_Goal ADD INDEX ( NodeId );" );
			st1.execute( "ALTER TABLE Nodes_Probability_Home_Goal ADD INDEX NodeExpectation0 ( NodeId, Expectation0 );" );
			
			st1.execute( "CREATE TABLE Nodes_Probability_Away_Goal AS SELECT NodeId, Reward_Probability_Away_Goal AS Expectation0 FROM Nodes;" );
			st1.execute( "ALTER TABLE Nodes_Probability_Away_Goal ADD INDEX ( NodeId );" );
			st1.execute( "ALTER TABLE Nodes_Probability_Away_Goal ADD INDEX NodeExpectation0 ( NodeId, Expectation0 );" );
			
			st1.execute( "CREATE TABLE Nodes_Expected_Win AS SELECT NodeId, Reward_Expected_Win AS Expectation0 FROM Nodes;" );
			st1.execute( "ALTER TABLE Nodes_Expected_Win ADD INDEX ( NodeId );" );
			st1.execute( "ALTER TABLE Nodes_Expected_Win ADD INDEX NodeExpectation0 ( NodeId, Expectation0 );" );
			
			st1.execute( "CREATE TABLE Nodes_Probability_Home_Win AS SELECT NodeId, Reward_Probability_Home_Win AS Expectation0 FROM Nodes;" );
			st1.execute( "ALTER TABLE Nodes_Probability_Home_Win ADD INDEX ( NodeId );" );
			st1.execute( "ALTER TABLE Nodes_Probability_Home_Win ADD INDEX NodeExpectation0 ( NodeId, Expectation0 );" );
			
			st1.execute( "CREATE TABLE Nodes_Probability_Away_Win AS SELECT NodeId, Reward_Probability_Away_Win AS Expectation0 FROM Nodes;" );
			st1.execute( "ALTER TABLE Nodes_Probability_Away_Win ADD INDEX ( NodeId );" );
			st1.execute( "ALTER TABLE Nodes_Probability_Away_Win ADD INDEX NodeExpectation0 ( NodeId, Expectation0 );" );
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to migrate to working schema;" );
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	private static int computeExpectedGoals( int iteration )
	{
		try
		{
			System.out.println( "Adding nodes to list." );
			ArrayList<Integer> ids = new ArrayList<Integer>();
			Statement st1 = con.createStatement();
			ResultSet rs1 = st1.executeQuery( "SELECT NodeId FROM Nodes_Expected_Goals;" );
			while ( rs1.next() )
			{
				ids.add( rs1.getInt( "NodeId" ) );
			}
			rs1.close();
			System.out.println( "Finished adding nodes to list." );
			
			System.out.println( "Adding column to table..." );
			st1.execute( "ALTER TABLE Nodes_Expected_Goals ADD COLUMN Expectation" + iteration + " DOUBLE;" );
			st1.execute( "ALTER TABLE Nodes_Probability_Home_Goal ADD COLUMN Expectation" + iteration + " DOUBLE;" );
			st1.execute( "ALTER TABLE Nodes_Probability_Away_Goal ADD COLUMN Expectation" + iteration + " DOUBLE;" );
			st1.execute( "ALTER TABLE Nodes_Expected_Win ADD COLUMN Expectation" + iteration + " DOUBLE;" );
			st1.execute( "ALTER TABLE Nodes_Probability_Home_Win ADD COLUMN Expectation" + iteration + " DOUBLE;" );
			st1.execute( "ALTER TABLE Nodes_Probability_Away_Win ADD COLUMN Expectation" + iteration + " DOUBLE;" );
			if ( iteration > 60 )
			{
				st1.execute( "ALTER TABLE Nodes_Expected_Goals DROP INDEX NodeExpectation" + ( iteration - 60 ) + ";" );
				st1.execute( "ALTER TABLE Nodes_Expected_Goals DROP COLUMN Expectation" + iteration + ";" );
				
				st1.execute( "ALTER TABLE Nodes_Probability_Home_Goal DROP INDEX NodeExpectation" + ( iteration - 60 ) + ";" );
				st1.execute( "ALTER TABLE Nodes_Probability_Home_Goal DROP COLUMN Expectation" + iteration + ";" );

				st1.execute( "ALTER TABLE Nodes_Probability_Away_Goal DROP INDEX NodeExpectation" + ( iteration - 60 ) + ";" );
				st1.execute( "ALTER TABLE Nodes_Probability_Away_Goal DROP COLUMN Expectation" + iteration + ";" );
				
				st1.execute( "ALTER TABLE Nodes_Expected_Win DROP INDEX NodeExpectation" + ( iteration - 60 ) + ";" );
				st1.execute( "ALTER TABLE Nodes_Expected_Win DROP COLUMN Expectation" + iteration + ";" );
				
				st1.execute( "ALTER TABLE Nodes_Probability_Home_Win DROP INDEX NodeExpectation" + ( iteration - 60 ) + ";" );
				st1.execute( "ALTER TABLE Nodes_Probability_Home_Win DROP COLUMN Expectation" + iteration + ";" );
				
				st1.execute( "ALTER TABLE Nodes_Probability_Away_Win DROP INDEX NodeExpectation" + ( iteration - 60 ) + ";" );
				st1.execute( "ALTER TABLE Nodes_Probability_Away_Win DROP COLUMN Expectation" + iteration + ";" );
			}
			st1.execute( "ALTER TABLE Nodes_Expected_Goals ADD INDEX NodeExpectation" + iteration + " ( NodeId, Expectation" + iteration + " );" );
			st1.execute( "ALTER TABLE Nodes_Probability_Home_Goal ADD INDEX NodeExpectation" + iteration + " ( NodeId, Expectation" + iteration + " );" );
			st1.execute( "ALTER TABLE Nodes_Probability_Away_Goal ADD INDEX NodeExpectation" + iteration + " ( NodeId, Expectation" + iteration + " );" );
			st1.execute( "ALTER TABLE Nodes_Expected_Win ADD INDEX NodeExpectation" + iteration + " ( NodeId, Expectation" + iteration + " );" );
			st1.execute( "ALTER TABLE Nodes_Probability_Home_Win ADD INDEX NodeExpectation" + iteration + " ( NodeId, Expectation" + iteration + " );" );
			st1.execute( "ALTER TABLE Nodes_Probability_Away_Win ADD INDEX NodeExpectation" + iteration + " ( NodeId, Expectation" + iteration + " );" );
			System.out.println( "Finished adding column to table." );
			
			int len = ids.size();
			
			for ( int i = 0; i < len; i++ )
			{
				int nodeId = ids.get( i );
				if ( i % 5000 == 0 )
				{
					System.out.println( "Processing Node " + nodeId );
				}
				double alpha = computeAlpha( iteration );
				
				rs1 = st1.executeQuery( "SELECT Reward_Expected_Goals, " + 
										"Reward_Probability_Home_Goal, " + 
										"Reward_Probability_Away_Goal, " +
										"Reward_Expected_Win, " + 
										"Reward_Probability_Home_Win, " + 
										"Reward_Probability_Away_Win, " +
										"Occurrences FROM Nodes WHERE NodeId = " + nodeId + ";" );
				rs1.first();
				double reward_expected_goals = rs1.getDouble( "Reward_Expected_Goals" );
				double reward_probability_home_goal = rs1.getDouble( "Reward_Probability_Home_Goal" );
				double reward_probability_away_goal = rs1.getDouble( "Reward_Probability_Away_Goal" );
				double reward_expected_win = rs1.getDouble( "Reward_Expected_Win" );
				double reward_probability_home_win = rs1.getDouble( "Reward_Probability_Home_Win" );
				double reward_probability_away_win = rs1.getDouble( "Reward_Probability_Away_Win" );
				int occurrences = rs1.getInt( "Occurrences" );
				rs1.close();
				
//				if ( iteration == 0 )
//				{
//					st1.execute( "UPDATE Nodes_Expected_Goals SET Expectation" + iteration + " = " + ( alpha * reward ) + ";" );
//					continue;
//				}
				
				rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + " FROM Nodes_Expected_Goals WHERE NodeId = " + nodeId + ";" );
				rs1.first();
				double last_expected_goals = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
				rs1.close();
				
				rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + " FROM Nodes_Probability_Home_Goal WHERE NodeId = " + nodeId + ";" );
				rs1.first();
				double last_probability_home_goal = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
				rs1.close();
				
				rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + " FROM Nodes_Probability_Away_Goal WHERE NodeId = " + nodeId + ";" );
				rs1.first();
				double last_probability_away_goal = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
				rs1.close();
				
				rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + " FROM Nodes_Expected_Win WHERE NodeId = " + nodeId + ";" );
				rs1.first();
				double last_expected_win = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
				rs1.close();
				
				rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + " FROM Nodes_Probability_Home_Win WHERE NodeId = " + nodeId + ";" );
				rs1.first();
				double last_probability_home_win = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
				rs1.close();
				
				rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + " FROM Nodes_Probability_Away_Win WHERE NodeId = " + nodeId + ";" );
				rs1.first();
				double last_probability_away_win = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
				rs1.close();
				
				rs1 = st1.executeQuery( "SELECT ToNodeId, Occurrences FROM Edges WHERE FromNodeId = " + nodeId + ";" );
				ArrayList<Integer> children = new ArrayList<Integer>();
				ArrayList<Integer> counts = new ArrayList<Integer>();
				while ( rs1.next() )
				{
					children.add( rs1.getInt( "ToNodeId" ) );
					counts.add( rs1.getInt( "Occurrences" ) );
				}
				rs1.close();
				
				int numChildren = children.size();
				
				double last_children_expected_goals = 0.0;
				double last_children_probability_home_goal = 0.0;
				double last_children_probability_away_goal = 0.0;
				double last_children_expected_win = 0.0;
				double last_children_probability_home_win = 0.0;
				double last_children_probability_away_win = 0.0;
				
				for ( int j = 0; j < numChildren; j++ )
				{
					rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + 
										    " FROM Nodes_Expected_Goals " + 
										    "WHERE NodeId = " + children.get( j ) + ";" );
					rs1.first();
					double expectation = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
					rs1.close();
					last_children_expected_goals += ( ( ( double ) counts.get( j ) ) / ( ( double ) occurrences ) ) * expectation;
					
					rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + 
						    " FROM Nodes_Probability_Home_Goal " + 
						    "WHERE NodeId = " + children.get( j ) + ";" );
					rs1.first();
					expectation = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
					rs1.close();
					last_children_probability_home_goal += ( ( ( double ) counts.get( j ) ) / ( ( double ) occurrences ) ) * expectation;
					
					rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + 
						    " FROM Nodes_Probability_Away_Goal " + 
						    "WHERE NodeId = " + children.get( j ) + ";" );
					rs1.first();
					expectation = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
					rs1.close();
					last_children_probability_away_goal += ( ( ( double ) counts.get( j ) ) / ( ( double ) occurrences ) ) * expectation;
					
					rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + 
						    " FROM Nodes_Expected_Win " + 
						    "WHERE NodeId = " + children.get( j ) + ";" );
					rs1.first();
					expectation = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
					rs1.close();
					last_children_expected_win += ( ( ( double ) counts.get( j ) ) / ( ( double ) occurrences ) ) * expectation;
					
					rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + 
						    " FROM Nodes_Probability_Home_Win " + 
						    "WHERE NodeId = " + children.get( j ) + ";" );
					rs1.first();
					expectation = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
					rs1.close();
					last_children_probability_home_win += ( ( ( double ) counts.get( j ) ) / ( ( double ) occurrences ) ) * expectation;
					
					rs1 = st1.executeQuery( "SELECT Expectation" + ( iteration - 1 ) + 
						    " FROM Nodes_Probability_Away_Win " + 
						    "WHERE NodeId = " + children.get( j ) + ";" );
					rs1.first();
					expectation = rs1.getDouble( "Expectation" + ( iteration - 1 ) );
					rs1.close();
					last_children_probability_away_win += ( ( ( double ) counts.get( j ) ) / ( ( double ) occurrences ) ) * expectation;
				}
								
				//double newexpectation = ( 1.0 - alpha ) * lastexpectation + ( alpha ) * ( reward + discount * lastchildrenexpectation );
				double new_expected_goals = reward_expected_goals + last_children_expected_goals;
				double new_probability_home_goal = reward_probability_home_goal + last_children_probability_home_goal;
				if ( new_probability_home_goal > 1.0 )
				{
					new_probability_home_goal = 1.0;
				}
				double new_probability_away_goal = reward_probability_away_goal + last_children_probability_away_goal;
				if ( new_probability_away_goal > 1.0 )
				{
					new_probability_away_goal = 1.0;
				}
				double new_expected_win = reward_expected_win + last_children_expected_win;
				double new_probability_home_win = reward_probability_home_win + last_children_probability_home_win;
				if ( new_probability_home_win > 1.0 )
				{
					new_probability_home_win = 1.0;
				}
				double new_probability_away_win = reward_probability_away_win + last_children_probability_away_win;
				if ( new_probability_away_win > 1.0 )
				{
					new_probability_away_win = 1.0;
				}
				
				st1.execute( "UPDATE Nodes_Expected_Goals " + 
							 "SET Expectation" + iteration + " = " + new_expected_goals + " " +
							 "WHERE NodeId = " + nodeId + ";" );
				st1.execute( "UPDATE Nodes_Probability_Home_Goal " + 
						 "SET Expectation" + iteration + " = " + new_probability_home_goal + " " +
						 "WHERE NodeId = " + nodeId + ";" );
				st1.execute( "UPDATE Nodes_Probability_Away_Goal " + 
						 "SET Expectation" + iteration + " = " + new_probability_away_goal + " " +
						 "WHERE NodeId = " + nodeId + ";" );
				st1.execute( "UPDATE Nodes_Expected_Win " + 
						 "SET Expectation" + iteration + " = " + new_expected_win + " " +
						 "WHERE NodeId = " + nodeId + ";" );
				st1.execute( "UPDATE Nodes_Probability_Home_Win " + 
						 "SET Expectation" + iteration + " = " + new_probability_home_win + " " +
						 "WHERE NodeId = " + nodeId + ";" );
				st1.execute( "UPDATE Nodes_Probability_Away_Win " + 
						 "SET Expectation" + iteration + " = " + new_probability_away_win + " " +
						 "WHERE NodeId = " + nodeId + ";" );
			}
			
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to compute expected goals." );
			e.printStackTrace();
			return -1;
		}
		
		System.gc();
		return 0;
	}
	
	private static double computeAlpha( int iteration )
	{
		double alpha = 1.0 / ( 1.0 + ((double)iteration));
		return alpha;
	}
	
	private static boolean checkConvergence(int iteration)
	{
		try
		{
			Statement st1 = con.createStatement();
			double last = 0.0;
			double current = 0.0;

			ResultSet rs1 = st1.executeQuery("SELECT SUM(ABS(Expectation" + ( iteration - 1 ) + ")) AS LastTotal, " + 
											 "SUM(ABS(Expectation" + iteration + ")) AS CurrentTotal FROM Nodes_Expected_Goals;" );
			rs1.first();
			last = rs1.getDouble( "LastTotal" );
			current = rs1.getDouble( "CurrentTotal" );
			rs1.close();
			double relativeChange_expected_goals = Math.abs( current - last ) / Math.abs( last );
			System.out.println("Relative change expected goals: " + relativeChange_expected_goals );
			
			rs1 = st1.executeQuery("SELECT SUM(ABS(Expectation" + ( iteration - 1 ) + ")) AS LastTotal, " + 
					 "SUM(ABS(Expectation" + iteration + ")) AS CurrentTotal FROM Nodes_Probability_Home_Goal;" );
			rs1.first();
			last = rs1.getDouble( "LastTotal" );
			current = rs1.getDouble( "CurrentTotal" );
			rs1.close();
			double relativeChange_probability_home_goal = Math.abs( current - last ) / Math.abs( last );
			System.out.println("Relative change probability_home_goal: " + relativeChange_probability_home_goal );
			
			rs1 = st1.executeQuery("SELECT SUM(ABS(Expectation" + ( iteration - 1 ) + ")) AS LastTotal, " + 
					 "SUM(ABS(Expectation" + iteration + ")) AS CurrentTotal FROM Nodes_Probability_Away_Goal;" );
			rs1.first();
			last = rs1.getDouble( "LastTotal" );
			current = rs1.getDouble( "CurrentTotal" );
			rs1.close();
			double relativeChange_probability_away_goal = Math.abs( current - last ) / Math.abs( last );
			System.out.println("Relative change probability_away_goal: " + relativeChange_probability_away_goal );
			
			
			rs1 = st1.executeQuery("SELECT SUM(ABS(Expectation" + ( iteration - 1 ) + ")) AS LastTotal, " + 
					 "SUM(ABS(Expectation" + iteration + ")) AS CurrentTotal FROM Nodes_Expected_Win;" );
			rs1.first();
			last = rs1.getDouble( "LastTotal" );
			current = rs1.getDouble( "CurrentTotal" );
			rs1.close();
			double relativeChange_expected_win = Math.abs( current - last ) / Math.abs( last );
			System.out.println("Relative change expected win: " + relativeChange_expected_win );
			
			rs1 = st1.executeQuery("SELECT SUM(ABS(Expectation" + ( iteration - 1 ) + ")) AS LastTotal, " + 
			"SUM(ABS(Expectation" + iteration + ")) AS CurrentTotal FROM Nodes_Probability_Home_Win;" );
			rs1.first();
			last = rs1.getDouble( "LastTotal" );
			current = rs1.getDouble( "CurrentTotal" );
			rs1.close();
			double relativeChange_probability_home_win = Math.abs( current - last ) / Math.abs( last );
			System.out.println("Relative change probability_home_win: " + relativeChange_probability_home_win );
			
			rs1 = st1.executeQuery("SELECT SUM(ABS(Expectation" + ( iteration - 1 ) + ")) AS LastTotal, " + 
			"SUM(ABS(Expectation" + iteration + ")) AS CurrentTotal FROM Nodes_Probability_Away_Win;" );
			rs1.first();
			last = rs1.getDouble( "LastTotal" );
			current = rs1.getDouble( "CurrentTotal" );
			rs1.close();
			double relativeChange_probability_away_win = Math.abs( current - last ) / Math.abs( last );
			System.out.println("Relative change probability_away_win: " + relativeChange_probability_away_win );
			
			st1.close();
			
			try
			{
				fw.write( iteration + "," + 
						  relativeChange_expected_goals + "," +
						  relativeChange_probability_home_goal + "," + 
						  relativeChange_probability_away_goal + "," + 
						  relativeChange_expected_win + "," +
						  relativeChange_probability_home_win + "," +
						  relativeChange_probability_away_win + "\n" );
			}
			catch (IOException e)
			{
				System.out.println( "Failed to write to file." );
				e.printStackTrace();
				return false;
			}
			
			
			if ( ( relativeChange_expected_goals < convergence ) ||
				 ( relativeChange_probability_home_goal < convergence ) ||
				 ( relativeChange_probability_away_goal < convergence ) ||
				 ( relativeChange_expected_win < convergence ) || 
				 ( relativeChange_probability_home_win < convergence ) ||
				 ( relativeChange_probability_away_win < convergence ) )
			{
				return true;
			}
		}
		catch ( SQLException e )
		{
			System.out.println( "Could not check convergence." );
			e.printStackTrace();
			return false;
		}
		
		System.gc();
		return false;
	}
}
