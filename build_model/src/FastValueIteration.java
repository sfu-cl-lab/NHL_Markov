import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import com.mysql.jdbc.Connection;


public class FastValueIteration
{
	//Configuration variables
	private static String dbaddress = "mysql://localhost";
	private static String dbname = "NHL_Sequence_Tree_Full_GD_MD";
	private static String dbname2 = "NHL_Fast_Value_Iteration_Full_GD_MD";
	private static double convergence_criteria = 0.0001;
	private static int max_number_of_iterations = 100000;
	
	//Internal variables
	private static Connection con = null;
	private static double[] last_convergence_value = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
	private static double[] current_convergence_value = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
	private static boolean[] converged = { false, false, false, false, false, false, false, false, false }; 
	private static String[] table_names = { "Expected_Wins",
											"Probability_Home_Win",
											"Probability_Away_Win",
											"Expected_Goals",
											"Probability_Next_Home_Goal",
											"Probability_Next_Away_Goal",
											"Expected_Penalties",
											"Probability_Next_Home_Penalty",
											"Probability_Next_Away_Penalty" };
	
	private static HashMap<Integer,ValueIterationNode> nodes = null;
	private static ArrayList<Integer> nodesList = null;
	
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
		
		//Get Nodes, edges
		nodes = new HashMap<Integer,ValueIterationNode>();
		nodesList = new ArrayList<Integer>();
		
		if ( 0 != getNodesAndEdges() )
		{
			System.out.println( "Failed to get nodes and edges." );
			return;
		}
		
		long t3 = System.currentTimeMillis();
		System.out.println( "Time to get nodes and edges: " + ( ( t3 - t2 ) / 1000.0 ) + "s" );
		System.out.println( "Cumulative running time: " + ( ( t3 - start ) / 1000.0 ) + "s" );
		
		//Run value iteration
		if ( 0 != runValueIteration( start ) )
		{
			System.out.println( "Value iteration failed." );
			return;
		}
		
		long t4 = System.currentTimeMillis();
		System.out.println( "Time to run value iteration: " + ( ( t4 - t3 ) / 1000.0 ) + "s" );
		System.out.println( "Cumulative running time: " + ( ( t4 - start ) / 1000.0 ) + "s" );
		
		
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
			
			st1.execute( "ALTER TABLE Edges ADD INDEX ( ToNodeId );" );
			
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
			
			//CheckingMultipleIterations
			String table = "Probability_Next_Home_Goal";
			st1.execute( "DROP TABLE IF EXISTS CheckingMultipleIterations_" + table + ";" );
			String createStatement = "CREATE TABLE CheckingMultipleIterations_" + table + " ( NodeId INT, ";
			for ( int i = 1; i <= 50; i++ )
			{
				createStatement += "`Iteration_" + i + "` DOUBLE";
				
				if ( i != 50 )
				{
					createStatement += ", ";
				}
			}
			
			createStatement += " );" ;
			System.out.println( createStatement );
			st1.execute( createStatement );
			
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
			
			System.out.println( "ALTER TABLE " + name + " ADD COLUMN LastValue DOUBLE DEFAULT 0.0, ADD COLUMN CurrentValue DOUBLE DEFAULT 0.0, " + 
								"ADD COLUMN QStar DOUBLE DEFAULT 0.0;" );
			st1.execute( "ALTER TABLE " + name + " ADD COLUMN LastValue DOUBLE DEFAULT 0.0, ADD COLUMN CurrentValue DOUBLE DEFAULT 0.0, " + 
					"ADD COLUMN QStar DOUBLE DEFAULT 0.0;" );
			
			System.out.println( "ALTER TABLE " + name + " ADD INDEX ( NodeId, LastValue, CurrentValue, QStar );" );
			st1.execute( "ALTER TABLE " + name + " ADD INDEX ( NodeId, LastValue, CurrentValue, QStar );" );
			
			System.out.println( "ALTER TABLE " + name + " ADD INDEX ( NodeId, CurrentValue );" );
			st1.execute( "ALTER TABLE " + name + " ADD INDEX ( NodeId, CurrentValue );" );
			
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
		int len = nodesList.size();
		for ( int i = 1; i <= max_number_of_iterations; i++ )
		{
			long last = System.currentTimeMillis();
			
			//Update each node
			for ( int j = 0; j < len; j++ )
			{
				int nodeId = nodesList.get( j );
				
				ValueIterationNode node = nodes.get( nodeId );
				
				//Expected_Wins
				if ( !converged[0] )
				{
					ArrayList<Integer> children = node.getChildren();
					ArrayList<Integer> childOccurrences = node.getChildrenOccurences();
					int numChildren = children.size();
					
					double value = 0.0;
					double qstar = 0.0;
					
					for ( int k = 0; k < numChildren; k++ )
					{
						ValueIterationNode childNode = nodes.get( children.get( k ) );
						double subval = ( ( double ) childOccurrences.get( k ) ) * childNode.getLastValue( 0 );
						if ( qstar <= subval )
						{
							qstar = subval;
						}
						value += subval;
					}
					double contribution = node.getRewardExpectedWins() + ( 1 / ( ( double ) node.getOccurrences() ) ) * value;
					double finalqstar =  node.getRewardExpectedWins() + ( 1 / ( ( double ) node.getOccurrences() ) ) * qstar;
					node.setCurrentQStar( 0, finalqstar );
					node.setCurrentValue(0, contribution);
					current_convergence_value[0] += Math.abs( contribution );
				}
				
				//Probability_Home_Win
				if ( !converged[1] )
				{
					ArrayList<Integer> children = node.getChildren();
					ArrayList<Integer> childOccurrences = node.getChildrenOccurences();
					int numChildren = children.size();
					
					double value = 0.0;
					double qstar = 0.0;
					
					for ( int k = 0; k < numChildren; k++ )
					{
						ValueIterationNode childNode = nodes.get( children.get( k ) );
						double subval = 0.0;
						if ( childNode.isHomeWin() )
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * 1.0;
						}
						else if ( childNode.isAwayWin() )
						{
							subval = 0.0;
						}
						else
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * childNode.getLastValue( 1 );
						}
						
						if ( qstar <= subval )
						{
							qstar = subval;
						}
						value += subval;
					}
					double contribution = ( 1 / ( ( double ) node.getOccurrences() ) ) * value;
					double finalqstar =  ( 1 / ( ( double ) node.getOccurrences() ) ) * qstar;
					if ( node.isHomeWin() )
					{
						finalqstar += 1.0;
						contribution += 1.0;
					}
					node.setCurrentQStar( 1, finalqstar );
					node.setCurrentValue( 1, contribution);
					current_convergence_value[ 1 ] += Math.abs( contribution );
				}
				
				//Probability_Away_Win
				if ( !converged[2] )
				{
					ArrayList<Integer> children = node.getChildren();
					ArrayList<Integer> childOccurrences = node.getChildrenOccurences();
					int numChildren = children.size();
					
					double value = 0.0;
					double qstar = 0.0;
					
					for ( int k = 0; k < numChildren; k++ )
					{
						ValueIterationNode childNode = nodes.get( children.get( k ) );
						double subval = 0.0;
						if ( childNode.isAwayWin() )
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * 1.0;
						}
						else if ( childNode.isHomeWin() )
						{
							subval = 0.0;
						}
						else
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * childNode.getLastValue( 2 );
						}
						
						if ( qstar <= subval )
						{
							qstar = subval;
						}
						value += subval;
					}
					double contribution = ( 1 / ( ( double ) node.getOccurrences() ) ) * value;
					double finalqstar =  ( 1 / ( ( double ) node.getOccurrences() ) ) * qstar;
					if ( node.isAwayWin() )
					{
						finalqstar += 1.0;
						contribution += 1.0;
					}
					node.setCurrentQStar( 2, finalqstar );
					node.setCurrentValue( 2, contribution);
					current_convergence_value[ 2 ] += Math.abs( contribution );
				}
				
				//Expected_Goals
				if ( !converged[3] )
				{
					ArrayList<Integer> children = node.getChildren();
					ArrayList<Integer> childOccurrences = node.getChildrenOccurences();
					int numChildren = children.size();
					
					double value = 0.0;
					double qstar = 0.0;
					
					for ( int k = 0; k < numChildren; k++ )
					{
						ValueIterationNode childNode = nodes.get( children.get( k ) );
						double subval = ( ( double ) childOccurrences.get( k ) ) * childNode.getLastValue( 3 );
						if ( qstar <= subval )
						{
							qstar = subval;
						}
						value += subval;
					}
					double contribution = node.getRewardExpectedGoals() + ( 1 / ( ( double ) node.getOccurrences() ) ) * value;
					double finalqstar =  node.getRewardExpectedGoals() + ( 1 / ( ( double ) node.getOccurrences() ) ) * qstar;
					node.setCurrentQStar( 3, finalqstar );
					node.setCurrentValue( 3, contribution);
					current_convergence_value[3] += Math.abs( contribution );
				}
				
				//Probability_Next_Home_Goal
				if ( !converged[4] )
				{
					ArrayList<Integer> children = node.getChildren();
					ArrayList<Integer> childOccurrences = node.getChildrenOccurences();
					int numChildren = children.size();
					
					double value = 0.0;
					double qstar = 0.0;
					
					for ( int k = 0; k < numChildren; k++ )
					{
						ValueIterationNode childNode = nodes.get( children.get( k ) );
						double subval = 0.0;
						if ( childNode.isHomeGoal() )
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * 1.0;
						}
						else if ( childNode.isAwayGoal() )
						{
							subval = 0.0;
						}
						else
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * childNode.getLastValue( 4 );
						}
						
						if ( qstar <= subval )
						{
							qstar = subval;
						}
						value += subval;
					}
					double contribution = ( 1 / ( ( double ) node.getOccurrences() ) ) * value;
					double finalqstar =  ( 1 / ( ( double ) node.getOccurrences() ) ) * qstar;
					node.setCurrentQStar( 4, finalqstar );
					node.setCurrentValue( 4, contribution);
					node.setIterationValue( i, contribution );
					current_convergence_value[ 4 ] += Math.abs( contribution );
				}
				
				//Probability_Next_Away_Goal
				if ( !converged[5] )
				{
					ArrayList<Integer> children = node.getChildren();
					ArrayList<Integer> childOccurrences = node.getChildrenOccurences();
					int numChildren = children.size();
					
					double value = 0.0;
					double qstar = 0.0;
					
					for ( int k = 0; k < numChildren; k++ )
					{
						ValueIterationNode childNode = nodes.get( children.get( k ) );
						double subval = 0.0;
						if ( childNode.isAwayGoal() )
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * 1.0;
						}
						else if ( childNode.isHomeGoal() )
						{
							subval = 0.0;
						}
						else
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * childNode.getLastValue( 5 );
						}
						
						if ( qstar <= subval )
						{
							qstar = subval;
						}
						value += subval;
					}
					double contribution = ( 1 / ( ( double ) node.getOccurrences() ) ) * value;
					double finalqstar =  ( 1 / ( ( double ) node.getOccurrences() ) ) * qstar;
					node.setCurrentQStar( 5, finalqstar );
					node.setCurrentValue( 5, contribution);
					current_convergence_value[ 5 ] += Math.abs( contribution );
				}
				
				//Expected_Penalties
				if ( !converged[6] )
				{
					ArrayList<Integer> children = node.getChildren();
					ArrayList<Integer> childOccurrences = node.getChildrenOccurences();
					int numChildren = children.size();
					
					double value = 0.0;
					double qstar = 0.0;
					
					for ( int k = 0; k < numChildren; k++ )
					{
						ValueIterationNode childNode = nodes.get( children.get( k ) );
						double subval = ( ( double ) childOccurrences.get( k ) ) * childNode.getLastValue( 6 );
						if ( qstar <= subval )
						{
							qstar = subval;
						}
						value += subval;
					}
					double contribution = node.getRewardExpectedPenalties() + ( 1 / ( ( double ) node.getOccurrences() ) ) * value;
					double finalqstar =  node.getRewardExpectedPenalties() + ( 1 / ( ( double ) node.getOccurrences() ) ) * qstar;
					node.setCurrentQStar( 6, finalqstar );
					node.setCurrentValue( 6, contribution);
					current_convergence_value[6] += Math.abs( contribution );
				}
				
				//Probability_Next_Home_Penalty
				if ( !converged[7] )
				{
					ArrayList<Integer> children = node.getChildren();
					ArrayList<Integer> childOccurrences = node.getChildrenOccurences();
					int numChildren = children.size();
					
					double value = 0.0;
					double qstar = 0.0;
					
					for ( int k = 0; k < numChildren; k++ )
					{
						ValueIterationNode childNode = nodes.get( children.get( k ) );
						double subval = 0.0;
						if ( childNode.isHomePenalty() )
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * 1.0;
						}
						else if ( childNode.isAwayPenalty() )
						{
							subval = 0.0;
						}
						else
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * childNode.getLastValue( 7 );
						}
						
						if ( qstar <= subval )
						{
							qstar = subval;
						}
						value += subval;
					}
					double contribution = ( 1 / ( ( double ) node.getOccurrences() ) ) * value;
					double finalqstar =  ( 1 / ( ( double ) node.getOccurrences() ) ) * qstar;
					node.setCurrentQStar( 7, finalqstar );
					node.setCurrentValue( 7, contribution);
					current_convergence_value[ 7 ] += Math.abs( contribution );
				}
				
				//Probability_Next_Away_Penalty
				if ( !converged[8] )
				{
					ArrayList<Integer> children = node.getChildren();
					ArrayList<Integer> childOccurrences = node.getChildrenOccurences();
					int numChildren = children.size();
					
					double value = 0.0;
					double qstar = 0.0;
					
					for ( int k = 0; k < numChildren; k++ )
					{
						ValueIterationNode childNode = nodes.get( children.get( k ) );
						double subval = 0.0;
						if ( childNode.isAwayPenalty() )
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * 1.0;
						}
						else if ( childNode.isHomePenalty() )
						{
							subval = 0.0;
						}
						else
						{
							subval = ( ( double ) childOccurrences.get( k ) ) * childNode.getLastValue( 8 );
						}
						
						if ( qstar <= subval )
						{
							qstar = subval;
						}
						value += subval;
					}
					double contribution = ( 1 / ( ( double ) node.getOccurrences() ) ) * value;
					double finalqstar =  ( 1 / ( ( double ) node.getOccurrences() ) ) * qstar;
					node.setCurrentQStar( 8, finalqstar );
					node.setCurrentValue( 8, contribution);
					current_convergence_value[ 8 ] += Math.abs( contribution );
				}
			}
			
			for ( int j = 0; j < len; j++ )
			{
				int nodeId = nodesList.get( j );
				ValueIterationNode node = nodes.get( nodeId );
				node.backupLastValues();
			}
			
			//Check convergence
			for ( int j = 0; j < 9; j++ )
			{
				if ( converged[j] )
				{
					continue;
				}
				
				if ( 1 == checkConvergence( table_names[j], j, i ) )
				{
					converged[j] = true;
				}
				
				if ( converged[j] )
				{
					for ( int k = 0; k < len; k++ )
					{
						int nodeId = nodesList.get( k );
						ValueIterationNode node = nodes.get( nodeId );
						double lastValue = node.getLastValue( j );
						double currentValue = node.getCurrentValue( j );
						double qstar = node.getQStar( j );

						try
						{
							Statement st1 = con.createStatement();
							st1.execute( "UPDATE " + dbname2 + "." + table_names[j] + " SET LastValue = " + lastValue + ", " + 
										 "CurrentValue = " + currentValue + ", QStar = " + qstar + " WHERE NodeId = " + nodeId + ";" );
							//if ( j == 4 )
							//{
							//	st1.execute( node.generateInsertStatement( "Probability_Next_Home_Goal" ) );
							//}
							st1.close();
						}
						catch (SQLException e)
						{
							System.out.println( "Failed to update values." );
							e.printStackTrace();
							return -1;
						}
					}
				}
			}
			
			long current = System.currentTimeMillis();
			System.out.println( "Iteration " + i + " runtime: " + ( ( current - last ) / 1000.0 ) + "s" );
			System.out.println( "Cumulative running time: " + ( ( current - startTime ) / 1000.0 ) + "s" );
			
			last = current;
			
			boolean allConverged = true;
			for ( int j = 0; j < 9; j++ )
			{
				if ( converged[j] == false )
				{
					allConverged = false;
					break;
				}
			}
			
			if ( allConverged )
			{
				break;
			}
		}
		
		for ( int j = 0; j < 9; j++ )
		{
			if ( converged[j] == false )
			{
				for ( int k = 0; k < len; k++ )
				{
					int nodeId = nodesList.get( k );
					ValueIterationNode node = nodes.get( nodeId );
					double lastValue = node.getLastValue( j );
					double currentValue = node.getCurrentValue( j );
					double qstar = node.getQStar( j );

					try
					{
						Statement st1 = con.createStatement();
						st1.execute( "UPDATE " + dbname2 + "." + table_names[j] + " SET LastValue = " + lastValue + ", " + 
									 "CurrentValue = " + currentValue + ", QStar = " + qstar + " WHERE NodeId = " + nodeId + ";" );
						//if ( j == 4 )
						//{
						//	st1.execute( node.generateInsertStatement( "Probability_Next_Home_Goal" ) );
						//}
						st1.close();
					}
					catch (SQLException e)
					{
						System.out.println( "Failed to update values." );
						e.printStackTrace();
						return -1;
					}
				}
			}
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
	
	private static int getNodesAndEdges()
	{
		try
		{
			Statement st1 = con.createStatement();
			ResultSet rs1 = st1.executeQuery( "SELECT * FROM " + dbname2 + ".Nodes;" );
			
			while ( rs1.next() )
			{
				int nodeId = rs1.getInt( "NodeId" );
				int occurrences = rs1.getInt( "Occurrences" );
				
				ValueIterationNode newNode = new ValueIterationNode( nodeId, occurrences );
				nodesList.add( nodeId );
				
				//Get rewards
				int reward_expected_wins = rs1.getInt( "Reward_Expected_Win" );
				newNode.setRewardExpectedWins( reward_expected_wins );
				int reward_expected_goals = rs1.getInt( "Reward_Expected_Goals" );
				newNode.setRewardExpectedGoals( reward_expected_goals );
				int reward_expected_penalties = rs1.getInt( "Reward_Expected_Penalties" );
				newNode.setRewardExpectedPenalties( reward_expected_penalties );
				
				Statement st2 = con.createStatement();
				
				ResultSet rs2 = st2.executeQuery( "SELECT * FROM " + dbname2 + ".Edges WHERE FromNodeId = " + nodeId + ";" );
				
				while ( rs2.next() )
				{
					int childNodeId = rs2.getInt( "ToNodeId" );
					int valueOfChildOccurrences = rs2.getInt( "Occurrences" );
					
					newNode.addChild(childNodeId, valueOfChildOccurrences);
				}
				
				rs2.close();
				
				st2.close();
				
				nodes.put( nodeId, newNode );
			}
			
			rs1.close();
			st1.close();
		}
		catch( SQLException e )
		{
			System.out.println( "Failed to get nodes and edges." );
			e.printStackTrace();
			return -1;
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
