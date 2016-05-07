import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.mysql.jdbc.Connection;

public class GetPlayerValues
{
	private static String databaseAddress = "mysql://localhost";
	private static String databaseData = "nhl_final2";
	private static String databaseTree = "nhl_sequence_tree_full";
	private static String databaseValueIteration = "nhl_fast_value_iteration_full";
	private static String databasePlayerValues = "NHL_Player_Valuations_Win";
	private static String metricTable = "action_values_method_1_probability_next_win";
	
	private static Connection con = null;
	private static File f = null;
	private static FileWriter fw = null;
	
	public static void main(String[] args)
	{
		//Connect to database
		if ( 0 != connectToDatabase() )
		{
			return;
		}
		
		//Create new database
		if ( 0 != createNewDatabase() )
		{
			return;
		}
		
		f = new File ( "FailedActionValues.txt" );
		try
		{
			fw = new FileWriter( f );
		}
		catch (IOException e)
		{
			System.out.println( "Failed to open file for writing." );
			e.printStackTrace();
			return;
		}
		
		//Apply action values to players
		if ( 0 != applyActionValuesToPlayers() )
		{
			return;
		}
		
		//Disconnect database
		if ( 0 != disconnectFromDatabase() )
		{
			return;
		}
		
		try
		{
			fw.close();
		}
		catch (IOException e)
		{
			System.out.println( "Failed to close file." );
			e.printStackTrace();
			return;
		}
	}
	
	public static int connectToDatabase()
	{
		String CONN_STR = "jdbc:" + databaseAddress;
		try 
		{
			java.lang.Class.forName( "com.mysql.jdbc.Driver" );
		} 
		catch ( Exception ex ) 
		{
			System.out.println( "Unable to load MySQL JDBC driver" );
			return -1;
		}
		
		try
		{
			con = (Connection) DriverManager.getConnection( CONN_STR, 
					"root", 
					"root" );
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to connect to database." );
			e.printStackTrace();
			return -2;
		}
		return 0;
	}
	
	public static int createNewDatabase()
	{
		try
		{
			Statement st1 = con.createStatement();
			
			st1.execute( "DROP SCHEMA IF EXISTS " + databasePlayerValues + ";" );
			
			st1.execute( "CREATE SCHEMA " + databasePlayerValues + ";" );
			
			st1.execute( "USE " + databasePlayerValues + ";" );
			
			st1.execute( "CREATE TABLE PlayerValuations ( GameId INT, EventNumber INT, PlayerId INT, FromNodeId INT, ToNodeId INT, " +
						 "`Action` TEXT, ActionValue DOUBLE );" );
			
			st1.execute( "ALTER TABLE PlayerValuations ADD INDEX ( GameId, EventNumber );" );
			st1.execute( "ALTER TABLE PlayerValuations ADD INDEX ( PlayerId );" );
			st1.execute( "ALTER TABLE PlayerValuations ADD INDEX ( FromNodeId );" );
			st1.execute( "ALTER TABLE PlayerValuations ADD INDEX ( ToNodeId );" );
			
			st1.execute( "CREATE TABLE PowerPlayPlayerValuations ( GameId INT, EventNumber INT, PlayerId INT, FromNodeId INT, ToNodeId INT, " +
					 "`Action` TEXT, ActionValue DOUBLE );" );
			
			st1.execute( "ALTER TABLE PowerPlayPlayerValuations ADD INDEX ( GameId, EventNumber );" );
			st1.execute( "ALTER TABLE PowerPlayPlayerValuations ADD INDEX ( PlayerId );" );
			st1.execute( "ALTER TABLE PowerPlayPlayerValuations ADD INDEX ( FromNodeId );" );
			st1.execute( "ALTER TABLE PowerPlayPlayerValuations ADD INDEX ( ToNodeId );" );
			
			st1.execute( "CREATE TABLE PenaltyKillPlayerValuations ( GameId INT, EventNumber INT, PlayerId INT, FromNodeId INT, ToNodeId INT, " +
					 "`Action` TEXT, ActionValue DOUBLE );" );
			
			st1.execute( "ALTER TABLE PenaltyKillPlayerValuations ADD INDEX ( GameId, EventNumber );" );
			st1.execute( "ALTER TABLE PenaltyKillPlayerValuations ADD INDEX ( PlayerId );" );
			st1.execute( "ALTER TABLE PenaltyKillPlayerValuations ADD INDEX ( FromNodeId );" );
			st1.execute( "ALTER TABLE PenaltyKillPlayerValuations ADD INDEX ( ToNodeId );" );
			
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to create new database." );
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	public static int applyActionValuesToPlayers()
	{
		try
		{
			Statement st1 = con.createStatement();
			
			st1.execute( "USE " + databaseData + ";" );
			
			ArrayList<Integer> GameIds = new ArrayList<Integer>();
			
			ResultSet rs1 = st1.executeQuery( "SELECT DISTINCT GameId FROM play_by_play_events WHERE GameId >= 2007020001 ORDER BY GameId DESC;" );
			
			while ( rs1.next() )
			{
				GameIds.add( rs1.getInt( "GameId" ) );
			}
			
			rs1.close();
			
			int number_of_games = GameIds.size();
			
			for ( int i = 0; i < number_of_games; i++ )
			{
				int gameId = GameIds.get( i );
				System.out.println( "Getting player valuations for Game = " + gameId );
				
				ArrayList<Integer> eventNumbers = new ArrayList<Integer>();
				ArrayList<String> eventTypes = new ArrayList<String>();
				ArrayList<Integer> externalEventIds = new ArrayList<Integer>();
				
				System.out.println( "Getting play-by-play events." );
				
				rs1 = st1.executeQuery( "SELECT * FROM play_by_play_events WHERE GameId = " + gameId + " ORDER BY EventNumber ASC;" );
				
				while ( rs1.next() )
				{
					String eventType = rs1.getString( "EventType" );
					
					if ( !( eventType.equalsIgnoreCase( "BLOCKED SHOT" ) || 
							eventType.equalsIgnoreCase( "FACEOFF" ) ||
							eventType.equalsIgnoreCase( "GIVEAWAY" ) ||
							eventType.equalsIgnoreCase( "GOAL" ) ||
							eventType.equalsIgnoreCase( "HIT" ) ||
							eventType.equalsIgnoreCase( "MISSED SHOT" ) ||
							eventType.equalsIgnoreCase( "PENALTY" ) ||
							eventType.equalsIgnoreCase( "SHOT" ) ||
							eventType.equalsIgnoreCase( "TAKEAWAY" ) ) )
					{
						continue;
					}
					
					eventNumbers.add( rs1.getInt( "EventNumber" ) );
					eventTypes.add( eventType );
					externalEventIds.add( rs1.getInt( "ExternalEventId" ) );
				}
				rs1.close();
				
				//Get corresponding edges
				System.out.println( "Getting edges." );
				
				int number_of_player_actions = eventNumbers.size();
				
				ArrayList<Integer> fromNodeIds = new ArrayList<Integer>();
				ArrayList<Integer> toNodeIds = new ArrayList<Integer>();
				
				for ( int j = 0; j < number_of_player_actions; j++ )
				{
					int eventNumber = eventNumbers.get( j );
					
					rs1 = st1.executeQuery( "SELECT * FROM " + databaseTree + ".Play_By_Play_Events_With_Node_Information WHERE " +
											"GameId = " + gameId + " AND EventNumber = " + eventNumber + ";" );
					
					rs1.first();
					
					fromNodeIds.add( rs1.getInt( "StartingNodeId" ) );
					toNodeIds.add( rs1.getInt( "EndingNodeId" ) );
					
					rs1.close();
				}
				
				//Get Home or Away, and Manpower Differential
				ArrayList<String> teamInvolved = new ArrayList<String>();
				ArrayList<Integer> manpowerDifferential = new ArrayList<Integer>();
				for ( int j = 0; j < number_of_player_actions; j++ )
				{
					int nodeId = toNodeIds.get( j );
					
					rs1 = st1.executeQuery( "SELECT NodeName, ManpowerDifferential FROM " + databaseTree + ".Nodes WHERE NodeId = " + nodeId + ";" );
					
					rs1.first();
					
					if ( rs1.getString( "NodeName" ).contains("HOME") )
					{
						teamInvolved.add( "HOME" );
					}
					else if ( rs1.getString( "NodeName" ).contains( "AWAY" ) )
					{
						teamInvolved.add( "AWAY" );
					}
					else
					{
						teamInvolved.add( "UNSPECIFIED" );
					}
					
					manpowerDifferential.add( rs1.getInt( "ManpowerDifferential" ) );
					
					rs1.close();
				}
				
				//Get Action Values
				System.out.println( "Getting action values." );
				
				ArrayList<Double> actionValues = new ArrayList<Double>();
				
				for ( int j = 0; j < number_of_player_actions; j++ )
				{
					int fromNodeId = fromNodeIds.get( j );
					int toNodeId = toNodeIds.get( j );
					
					rs1 = st1.executeQuery( "SELECT * FROM " + databaseValueIteration + "." + metricTable + " WHERE FromNodeId = " +
											fromNodeId + " AND ToNodeId = " + toNodeId + ";" );
					
					
					if ( !rs1.first() )
					{
						System.out.println( "fromNodeId = " + fromNodeId + ", toNodeId = " + toNodeId + ", gameId = " + gameId + ", eventNumber = " + eventNumbers.get( j ) );
						try
						{
							fw.write( "fromNodeId = " + fromNodeId + ", toNodeId = " + toNodeId + ", gameId = " + gameId + ", eventNumber = " + eventNumbers.get( j ) + "\n" );
						}
						catch (IOException e)
						{
							System.out.println( "Failed to write to file." );
							e.printStackTrace();
							return -3;
						}
						actionValues.add( 0.0 );
					}
					else
					{
						actionValues.add( rs1.getDouble( "ActionValue" ) );
					}
					
					rs1.close();
				}
				
				System.out.println( "Getting player IDs." );
				
				ArrayList<Integer> playerIds = new ArrayList<Integer>();
				
				for ( int j = 0; j < number_of_player_actions; j++ )
				{
					String eventType = eventTypes.get( j );
					int externalEventId = externalEventIds.get( j );
					
					int playerId = retrievePlayerId( eventType, externalEventId, gameId );
					
					if ( playerId < 0 )
					{
						return -2;
					}
					else if ( playerId == 0 )
					{
						System.out.println( "GameId = " + gameId + ", eventNumber = " + eventNumbers.get( j ) + ", externalEventId = " +
											externalEventId + ", fromNodeId = " + fromNodeIds.get( j ) + ", toNodeId = " + toNodeIds.get( j ) );
					}
					
					playerIds.add( playerId );
				}
				
				System.out.println( "Writing player values." );
				
				//Write player values
				for ( int j = 0; j < number_of_player_actions; j++ )
				{
					if ( playerIds.get( j ) == 0 )
					{
						try 
						{
							fw.write( "GameId = " + gameId + ", eventNumber = " + eventNumbers.get( j ) + ", externalEventId = " +
									externalEventIds.get( j ) + ", fromNodeId = " + fromNodeIds.get( j ) + ", toNodeId = " + toNodeIds.get( j ) + "\n");
						} 
						catch (IOException e) 
						{
							System.out.println( "Couldn't write to file." );
							e.printStackTrace();
							return -4;
						}
						continue;
					}
					st1.execute( "INSERT IGNORE INTO " + databasePlayerValues + ".PlayerValuations VALUES ( " +
								 GameIds.get( i ) + "," +
								 eventNumbers.get( j ) + "," +
								 playerIds.get( j ) + "," +
								 fromNodeIds.get( j ) + "," +
								 toNodeIds.get( j ) + "," +
								 "\"" + eventTypes.get( j ) + "\"," +
								 actionValues.get( j ) + " );" );
					
					if ( ( ( teamInvolved.get( j ).equalsIgnoreCase( "HOME" ) ) && 
						 ( manpowerDifferential.get( j ) > 0 ) ) ||
						 ( ( teamInvolved.get( j ).equalsIgnoreCase( "AWAY" ) ) &&
						 ( manpowerDifferential.get( j ) < 0 ) ) )
					{
						st1.execute( "INSERT IGNORE INTO " + databasePlayerValues + ".PowerPlayPlayerValuations VALUES ( " +
								 GameIds.get( i ) + "," +
								 eventNumbers.get( j ) + "," +
								 playerIds.get( j ) + "," +
								 fromNodeIds.get( j ) + "," +
								 toNodeIds.get( j ) + "," +
								 "\"" + eventTypes.get( j ) + "\"," +
								 actionValues.get( j ) + " );" );
					}
					else if ( ( ( teamInvolved.get( j ).equalsIgnoreCase( "HOME" ) ) && 
							 ( manpowerDifferential.get( j ) < 0 ) ) ||
							 ( ( teamInvolved.get( j ).equalsIgnoreCase( "AWAY" ) ) &&
							 ( manpowerDifferential.get( j ) > 0 ) ) )
					{
						st1.execute( "INSERT IGNORE INTO " + databasePlayerValues + ".PenaltyKillPlayerValuations VALUES ( " +
									 GameIds.get( i ) + "," +
									 eventNumbers.get( j ) + "," +
									 playerIds.get( j ) + "," +
									 fromNodeIds.get( j ) + "," +
									 toNodeIds.get( j ) + "," +
									 "\"" + eventTypes.get( j ) + "\"," +
									 actionValues.get( j ) + " );" );
					}
				}
			}
			
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to get player action values." );
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	public static int disconnectFromDatabase()
	{
		try
		{
			con.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to close database connection." );
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	public static int retrievePlayerId( String eventType, int eventId, int gameId )
	{
		int playerId = 0;
		Statement st2 = null;
		ResultSet rs2 = null;
		
		switch ( eventType )
		{
		case "BLOCKED SHOT":
			
			try {
				st2 = con.createStatement();
				
				rs2 = st2.executeQuery( "SELECT PlayerId FROM " + databaseData + 
												  ".event_blocked_shot WHERE GameId = " + gameId + " AND BlockId = " + eventId + ";" );
				
				rs2.first();
				
				playerId = rs2.getInt( "PlayerId" );
				
				rs2.close();
				
				st2.close();
			} 
			catch (SQLException e)
			{
				System.out.println( "Failed to get player id for blocked shot." );
				e.printStackTrace();
				return -1;
			}
			
			break;
		case "FACEOFF":
			try
			{
				st2 = con.createStatement();
				
				rs2 = st2.executeQuery( "SELECT * FROM " + databaseData + ".event_faceoff WHERE GameId = " + gameId + " AND FaceoffId = " + eventId + ";" );
				
				rs2.first();
				
				int team_winning_id = rs2.getInt( "FaceoffWinningTeamId" );
				if ( team_winning_id == rs2.getInt( "AwayTeamId" ) )
				{
					playerId = rs2.getInt( "AwayPlayerId" );
				}
				else if ( team_winning_id == rs2.getInt( "HomeTeamId" ) )
				{
					playerId = rs2.getInt( "HomePlayerId" );
				}
				
				rs2.close();
				
				st2.close();
				
			}
			catch ( SQLException e )
			{
				System.out.println( "Failed to get player id for faceoff." );
				e.printStackTrace();
				return -2;
			}
			break;
		case "GIVEAWAY":
			try
			{
				st2 = con.createStatement();
				
				rs2 = st2.executeQuery( "SELECT PlayerId FROM " + databaseData + ".event_giveaway WHERE GameId = " + gameId + " AND GiveawayId = " + eventId + ";" );
				
				rs2.first();
				
				playerId = rs2.getInt( "PlayerId" );
				
				rs2.close();
				
				st2.close();
			}
			catch ( SQLException e )
			{
				System.out.println( "Failed to get player id for giveaway." );
				e.printStackTrace();
				return -3;
			}
			break;
		case "GOAL":
			try
			{
				st2 = con.createStatement();
				
				rs2 = st2.executeQuery( "SELECT GoalScorerId FROM " + databaseData + ".event_goal WHERE GameId = " + gameId + " AND GoalId = " + eventId + ";" );
				
				rs2.first();
				
				playerId = rs2.getInt( "GoalScorerId" );
				
				rs2.close();
				
				st2.close();
			}
			catch ( SQLException e )
			{
				System.out.println( "Failed to get player id for goal." );
				e.printStackTrace();
				return -4;
			}
			break;
		case "HIT":
			try
			{
				st2 = con.createStatement();
				
				rs2 = st2.executeQuery( "SELECT PlayerId FROM " + databaseData + ".event_hit WHERE GameId = " + gameId + " AND HitId = " + eventId + ";" );
				
				rs2.first();
				
				playerId = rs2.getInt( "PlayerId" );
				
				rs2.close();
				
				st2.close();
			}
			catch ( SQLException e )
			{
				System.out.println( "Failed to get player id for hit." );
				e.printStackTrace();
				return -5;
			}
			break;
		case "MISSED SHOT":
			try
			{
				st2 = con.createStatement();
				
				rs2 = st2.executeQuery( "SELECT PlayerId FROM " + databaseData + ".event_missed_shot WHERE GameId = " + gameId + " AND MissId = " + eventId + ";" );
				
				rs2.first();
				
				playerId = rs2.getInt( "PlayerId" );
				
				rs2.close();
				
				st2.close();
			}
			catch ( SQLException e )
			{
				System.out.println( "Failed to get player id for missed shot." );
				e.printStackTrace();
				return -6;
			}
			break;
		case "PENALTY":
			try
			{
				st2 = con.createStatement();
				
				rs2 = st2.executeQuery( "SELECT PlayerId FROM " + databaseData + ".event_penalty WHERE GameId = " + gameId + " AND PenaltyId = " + eventId + ";" );
				
				rs2.first();
				
				playerId = rs2.getInt( "PlayerId" );
				
				rs2.close();
				
				st2.close();
			}
			catch ( SQLException e )
			{
				System.out.println( "Failed to get player id for penalty." );
				e.printStackTrace();
				return -7;
			}
			break;
		case "SHOT":
			try
			{
				st2 = con.createStatement();
				
				rs2 = st2.executeQuery( "SELECT ShootingPlayerId FROM " + databaseData + ".event_shot WHERE GameId = " + gameId + " AND ShotId = " + eventId + ";" );
				
				if ( !rs2.first() )
				{
					playerId = 0;
				}
				else
				{
					playerId = rs2.getInt( "ShootingPlayerId" );
				}
				
				rs2.close();
				
				st2.close();
			}
			catch ( SQLException e )
			{
				System.out.println( "Failed to get player id for shot." );
				e.printStackTrace();
				return -8;
			}
			break;
		case "TAKEAWAY":
			try
			{
				st2 = con.createStatement();
				
				rs2 = st2.executeQuery( "SELECT PlayerId FROM " + databaseData + ".event_takeaway WHERE GameId = " + gameId + " AND TakeawayId = " + eventId + ";" );
				
				rs2.first();
				
				playerId = rs2.getInt( "PlayerId" );
				
				rs2.close();
				
				st2.close();
			}
			catch ( SQLException e )
			{
				System.out.println( "Failed to get player id for takeaway." );
				e.printStackTrace();
				return -9;
			}
			break;
		default:
			break;
		}
		
		return playerId;
	}
}
