import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.mysql.jdbc.Connection;

public class BuildTreeNoManDiff
{
	private static NodeNoManDiff currentNode = null;
	private static NodeNoManDiff root = null;
	private static NodeNoManDiff previousNode = null;
	private static Connection con = null;
	private static int homeGoal = 0;
	private static int awayGoal = 0;
	private static int numNodes = 1;
	private static int numEvents = 1;
	private static boolean homeWin = false;
	private static ArrayList<NodeNoManDiff> leaves = null;
	private static boolean addedLeafLast = false;
	private static boolean evaluateWins = false;
	private static boolean probability = true;
	private static boolean homeFocus = true;
	private static boolean evaluateAll = true;
	private static File f = null;
	private static FileWriter fw = null;
	private static String data_db = "NHL_Final";
	private static String tree_db = "NHL_Final_Sequence_Tree_No_Manpower";
	
	public static void main(String[] args)
	{
		long t1 = System.currentTimeMillis();
		root = new NodeNoManDiff();
		root.SetType( "Root" );
		root.SetNodeId( numNodes );
		
		f = new File( "BuildingTreeConvergenceNoManDiff.csv" );
		try
		{
			fw = new FileWriter( f );
			fw.write( "NumEvents, NumNodes\n" );
		}
		catch (IOException e)
		{
			System.out.println( "Failed to open file for writing." );
			e.printStackTrace();
			return;
		}
		
		ArrayList<Integer> GameIds = GetGameIds();
		if ( GameIds == null )
		{
			System.out.println( "Failed to get GameIds" );
			return;
		}
		
		int len = GameIds.size();
		
		leaves = new ArrayList<NodeNoManDiff>();
		
		if ( prepareNodeTable() != 0 )
		{
			System.out.println( "Failed to prepare node tables." );
			return;
		}
		
		if ( writeNodeToTable( root ) != 0 )
		{
			System.out.println( "Failed to write root to table." );
			return;
		}
		
		for ( int i = 0; i < len; i++ )
		{
			currentNode = root;
			previousNode = null;
			addedLeafLast = false;
			homeGoal = 0;
			awayGoal = 0;
			int gameId = GameIds.get( i );
			if ( SetWin( gameId ) != 0 )
			{
				System.out.println( "Could not set win." );
				return;
			}
			
			if ( AddEventsToTree( gameId ) != 0 )
			{
				System.out.println( "Failed to add events to tree for GameId = " + GameIds.get( i ) );
				return;
			}
			
			root.MarkUnvisited();
		}
		
		//root.PrintNode(0);
		System.out.println("NumEvents = " + numEvents + ", NumNodes = " + numNodes );
		long t2 = System.currentTimeMillis();
		System.out.println( "Total runtime: " + ((t2-t1)/1000.0) + "seconds" );
		
		try
		{
			fw.write(numEvents + "," + numNodes + "\n");
			fw.close();
		}
		catch (IOException e)
		{
			System.out.println( "Failed to close file." );
			e.printStackTrace();
			return;
		}
	}
	
	private static ArrayList<Integer> GetGameIds()
	{
		ArrayList<Integer> GameIds = new ArrayList<Integer>();
		
		String CONN_STR = "jdbc:mysql://127.0.0.1/";
		try 
		{
			java.lang.Class.forName( "com.mysql.jdbc.Driver" );
		} 
		catch ( Exception ex ) 
		{
			System.out.println( "Unable to load MySQL JDBC driver" );
			return null;
		}
		
		try
		{
			con = (Connection) DriverManager.getConnection( CONN_STR, 
					"root", 
					"root" );
			Statement st1 = con.createStatement();
			st1.execute( "USE " + data_db + ";" );
			ResultSet rs1 = st1.executeQuery( "SELECT DISTINCT GameId FROM Play_By_Play_Events WHERE GameId >= 2007020001;" );
			while ( rs1.next() )
			{
				GameIds.add( rs1.getInt( "GameId" ) );
			}
			rs1.close();
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to get GameIds" );
			e.printStackTrace();
			return null;
		}
		return GameIds;
	}
	
	private static int AddEventsToTree(int gameId)
	{
		try
		{
			Statement st1 = con.createStatement();
			ResultSet rs1 = st1.executeQuery( "SELECT * FROM Play_By_Play_Events " +
											  "WHERE GameId = " + gameId + " " +
											  "ORDER BY EventNumber ASC;" );
			while ( rs1.next() )
			{
				System.out.println( "NumEvents = " + numEvents + ", NumNodes = " + numNodes );
				fw.write(numEvents + "," + numNodes + "\n");
				
				//Get context
				//HomeMan
				//int HomeMan = getNumPlayers( rs1, true );
				//if ( HomeMan < 0 )
				//{
				//	System.out.println( "ERROR!" );
				//	return -2;
				//}
				
				//AwayMan
				//int AwayMan = getNumPlayers( rs1, false );
				//if ( AwayMan < 0 )
				//{
				//	System.out.println( "ERROR!" );
				//	return -3;
				//}
				
				//ManDiff
				//int ManDiff = HomeMan - AwayMan;
				
				//HomeGoal
				//AwayGoal
				
				//Period
				int periodNumber = rs1.getInt( "PeriodNumber" );
				
				//Event
				String eventType = rs1.getString( "EventType" );
				
				//ExternalEventId
				int externalEventId = rs1.getInt( "ExternalEventId" );
				
				//EventNumber
				int eventNumber = rs1.getInt( "EventNumber" );
				
				Statement st3 = con.createStatement();
				
				String TableName = "";
				String ExternalIdName = "";
				String TeamId = "";
				
				switch ( eventType )
				{
				case "FACEOFF" :
					TeamId = "FaceoffWinningTeamId";
					TableName = "event_faceoff";
					ExternalIdName = "FaceoffId";
					break;
				case "HIT" :
					TeamId = "HittingTeamId";
					TableName = "Event_Hit";
					ExternalIdName = "HitId";
					break;
				case "GOAL" :
					TeamId = "ScoringTeamId";
					TableName = "Event_Goal";
					ExternalIdName = "GoalId";
					break;
				case "SHOT" :
					TeamId = "ShotByTeamId";
					TableName = "Event_Shot";
					ExternalIdName = "ShotId";
					break;
				case "MISSED SHOT" :
					TeamId = "MissTeamId";
					TableName = "Event_Missed_Shot";
					ExternalIdName = "MissId";
					break;
				case "BLOCKED SHOT" :
					TeamId = "BlockTeamId";
					TableName = "Event_Blocked_Shot";
					ExternalIdName = "BlockId";
					break;
				case "GIVEAWAY" :
					TeamId = "GiveawayTeamId";
					TableName = "Event_Giveaway";
					ExternalIdName = "GiveawayId";
					break;
				case "TAKEAWAY" :
					TeamId = "TakeawayTeamId";
					TableName = "Event_Takeaway";
					ExternalIdName = "TakeawayId";
					break;
				case "PENALTY" :
					TeamId = "TeamPenaltyId";
					TableName = "event_penalty";
					ExternalIdName = "PenaltyId";
					break;
				default :
					break;
				}
				
				String Zone = "";
				
				if ( !TeamId.equalsIgnoreCase("") )
				{
					ResultSet rs3 = st3.executeQuery("SELECT AwayTeamId, " + TeamId + ", Zone " +
							  "FROM " + TableName + " " +
							  "WHERE " + ExternalIdName + " = " + externalEventId + ";");
					if ( rs3.first() )
					{
						if ( rs3.getInt( "AwayTeamId" ) == rs3.getInt( TeamId ) )
						{
							eventType = "AWAY:" + eventType;
						}
						else
						{
							eventType = "HOME:" + eventType;
						}
						
						Zone = rs3.getString("Zone");
						if ( null == Zone )
						{
							Zone = "Unspecified";
						}
						
						
					}
					rs3.close();
				}
				
				st3.close();
				
				NodeNoManDiff newNode = new NodeNoManDiff();
				newNode.SetType("Event");
				//newNode.SetAwayMan( AwayMan );
				//newNode.SetHomeMan( HomeMan );
				//newNode.SetManDiff( ManDiff );
				newNode.SetHomeGoals(homeGoal);
				newNode.SetAwayGoals(awayGoal);
				newNode.SetGoalDiff(homeGoal - awayGoal);
				newNode.SetPeriod( periodNumber );
				newNode.SetName(eventType);
				newNode.SetZone(Zone);
				
				//If Child, go to child
				if ( currentNode.CompareNode(root) )
				{
					if ( incrementNodeTableCount( root.GetNodeId() ) != 0 )
					{
						System.out.println( "Failed to increment root count." );
						return -7;
					}
					
					if ( !root.HasBeenVisited() )
					{
						root.SetVisited( true );
						root.IncrementGameCount();
						if ( homeWin )
						{
							root.IncrementHomeWinCount();
						}
						else
						{
							root.IncrementAwayWinCount();
						}
						
						if ( UpdateGameWinCounts( root ) != 0 )
						{
							System.out.println( "Failed to adjust game win count." );
							return -10;
						}
					}
					
					newNode.SetType("State");
					newNode.SetName("");
					newNode.SetZone("");
					NodeNoManDiff lookingAt = currentNode.FindChildNode(newNode);
					
					if ( lookingAt == null )
					{
						numNodes += 1;
						newNode.SetNodeId( numNodes );
						currentNode.AddChild( newNode );
						newNode.SetParent( currentNode );
						
						if ( addedLeafLast )
						{
							previousNode.AddChild( newNode );
							if ( writeEdgeToTable( previousNode, newNode ) != 0 )
							{
								System.out.println( "Failed to write edge to table." );
								return -3;
							}
							if ( incrementEdgeCount( previousNode.GetNodeId(), newNode.GetNodeId() ) != 0 )
							{
								System.out.println( "Failed to increment edge count." );
								return -5;
							}
							addedLeafLast = false;
						}
						previousNode = currentNode;
						currentNode = newNode;
						if ( writeNodeToTable( currentNode ) != 0 )
						{
							System.out.println( "Failed to write node to table." );
							return -2;
						}
						if ( writeEdgeToTable( previousNode, currentNode ) != 0 )
						{
							System.out.println( "Failed to write edge to table." );
							return -3;
						}
						
						newNode = new NodeNoManDiff();
						newNode.SetType("Event");
						//newNode.SetAwayMan( AwayMan );
						//newNode.SetHomeMan( HomeMan );
						//newNode.SetManDiff( ManDiff );
						newNode.SetHomeGoals(homeGoal);
						newNode.SetAwayGoals(awayGoal);
						newNode.SetGoalDiff(homeGoal - awayGoal);
						newNode.SetPeriod( periodNumber );
						newNode.SetName(eventType);
						newNode.SetZone( Zone );
					}
					else
					{
						if ( addedLeafLast )
						{
							NodeNoManDiff node = previousNode.FindChildNode( lookingAt );
							if ( null == node )
							{
								previousNode.AddChild( lookingAt );
								if ( writeEdgeToTable( previousNode, lookingAt ) != 0 )
								{
									System.out.println( "Failed to write edge to table." );
									return -4;
								}
								if ( incrementEdgeCount( previousNode.GetNodeId(), lookingAt.GetNodeId() ) != 0 )
								{
									System.out.println( "Failed to increment edge count." );
									return -5;
								}
							}
							addedLeafLast = false;
						}
						
						previousNode = currentNode;
						currentNode = lookingAt;
					}
					
					if ( incrementEdgeCount( previousNode.GetNodeId(), currentNode.GetNodeId() ) != 0 )
					{
						System.out.println( "Failed to increment edge count." );
						return -5;
					}
					
					newNode.SetType("Event");
					newNode.SetName(eventType);
					newNode.SetZone(Zone);
					
					currentNode.IncrementCount();
					if ( incrementNodeTableCount( currentNode.GetNodeId() ) != 0 )
					{
						System.out.println( "Failed to increment node count." );
						return -6;
					}
					
					if ( !currentNode.HasBeenVisited() )
					{
						currentNode.SetVisited( true );
						currentNode.IncrementGameCount();
						if ( homeWin )
						{
							currentNode.IncrementHomeWinCount();
						}
						else
						{
							currentNode.IncrementAwayWinCount();
						}
						
						if ( UpdateGameWinCounts( currentNode ) != 0 )
						{
							System.out.println( "Failed to update game win count." );
							return -11;
						}
					}
				}
				
				if ( eventType.endsWith(":GOAL") )
				{
					newNode.SetName(eventType.replace(":GOAL", ":SHOT"));
				}
				
				NodeNoManDiff lookingAt = currentNode.FindChildNode( newNode );
				if ( lookingAt == null )
				{
					numNodes += 1;
					newNode.SetNodeId( numNodes );
					currentNode.AddChild( newNode );
					newNode.SetParent( currentNode );
					previousNode = currentNode;
					currentNode = newNode;
					
					if ( writeNodeToTable( currentNode ) != 0 )
					{
						System.out.println( "Failed to write new node to table." );
						return -8;
					}
					if ( writeEdgeToTable( previousNode, currentNode ) != 0 )
					{
						System.out.println( "Failed to write new edge to table." );
						return -9;
					}
				}
				else
				{
					previousNode = currentNode;
					currentNode = lookingAt;
				}
				
				st3 = con.createStatement();
				st3.execute( "INSERT IGNORE INTO " + tree_db + ".Play_By_Play_Events_With_Node_Information VALUES ( " +
							 gameId + ", " + eventNumber + ", " + previousNode.GetNodeId() + ", " + currentNode.GetNodeId() +
							 " );" );
				st3.close();
				
				if ( incrementEdgeCount( previousNode.GetNodeId(), currentNode.GetNodeId() ) != 0 )
				{
					System.out.println( "Failed to increment edge count." );
					return -10;
				}
				
				currentNode.IncrementCount();
				
				if ( incrementNodeTableCount( currentNode.GetNodeId() ) != 0 )
				{
					System.out.println( "Failed to increment node count." );
					return -11;
				}
				
				if ( !currentNode.HasBeenVisited() )
				{
					currentNode.SetVisited( true );
					currentNode.IncrementGameCount();
					if ( homeWin )
					{
						currentNode.IncrementHomeWinCount();
					}
					else
					{
						currentNode.IncrementAwayWinCount();
					}
					
					if ( UpdateGameWinCounts( currentNode ) != 0 )
					{
						System.out.println( "Failed to update game win count." );
						return -12;
					}
				}
				
				if ( eventType.endsWith(":GOAL") )
				{
					newNode = new NodeNoManDiff();
					newNode.SetType("Event");
					//newNode.SetAwayMan( AwayMan );
					//newNode.SetHomeMan( HomeMan );
					//newNode.SetManDiff( ManDiff );
					newNode.SetHomeGoals(homeGoal);
					newNode.SetAwayGoals(awayGoal);
					newNode.SetGoalDiff(homeGoal - awayGoal);
					newNode.SetPeriod( periodNumber );
					newNode.SetName(eventType);
					newNode.SetZone(Zone);
					lookingAt = currentNode.FindChildNode( newNode );
					if ( lookingAt == null )
					{
						numNodes += 1;
						newNode.SetNodeId( numNodes );
						currentNode.AddChild( newNode );
						newNode.SetParent( currentNode );
						previousNode = currentNode;
						currentNode = newNode;
						if ( writeNodeToTable( currentNode ) != 0 )
						{
							System.out.println( "Failed to write new node to table." );
							return -8;
						}
						if ( writeEdgeToTable( previousNode, currentNode ) != 0 )
						{
							System.out.println( "Failed to write new edge to table." );
							return -9;
						}
					}
					else
					{
						previousNode = currentNode;
						currentNode = lookingAt;
					}
					
					if ( incrementEdgeCount( previousNode.GetNodeId(), currentNode.GetNodeId() ) != 0 )
					{
						System.out.println( "Failed to increment edge count." );
						return -10;
					}
					
					currentNode.IncrementCount();
					
					if ( incrementNodeTableCount( currentNode.GetNodeId() ) != 0 )
					{
						System.out.println( "Failed to increment node count." );
						return -11;
					}
					
					if ( !currentNode.HasBeenVisited() )
					{
						currentNode.SetVisited( true );
						currentNode.IncrementGameCount();
						if ( homeWin )
						{
							currentNode.IncrementHomeWinCount();
						}
						else
						{
							currentNode.IncrementAwayWinCount();
						}
						
						if ( UpdateGameWinCounts( currentNode ) != 0 )
						{
							System.out.println( "Failed to update game win count." );
							return -12;
						}
					}
				}
				
				if ( eventType.equalsIgnoreCase("STOPPAGE") ||
					 eventType.contains("GOAL") ||
					 eventType.equalsIgnoreCase("SHOOTOUT COMPLETED") ||
					 //eventType.equalsIgnoreCase("GAME OFF" ) ||
					 //eventType.equalsIgnoreCase("GAME END" ) ||
					 eventType.contains("PENALTY" ) ||
					 eventType.equalsIgnoreCase( "EARLY INTERMISSION END" ) ||
					 eventType.equalsIgnoreCase( "PERIOD END" ) )
				{
					int numLeaves = leaves.size();
					boolean inLeaves = false;
					for ( int i = 0; i < numLeaves; i++ )
					{
						if ( leaves.get( i ).CompareNode( currentNode ) )
						{
							inLeaves = true;
						}
					}
					
					if ( !inLeaves )
					{
						leaves.add( currentNode );
					}
					addedLeafLast = true;
					
					previousNode = currentNode;
					//Need to add new state as child of leaf
					currentNode = root;
					
					if ( eventType.contains("GOAL") )
					{
						Statement st2 = con.createStatement();
						int goalId = rs1.getInt("ExternalEventId");
						ResultSet rs2 = st2.executeQuery( "SELECT * FROM event_goal " + 
														  "WHERE GoalId = " + goalId + ";" );
						if ( !rs2.first() )
						{
							System.out.println( "Didn't know which team to add to...");
						}
						else if ( rs2.getInt("AwayTeamId" ) == rs2.getInt("ScoringTeamId") )
						{
							awayGoal += 1;
						}
						else
						{
							homeGoal += 1;
						}
						rs2.close();
						st2.close();
					}
				}
					
				numEvents += 1;
			}
			rs1.close();
			
			NodeNoManDiff newNode = new NodeNoManDiff();
			newNode.SetType("Terminal");
			if ( homeWin )
			{
				newNode.SetName("HOME:WIN");
			}
			else
			{
				newNode.SetName("AWAY:WIN");
			}
			
			NodeNoManDiff lookingAt = previousNode.FindChildNode( newNode );
			if ( null == lookingAt )
			{
				numNodes += 1;
				newNode.SetNodeId( numNodes );
				previousNode.AddChild( newNode );
				newNode.SetParent( previousNode );
				if ( writeNodeToTable( newNode ) != 0 )
				{
					System.out.println( "Failed to write node to table." );
					return -13;
				}
				if ( writeEdgeToTable( previousNode, newNode ) != 0 )
				{
					System.out.println( "Failed to write edge to table." );
					return -14;
				}
				
				currentNode = newNode;
			}
			else
			{
				currentNode = lookingAt;
			}
			
			if ( incrementEdgeCount( previousNode.GetNodeId(), currentNode.GetNodeId() ) != 0 )
			{
				System.out.println( "Failed to increment edge count." );
				return -10;
			}
			
			currentNode.IncrementCount();
			
			if ( incrementNodeTableCount( currentNode.GetNodeId() ) != 0 )
			{
				System.out.println( "Failed to increment node count." );
				return -11;
			}
			
			if ( !currentNode.HasBeenVisited() )
			{
				currentNode.SetVisited( true );
				currentNode.IncrementGameCount();
				if ( homeWin )
				{
					currentNode.IncrementHomeWinCount();
				}
				else
				{
					currentNode.IncrementAwayWinCount();
				}
				
				if ( UpdateGameWinCounts( currentNode ) != 0 )
				{
					System.out.println( "Failed to update game win count." );
					return -12;
				}
			}
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to add events to tree." );
			e.printStackTrace();
			return -1;
		}
		catch ( IOException e )
		{
			System.out.println( "Failed to write convergence file." );
			e.printStackTrace();
			return -2;
		}	
		return 0;
	}
	
	private static int getNumPlayers( ResultSet rs, boolean home )
	{
		int count = 0;
		
		if ( rs == null )
		{
			System.out.println( "Given empty result set." );
			return -1;
		}
		
		String query = "SELECT COUNT(*) AS NumSkaters FROM Skater WHERE ";
		
		int numPlayers = 0;
		for ( int i = 1; i <= 8; i++ )
		{
			int playerId = 0;
			if ( home )
			{
				try
				{
					playerId = rs.getInt( "HomePlayer" + i );
				}
				catch ( SQLException e )
				{
					System.out.println( "Failed to get HomePlayer" + i );
					e.printStackTrace();
					return -2;
				}
			}
			else
			{
				try
				{
					playerId = rs.getInt( "AwayPlayer" + i );
				}
				catch ( SQLException e )
				{
					System.out.println( "Failed to get AwayPlayer" + i );
					e.printStackTrace();
					return -3;
				}
			}
			if ( playerId != 0 )
			{
				numPlayers += 1;
				if ( i == 1 )
				{
					query += " SkaterId = " + playerId;
					continue;
				}
				
				query += " OR SkaterId = " + playerId;
			}
		}
		
		if ( numPlayers == 0 )
		{
			return 0;
		}
		
		try
		{
			Statement st1 = con.createStatement();
			ResultSet rs1 = st1.executeQuery( query + ";" );
			if ( !rs1.first() )
			{
				System.out.println( "ResultSet is empty." );
				return -4;
			}
			
			count = rs1.getInt( "NumSkaters" );
			
			rs1.close();
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Couldn't get count." );
			e.printStackTrace();
			return -5;
		}
		
		return count;
	}
	
	private static int writeNodeToTable( NodeNoManDiff node )
	{
		try
		{
			Statement st1 = con.createStatement();
			int nodeId = node.GetNodeId();
			String type = node.GetType();
			if ( type == null )
			{
				type = "\\N";
			}
			String name = node.GetName();
			if ( name == null )
			{
				name = "\\N";
			}
			int goalDiff = node.GetGoalDiff();
			//int manDiff = node.GetManDiff();
			int period = node.GetPeriod();
			String zone = node.GetZone();
			if ( null == zone )
			{
				zone = "UNSPECIFIED";
			}
			int occurrences = 0;
			double reward_expected_goals = 0.0;
			double reward_probability_home_goal = 0.0;
			double reward_probability_away_goal = 0.0;
			double reward_expected_win = 0.0;
			double reward_probability_home_win = 0.0;
			double reward_probability_away_win = 0.0;
			
			if ( name.endsWith("GOAL") )
			{
				if ( name.contains( "HOME" ) )
				{
					reward_expected_goals = 1.0;
					reward_probability_home_goal = 1.0;
				}
				else
				{
					reward_expected_goals = -1.0;
					reward_probability_away_goal = 1.0;
				}
			}
			else if ( name.contains( "WIN" ) )
			{
				if ( name.contains( "HOME" ) )
				{
					reward_expected_win = 1.0;
					reward_probability_home_win = 1.0;
				}
				else
				{
					reward_expected_win = -1.0;
					reward_probability_away_win = 1.0;
				}
			}
			
	
			int gameCount = node.GetGameCount();
			int homeWinCount = node.GetHomeWinCount();
			int awayWinCount = node.GetAwayWinCount();
			st1.execute( "INSERT IGNORE INTO NHL_Final_Sequence_Tree_No_Manpower.Nodes VALUES ( " +
						 nodeId + ", " +
					 "\"" + type + "\", " +
					 "\"" + name + "\", " +
					 ( goalDiff ) + ", " +
					 //manDiff + ", " +
					 period + ", " +
					 "\"" + zone.replace("\"", "") + "\", "+
					 occurrences + ", " +
					 reward_expected_goals + ", " +
					 reward_probability_home_goal + ", " +
					 reward_probability_away_goal + ", " +
					 reward_expected_win + ", " +
					 reward_probability_home_win + ", " +
					 reward_probability_away_win + ", " +
					 "0.0," + 
					 gameCount + ", " +
					 homeWinCount + ", " +
					 awayWinCount + " );" );
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to write Node to table." );
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	private static int incrementNodeTableCount( int nodeId )
	{
		try
		{
			Statement st1 = con.createStatement();
			st1.execute( "UPDATE NHL_Final_Sequence_Tree_No_Manpower.Nodes " + 
						 "SET Occurrences = Occurrences + 1 " +
						 "WHERE NodeId = " + nodeId + ";" );
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to increment count." );
			return -1;
		}
		return 0;
	}
	
	private static int writeEdgeToTable( NodeNoManDiff fromNode, NodeNoManDiff toNode )
	{
		try
		{
			Statement st1 = con.createStatement();
			int fromNodeId = fromNode.GetNodeId();
			int toNodeId = toNode.GetNodeId();
			int occurrences = 0;
			double qvalue = 0.0;
			st1.execute( "INSERT IGNORE INTO NHL_Final_Sequence_Tree_No_Manpower.Edges VALUES ( " +
						 fromNodeId + ", " +
						 toNodeId + ", " +
						 occurrences + ", " +
						 qvalue + " );" );
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to write edge to table." );
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	private static int incrementEdgeCount( int nodeId1, int nodeId2 )
	{
		try
		{
			Statement st1 = con.createStatement();
			st1.execute( "UPDATE NHL_Final_Sequence_Tree_No_Manpower.Edges " +
						 "SET Occurrences = Occurrences + 1 " +
						 "WHERE FromNodeId = " + nodeId1 + " " +
						 "AND ToNodeId = " + nodeId2 + ";" ); 
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to increase edge count." );
			return -1;
		}
		return 0;
	}
	
	private static int UpdateGameWinCounts( NodeNoManDiff node )
	{
		try
		{
			Statement st1 = con.createStatement();
			st1.execute( "UPDATE NHL_Final_Sequence_Tree_No_Manpower.Nodes " +
						 "SET GameCount = " + node.GetGameCount() + 
						 ", HomeWinCount = " + node.GetHomeWinCount() + 
						 ", AwayWinCount = " + node.GetAwayWinCount() + 
						 " WHERE NodeId = " + node.GetNodeId() + ";" ); 
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to update game & win counts." );
			return -1;
		}
		return 0;
	}
	
	private static int prepareNodeTable()
	{
		try
		{
			Statement st1 = con.createStatement();
			st1.execute( "DROP SCHEMA IF EXISTS " + tree_db + ";" );
			st1.execute( "CREATE SCHEMA " + tree_db + ";" );
			st1.execute( "CREATE TABLE " + tree_db + ".Nodes ( " +
							 "NodeId INT, " +
							 "NodeType TEXT, " +
							 "NodeName TEXT, " +
							 "GoalDifferential INT, " +
							 //"ManpowerDifferential INT, " +
							 "Period INT, " +
							 "Zone TEXT, " +
							 "Occurrences INT, " +
							 "Reward_Expected_Goals DOUBLE, " +
							 "Reward_Probability_Home_Goal DOUBLE, " +
							 "Reward_Probability_Away_Goal DOUBLE, " +
							 "Reward_Expected_Win DOUBLE, " +
							 "Reward_Probability_Home_Win DOUBLE, " +
							 "Reward_Probability_Away_Win DOUBLE, " +
							 "VStar DOUBLE, " + 
							 "GameCount INT, " + 
							 "HomeWinCount INT, " +
							 "AwayWinCount INT );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Nodes ADD INDEX ( NodeId );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Nodes ADD INDEX ( NodeId, Occurrences );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Nodes ADD INDEX ( NodeId, Reward_Expected_Goals );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Nodes ADD INDEX ( NodeId, Reward_Probability_Home_Goal );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Nodes ADD INDEX ( NodeId, Reward_Probability_Away_Goal );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Nodes ADD INDEX ( NodeId, Reward_Expected_Win );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Nodes ADD INDEX ( NodeId, Reward_Probability_Home_Win );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Nodes ADD INDEX ( NodeId, Reward_Probability_Away_Win );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Nodes ADD INDEX ( NodeId, Occurrences, Reward_Expected_Goals, Reward_Probability_Home_Goal, " + 
						 "Reward_Probability_Away_Goal, Reward_Expected_Win, Reward_Probability_Home_Win, Reward_Probability_Away_Win );" );
			st1.execute( "CREATE TABLE " + tree_db + ".Edges ( " +
						 "FromNodeId INT, " +
						 "ToNodeId INT, " +
						 "Occurrences INT, " + 
						 "QValue DOUBLE );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Edges ADD INDEX ( FromNodeId );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Edges ADD INDEX ( ToNodeId );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Edges ADD INDEX ( FromNodeId, ToNodeId, Occurrences );" );
			
			st1.execute( "CREATE TABLE " + tree_db + ".Play_By_Play_Events_With_Node_Information( " +
					 "GameId INT, " +
					 "EventNumber INT, " +
					 "StartingNodeId INT, " +
					 "EndingNodeId INT );" );
			st1.execute( "ALTER TABLE " + tree_db + ".Play_By_Play_Events_With_Node_Information ADD INDEX ( GameId, EventNumber, StartingNodeId, EndingNodeId );" );
			st1.close();
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			return -1;
		}
		
		return 0;
	}
	
	private static int SetWin( int gameId )
	{
		try
		{
			Statement st1 = con.createStatement();
			ResultSet rs1 = st1.executeQuery( "SELECT Result " + 
											  "FROM Plays_In " +
											  "WHERE GameId = " + gameId + " " +
											  "AND Venue = 'Home';" );
			if ( !rs1.first() )
			{
				System.out.println( "Could not retrieve game!" );
				homeWin = false;
				return -2;
			}
			
			if ( rs1.getInt( "Result" ) == 1 )
			{
				homeWin = true;
			}
			else
			{
				homeWin = false;
			}
			rs1.close();
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to set win flag;" );
			e.printStackTrace();
			return -1;
		}
		
		return 0;
	}
}
