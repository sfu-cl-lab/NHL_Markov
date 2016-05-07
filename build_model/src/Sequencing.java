import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;

import com.mysql.jdbc.Connection;

public class Sequencing
{
	public static String dbaddress = "mysql://127.0.0.1";
	private static Connection con;
	
	public static void main(String[] args)
	{
		//Connect to database
		connectDB();
		//Create Sequence Table
		Statement st1 = null;
		try {
			st1 = con.createStatement();
			st1.execute( "USE NHL_Final;" );
			st1.execute( "DROP TABLE IF EXISTS Sequences_Full;" );
			st1.execute( "CREATE TABLE IF NOT EXISTS Sequences_Full ( " + 
						 "GameId INT, " +
					     "SequenceNum INT, " +
					     "Sequence TEXT, " +
					     "TimeLabels TEXT, " + 
					     "PeriodNumber INT, " +
						 "StartingEventNum INT, " +
					     "EndingEventNum INT, " +
					     "SequenceLength INT, " +
					     "HomeGoals INT, " + 
					     "AwayGoals INT, " + 
					     "GoalDiff INT, " +
					     "HomeMan INT, " + 
					     "AwayMan INT, " + 
					     "ManDiff INT, " + 
					     "StartingTime TIME, " +
					     "EndingTime TIME, " +
					     "Duration TIME, " + 
					     "SequenceEndsInGoal INT, " + 
					     "SequenceEndsInHomeGoal INT, " +
					     "SequenceEndsInAwayGoal INT, " +
					     "SequenceEndsInPenalty INT, " +
					     "SequenceEndsInHomePenalty INT, " +
					     "SequenceEndsInAwayPenalty INT, " +
					     "SequenceInHomeWin INT, " +
					     "SequenceInAwayWin INT );" );
			st1.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println( "Failed to create table." );
			return;
		}
		
		//Get GameIds
		try
		{
			st1 = con.createStatement();
			ResultSet rs1 = st1.executeQuery( "SELECT DISTINCT GameId FROM Play_By_Play_Events WHERE GameId >= 2007020001;" );
			//For each GameId
			while ( rs1.next() )
			{
				String sequence = "";
				String timestamps = "";
				int GameId = rs1.getInt( "GameId" );
				System.out.println( "GameId = " + GameId );
				Statement st5 = con.createStatement();
				ResultSet rs5 = st5.executeQuery( "SELECT Venue, Result FROM Plays_In WHERE GameId = " + GameId + ";" );
				if ( rs5.first() == false )
				{
					rs5.close();
					st5.close();
					continue;
				}
				String Venue = rs5.getString( "Venue" );
				int Result = rs5.getInt( "Result" );
				rs5.close();
				st5.close();
				
				int homeWin = 0;
				int awayWin = 0;
				int numGames = 1;
				
				if ( Venue.equalsIgnoreCase( "Away" ) )
				{
					if ( Result == 1 )
					{
						awayWin = 1;
					}
					else
					{
						homeWin = 1;
					}
				}
				else
				{
					if ( Result == 1 )
					{
						homeWin = 1;
					}
					else
					{
						awayWin = 1;
					}
				}
				int SequenceNum = 1;
				int PeriodNumber = 0;
				int SequenceLength = 0;
				int StartingEvent = 0;
				int EndingEvent = 0;
				int HomeGoals = 0;
				int AwayGoals = 0;
				int HomeMan = 0;
				int AwayMan = 0;
				int EndsInGoal = 0;
				int EndsInPenalty = 0;
				int EndsInHomeGoal = 0;
				int EndsInAwayGoal = 0;
				int EndsInHomePenalty = 0;
				int EndsInAwayPenalty = 0;
				String Team = "";
				boolean first = true;
				Time startingTime = null;
				Time endingTime = null;
				//Get ordered events
				Statement st2 = con.createStatement();
				ResultSet rs2 = st2.executeQuery( "SELECT EventNumber, PeriodNumber, EventTime, EventType, ExternalEventId " +
												  "FROM Play_By_Play_Events " +
												  "WHERE GameId = " + GameId + " " +
												  "ORDER BY EventNumber ASC;" );
				//Create Sequence String
				//Write to Sequence Table
				while ( rs2.next() )
				{
					int eventAwayMan = 0;
					int eventHomeMan = 0;
					
					Statement st3 = con.createStatement();
					String QueryString = "SELECT ";
					
					String eventTime = rs2.getTime( "EventTime" ).toString();
					if ( !( timestamps.isEmpty() ) )
					{
						timestamps += ", ";
					}
					timestamps += eventTime;
					
					for ( int i = 1; i <= 9; i++ )
					{
						QueryString += "AwayPlayer" + i + ", HomePlayer" + i;
						if ( i != 9 )
						{
							QueryString += ", ";
						}
					}
					QueryString += " FROM Play_By_Play_Events WHERE GameId = " + GameId + " AND EventNumber = " + rs2.getInt("EventNumber") + ";";
					ResultSet rs3 = st3.executeQuery( QueryString );
					while ( rs3.next() )
					{
						String awayQuery = "SELECT COUNT(*) AS SkaterCount FROM skater WHERE ";
						String homeQuery = awayQuery;
						for ( int i = 1; i <= 9; i++ )
						{
							if ( i != 1 )
							{
								awayQuery += " OR ";
								homeQuery += " OR ";
							}
							awayQuery += "SkaterId = " + rs3.getInt( "AwayPlayer" + i );
							homeQuery += "SkaterId = " + rs3.getInt( "HomePlayer" + i );
						}
						
						Statement st4 = con.createStatement();
						ResultSet rs4 = st4.executeQuery( awayQuery + ";" );
						if ( rs4.first() )
						{
							eventAwayMan = rs4.getInt( "SkaterCount" );
						}
						rs4.close();
						rs4 = st4.executeQuery( homeQuery + ";" );
						if ( rs4.first() )
						{
							eventHomeMan = rs4.getInt( "SkaterCount" );
						}
						rs4.close();
						st4.close();
					}
					rs3.close();
					st3.close();
					
					if ( first ) 
					{
						StartingEvent = rs2.getInt( "EventNumber" );
						PeriodNumber = rs2.getInt( "PeriodNumber" );
						startingTime = rs2.getTime("EventTime");
						HomeMan = eventHomeMan;
						AwayMan = eventAwayMan;
						first = false;
					}
					
					String eventType = rs2.getString( "EventType" );
					int externalEventId = rs2.getInt("ExternalEventId");
					if ( eventType.equalsIgnoreCase( "GOAL" ) )
					{
						Statement st4 = con.createStatement();
						ResultSet rs4 = st4.executeQuery( "SELECT AwayTeamId, ScoringTeamId, Zone " +
														  "FROM Event_Goal " +
														  "WHERE GoalId = " + externalEventId + ";" );
						String Zone = null;
						
						if ( rs4.first() )
						{
							if ( rs4.getInt("AwayTeamId") == rs4.getInt("ScoringTeamId") )
							{
								Team = "AWAY";
								EndsInAwayGoal = 1;
							}
							else
							{
								Team = "HOME";
								EndsInHomeGoal = 1;
							}
							Zone = rs4.getString("Zone");
							if ( null == Zone )
							{
								Zone = "Unspecified";
							}
							Zone = Zone.toUpperCase().replace("\"", "");
							rs4.close();
							st4.close();
						}
						else
						{
							Team = "UNSPECIFIED";
							Zone = "UNSPECIFIED";
							rs4.close();
							st4.close();
						}
						
						if ( !sequence.equalsIgnoreCase( "" ) )
						{
							sequence += ", ";
						}
						//sequence += "\\'" + eventHomeMan + "v" + eventAwayMan + ":" + Team + ":" + Zone + ":SHOT\\', " + 
						//			"\\'" + eventHomeMan + "v" + eventAwayMan + ":STOPPAGE\\'";
						//sequence += "\\'" + eventHomeMan + "v" + eventAwayMan + ":" + Team + ":" + Zone + ":SHOT\\', " + 
						//"\\'" + eventHomeMan + "v" + eventAwayMan + ":" + Team + ":" + Zone + ":" + eventType + "\\'";
						sequence += "\\'" + Team + ":" + Zone + ":SHOT\\', " + 
								"\\'" + Team + ":" + Zone + ":" + eventType + "\\'";
						SequenceLength += 1;
						EndsInGoal = 1;
						
						if ( !(timestamps.isEmpty() ) )
						{
							timestamps += ", ";
						}
						timestamps += eventTime;
					}
					else if ( eventType.equalsIgnoreCase( "PENALTY" ) )
					{
						if ( !( sequence.equalsIgnoreCase( "" ) ) )
						{
							sequence += ", ";
						}
						String TeamId = "TeamPenaltyId";
						String Table = "event_penalty";
						String ExternalId = "PenaltyId";
						Statement st4 = con.createStatement();
						ResultSet rs4 = st4.executeQuery( "SELECT AwayTeamId, " + TeamId + ", Zone " +
														  "FROM " + Table + " " +
														  "WHERE " + ExternalId + " = " + externalEventId + ";" );
						
						String Zone = "";
						if ( rs4.first() == false )
						{
							Team = "UNSPECIFIED";
							Zone = "UNSPECIFIED";
						}
						else
						{
							if ( rs4.getInt("AwayTeamId") == rs4.getInt(TeamId) )
							{
								Team = "AWAY";
								EndsInAwayPenalty = 1;
							}
							else
							{
								Team = "HOME";
								EndsInHomePenalty = 1;
							}
							Zone = rs4.getString("Zone");
							if ( null == Zone )
							{
								Zone = "Unspecified";
							}
						}
						Zone = Zone.toUpperCase().replace("\"","");
						rs4.close();
						st4.close();
						//sequence += "\\'" + eventHomeMan + "v" + eventAwayMan + ":" + "STOPPAGE" + "\\'";
						//sequence += "\\'" + eventHomeMan + "v" + eventAwayMan + ":" + Team + ":" + Zone + ":" + eventType + "\\'";
						sequence += "\\'" +  Team + ":" + Zone + ":" + eventType + "\\'";
						EndsInPenalty = 1;
					}
					else if ( eventType.equalsIgnoreCase( "PERIOD END" ) ||
						 eventType.equalsIgnoreCase( "SHOOTOUT COMPLETED" ) )
					{
						if ( !(sequence.equalsIgnoreCase("")))
						{
							sequence += ", ";
						}
						//sequence += "\\'"+eventHomeMan + "v" + eventAwayMan + ":STOPPAGE\\'";
						//sequence += "\\'"+eventHomeMan + "v" + eventAwayMan + ":" + eventType + "\\'";
						sequence += "\\'" + eventType + "\\'";
					}
					else if ( eventType.equalsIgnoreCase( "FACEOFF" ) ||
							  eventType.equalsIgnoreCase( "HIT" ) ||
							  eventType.equalsIgnoreCase( "GIVEAWAY" ) ||
							  eventType.equalsIgnoreCase( "BLOCKED SHOT" ) ||
							  eventType.equalsIgnoreCase( "SHOT" ) ||
							  eventType.equalsIgnoreCase( "MISSED SHOT" ) ||
							  eventType.equalsIgnoreCase( "TAKEAWAY" ) )
					{
						if ( !( sequence.equalsIgnoreCase( "" ) ) )
						{
							sequence += ", ";
						}
						String TeamId = "";
						String Table = "";
						String ExternalId = "";
						switch ( eventType )
						{
						case "FACEOFF":
							TeamId = "FaceoffWinningTeamId";
							Table = "event_faceoff";
							ExternalId = "FaceoffId";
							break;
						case "HIT":
							TeamId = "HittingTeamId";
							Table = "Event_Hit";
							ExternalId = "HitId";
							break;
						case "GIVEAWAY" :
							TeamId = "GiveawayTeamId";
							Table = "Event_Giveaway";
							ExternalId = "GiveawayId";
							break;
						case "BLOCKED SHOT" :
							TeamId = "BlockTeamId";
							Table = "Event_Blocked_Shot";
							ExternalId = "BlockId";
							break;
						case "SHOT" :
							TeamId = "ShotByTeamId";
							Table = "Event_Shot";
							ExternalId = "ShotId";
							break;
						case "MISSED SHOT" :
							TeamId = "MissTeamId";
							Table = "Event_Missed_Shot";
							ExternalId = "MissId";
							break;
						case "TAKEAWAY" :
							TeamId = "TakeawayTeamId";
							Table = "Event_Takeaway";
							ExternalId = "TakeawayId";
							break;
						default :
							break;
						}
						Statement st4 = con.createStatement();
						ResultSet rs4 = st4.executeQuery( "SELECT AwayTeamId, " + TeamId + ", Zone " +
														  "FROM " + Table + " " +
														  "WHERE " + ExternalId + " = " + externalEventId + ";" );
						
						String Zone = "";
						if ( rs4.first() == false )
						{
							Team = "UNSPECIFIED";
							Zone = "UNSPECIFIED";
						}
						else
						{
							if ( rs4.getInt("AwayTeamId") == rs4.getInt(TeamId) )
							{
								Team = "AWAY";
							}
							else
							{
								Team = "HOME";
							}
							Zone = rs4.getString("Zone");
							if ( null == Zone )
							{
								Zone = "Unspecified";
							}
						}
						Zone = Zone.toUpperCase().replace("\"","");
						rs4.close();
						st4.close();
						
						//sequence += "\\'" + eventHomeMan + "v" + eventAwayMan + ":" + Team + ":" + Zone.replace("\"","") + ":" + eventType.replace(" ","_") + "\\'";
						sequence += "\\'" + Team + ":" + Zone.replace("\"","") + ":" + eventType.replace(" ","_") + "\\'";
					}
					else
					{
						if ( !( sequence.equalsIgnoreCase( "" ) ) )
						{
							sequence += ", ";
						}
						//sequence += "\\'"+eventHomeMan + "v" + eventAwayMan + ":" + eventType.replace(" ","_")+"\\'";
						sequence += "\\'" + eventType.replace(" ","_")+"\\'";
						EndsInGoal = 0;
					}
					SequenceLength += 1;
					
					if ( eventType.equalsIgnoreCase( "GOAL" ) ||
						 eventType.equalsIgnoreCase( "PERIOD END" ) ||
						 eventType.equalsIgnoreCase( "STOPPAGE" ) ||
						 eventType.equalsIgnoreCase( "PENALTY" ) ||
						 eventType.equalsIgnoreCase( "GAME END" ) ||
						 eventType.equalsIgnoreCase( "GAME OFF" ) ||
						 eventType.equalsIgnoreCase( "SHOOTOUT COMPLETED" ) ||
						 eventType.equalsIgnoreCase( "EARLY INTERMISSION END" ) )
					{
						EndingEvent = rs2.getInt( "EventNumber" );
						endingTime = rs2.getTime( "EventTime" );
						int durationSeconds = ( endingTime.getHours() * 3600 + endingTime.getMinutes() * 60 + endingTime.getSeconds() )
								- ( startingTime.getHours() * 3600 + startingTime.getMinutes() * 60 + startingTime.getSeconds() );
						int hr = durationSeconds/3600;
					    int rem = durationSeconds%3600;
					    int mn = rem/60;
					    int sec = rem%60;
					    String hrStr = (hr<10 ? "0" : "")+hr;
					    String mnStr = (mn<10 ? "0" : "")+mn;
					    String secStr = (sec<10 ? "0" : "")+sec;
//						int hours = endingTime.getHours() - startingTime.getHours();
//						int minutes = endingTime.getMinutes() - startingTime.getMinutes();
//						int seconds = endingTime.getSeconds() - startingTime.getSeconds();
//						String duration = "";
//						if ( hours < 10 )
//						{
//							duration += "0";
//						}
//						duration += "" + hours + ":";
//						if ( minutes < 10 )
//						{
//							duration += "0";
//						}
//						duration += "" + minutes + ":";
//						if ( seconds < 0 )
//						{
//							minutes -= 1;
//							seconds += 60;
//						}
//						if ( seconds < 10 )
//						{
//							duration += "0";
//						}
//						duration += "" + seconds;
						Statement st4 = con.createStatement();
						st4.execute( "INSERT IGNORE INTO Sequences_Full VALUES ( " + 
									 GameId + ", " +
									 SequenceNum + ",'[" +
									 sequence + "]', " +
									 "'[" + timestamps + "]', " + 
									 PeriodNumber + ", " +
									 StartingEvent + ", " +
									 EndingEvent + ", " +
									 SequenceLength + ", " +
									 HomeGoals + ", " +
									 AwayGoals + ", " +
									 (HomeGoals - AwayGoals) + ", " +
									 HomeMan + ", " +
									 AwayMan + ", " +
									 (HomeMan - AwayMan) + ",'" +
									 startingTime.toString() + "','" + 
									 endingTime.toString() + "', '" +
									 hrStr + ":" + mnStr + ":" + secStr + "', " +
									 EndsInGoal + ", " +
									 EndsInHomeGoal + ", " +
									 EndsInAwayGoal + ", " +
									 EndsInPenalty + ", " +
									 EndsInHomePenalty + ", " +
									 EndsInAwayPenalty + ", " +
									 homeWin + ", " +
									 awayWin + " );" );
						st4.close();
						sequence = "";
						timestamps = "";
						SequenceNum += 1;
						StartingEvent = 0;
						EndingEvent = 0;
						SequenceLength = 0;
						EndsInGoal = 0;
						EndsInHomeGoal = 0;
						EndsInAwayGoal = 0;
						EndsInPenalty = 0;
						EndsInHomePenalty = 0;
						EndsInAwayPenalty = 0;
						HomeMan = 0;
						AwayMan = 0;
						first = true;
					}
					if ( eventType.equalsIgnoreCase( "GOAL" ) )
					{
						if ( Team.equalsIgnoreCase( "AWAY" ) )
						{
							AwayGoals += 1;
						}
						else
						{
							HomeGoals += 1;
						}
					}
				}
				rs2.close();
				st2.close();
			}
			
			rs1.close();
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to get GameIds" );
			e.printStackTrace();
		}
		
		disconnectDB();
	}
	
	private static int connectDB()
	{
		String CONN_STR = "jdbc:" + dbaddress + "/";
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
		catch (SQLException e)
		{
			System.out.println( "Failed to connect to database." );
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
}
