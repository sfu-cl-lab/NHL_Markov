import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.mysql.jdbc.Connection;


public class SequenceVectorForClassification
{
	private static String dbaddress = "mysql://127.0.0.1";
	private static Connection con = null;
	private static HashMap<String,Integer> events = null;
	private static int counter = 0;
	private static int ticker = 0;
	
	public static void main(String[] args)
	{
		//Connect to database
		if ( connectDB() != 0 )
		{
			System.out.println( "Failed to connect to database." );
			return;
		}
		//Get all sequences_unaltered
		if ( initialProcesssequences_unaltered() != 0 )
		{
			System.out.println( "Failed to process sequences_unaltered." );
			return;
		}
		
		if ( writeHashToCSV() != 0 )
		{
			System.out.println( "Failed to write IDs to CSV" );
			return;
		}
		
		if ( writesequences_unalteredAsVector() != 0 )
		{
			System.out.println( "Failed to write sequences_unaltered vectors." );
			return;
		}
		
		if ( disconnectDB() != 0 )
		{
			System.out.println( "Failed to disconnect from database." );
			return;
		}
	}
	private static int initialProcesssequences_unaltered()
	{
		events = new HashMap<String,Integer>();
		try
		{
			//For each sequence
			Statement st1 = con.createStatement();
			st1.execute( "USE nhl_latest_iteration;" );
			ResultSet rs1 = st1.executeQuery( "SELECT Sequence FROM sequences_unaltered;" );
			while ( rs1.next() )
			{
				if ( ticker % 50 == 0 )
				{
					System.out.println( "Ticker = " + ticker );
				}
				String sequence = rs1.getString( "Sequence" );
				sequence = sequence.replace( "[", "" ).replace( "]", "" );
				//Split into events
				String[] sequenceEvents = sequence.split(",");
				int len = sequenceEvents.length;
				for ( int i = 0; i < len; i++ )
				{
					String event = sequenceEvents[i].trim();
					//Add events to hash
					if ( events.get( event ) == null )
					{
						counter += 1;
						events.put( event, counter );
					}
				}
				ticker += 1;
			}
			rs1.close();
			st1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Error when retrieving sequences_unaltered." );
			e.printStackTrace();
			return -1;
		}
		return 0;
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
	private static int writeHashToCSV()
	{
		//Write hash events to CSV
		File f = new File("EventHash.csv");
		try
		{
			FileWriter fw = new FileWriter(f);
			fw.write("EventId,Event\n");
			Iterator<Entry<String,Integer>> it = events.entrySet().iterator();
			while ( it.hasNext() )
			{
				Entry<String,Integer> eventEntry = (Entry<String,Integer>)it.next();
				fw.write(eventEntry.getValue()+","+eventEntry.getKey()+"\n");
			}
			fw.close();
		}
		catch (IOException e)
		{
			System.out.println( "Failed to write to file." );
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	private static int writesequences_unalteredAsVector()
	{
		ticker = 0;
		File f = new File( "SequenceVectorClassifications.csv" );
		File fpos = new File( "GoalPositive.csv" );
		File fneg = new File( "GoalNegative.csv" );
		File fvmsp = new File( "VMSPinput.txt" );
		try
		{
			FileWriter fw = new FileWriter( f );
			FileWriter fwpos = new FileWriter( fpos );
			FileWriter fwneg = new FileWriter( fneg );
			FileWriter fwvmsp = new FileWriter( fvmsp );
			fw.write("EndsInGoal," );
			fwpos.write("EndsInGoal," );
			fwneg.write("EndsInGoal," );
			for ( int i = 0; i < counter - 1; i++ )
			{
				fw.write( "EventType" + i + "," );
				fwpos.write( "EventType" + i + "," );
				fwneg.write( "EventType" + i + "," );
			}
			fw.write( "EventType" + counter + "\n" );
			fwpos.write( "EventType" + counter + "\n" );
			fwneg.write( "EventType" + counter + "\n" );
			Statement st1 = con.createStatement();
			st1.execute( "USE nhl_latest_iteration;" );
			ResultSet rs1 = st1.executeQuery( "SELECT Sequence, EndsInGoal FROM sequences_unaltered;" );
			while ( rs1.next() )
			{
				if ( ticker % 50 == 0 )
				{
					System.out.println( "Ticker2 = " + ticker );
				}
				int endsInGoal = rs1.getInt("EndsInGoal");
				
				if ( endsInGoal == 1 )
				{
					fw.write("1,");
					fwpos.write("1,");
				}
				else
				{
					fw.write("-1,");
					fwneg.write("-1,");
				}
				String[] sequence = rs1.getString("Sequence").replace("[","").replace("]", "").split(",");
				int sequenceLength = sequence.length;
				ArrayList<Integer> featureVector = new ArrayList<Integer>(Collections.nCopies(counter, 0));
				for ( int i = 0; i < sequenceLength; i++ )
				{
					String event = sequence[i].trim();
					fwvmsp.write( events.get(event) + " -1 " );
					int eventIndex = events.get(event)-1;
					int currentValue = featureVector.get( eventIndex );
					featureVector.set(eventIndex, currentValue + 1);
				}
				fwvmsp.write( "-2\n" );
				String vector = StringUtils.join( featureVector, "," );
				fw.write(vector + "\n" );
				if ( endsInGoal == 1 )
				{
					fwpos.write(vector + "\n" );
				}
				else
				{
					fwneg.write(vector + "\n" );
				}
				ticker += 1;
			}
			rs1.close();
			st1.close();
			fw.close();
			fwpos.close();
			fwneg.close();
			fwvmsp.close();
		}
		catch ( IOException e )
		{
			System.out.println( "Failed to write to file." );
			e.printStackTrace();
			return -1;
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to retrieve sequences_unaltered from database." );
			e.printStackTrace();
			return -2;
		}
		return 0;
	}
}
