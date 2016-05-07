import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Node
{
	private String type;
	private String name;
	private int homeGoals;
	private int awayGoals;
	private int goalDiff;
	private int homeMan;
	private int awayMan;
	private int manDiff;
	private String zone;
	private int period;
	private int count;
	private double pHomeGoal;
	private double pAwayGoal;
	private double pGoal;
	private ArrayList<Node> children;
	private ArrayList<ArrayList<Integer>> timeUntilNextEvent;
	private Node parent;
	private boolean visited;
	private int value;
	private int nodeId;
	private int gameCount;
	private int homeWinCount;
	private int awayWinCount;
	
	public Node()
	{
		this.visited = false;
		this.count = 0;
		this.children = new ArrayList<Node>();
		this.gameCount = 0;
		this.homeWinCount = 0;
		this.awayWinCount = 0;
	}
	
	public String GetType()
	{
		return this.type;
	}
	
	public void SetType( String newType ) 
	{
		this.type = newType;
	}
	
	public void SetName( String newName )
	{
		this.name = newName;
	}
	
	public String GetName()
	{
		return this.name;
	}
	
	public void SetHomeGoals( int goals )
	{
		this.homeGoals = goals;
	}
	
	public void SetAwayGoals( int goals )
	{
		this.awayGoals = goals;
	}
	
	public int GetGoalDiff()
	{
		return this.goalDiff;
	}
	
	public void SetGoalDiff( int newGoalDiff )
	{
		this.goalDiff = newGoalDiff;
	}
	
	public int GetHomeMan()
	{
		return this.homeMan;
	}
	
	public void SetHomeMan( int newHomeMan )
	{
		this.homeMan = newHomeMan;
	}
	
	public int GetAwayMan()
	{
		return this.awayMan;
	}
	
	public void SetAwayMan( int newAwayMan )
	{
		this.awayMan = newAwayMan;
	}
	
	public int GetManDiff()
	{
		return this.manDiff;
	}
	
	public void SetManDiff( int newManDiff )
	{
		this.manDiff = newManDiff;
	}
	
	public int GetPeriod()
	{
		return this.period;
	}
	
	public void SetPeriod( int newPeriod )
	{
		this.period = newPeriod;
	}
	
	public String GetZone()
	{
		return this.zone;
	}
	
	public void SetZone( String newZone )
	{
		this.zone = newZone;
	}
	
	public void SetNodeId( int id )
	{
		this.nodeId = id;
	}
	
	public int GetNodeId()
	{
		return this.nodeId;
	}
	
	public Node FindChildNode( Node node )
	{
		for ( Node child : this.children )
		{
			if ( child.CompareNode( node ) )
			{
				return child;
			}
		}
		return null;
	}
	
	public void SetParent( Node node )
	{
		this.parent = node;
	}
	
	public void AddChild( Node node )
	{
		this.children.add( node );
		node.SetParent( this );
	}
	
	public boolean CompareNode( Node node )
	{
		if ( !(this.GetType().equalsIgnoreCase(node.GetType()) ))
		{
			return false;
		}
		
		try
		{
			if ( this.GetType().equalsIgnoreCase( "Terminal" ) ||
				 node.GetType().equalsIgnoreCase( "Terminal" ) )
			{
				if ( this.GetType().equalsIgnoreCase( node.GetType() ) )
				{
					if ( node.GetName().equalsIgnoreCase( this.GetName() ) )
					{
						return true;
					}
					
					return false;
				}
				
				return false;
			}
		}
		catch ( NullPointerException e )
		{
			return false;
		}
		
		if ( this.GetType().equalsIgnoreCase("Event" ) ||
			 node.GetType().equalsIgnoreCase("Event"))
		{
			if ( !(this.GetName().equalsIgnoreCase(node.GetName()) ))
			{
				return false;
			}
		}
		
		if ( this.GetGoalDiff() != node.GetGoalDiff() )
		{
			return false;
		}
		
		if ( this.GetManDiff() != node.GetManDiff() )
		{
			return false;
		}
		
		if ( this.GetPeriod() != node.GetPeriod() )
		{
			return false;
		}
		
		try
		{
			if ( !(this.GetZone().equalsIgnoreCase(node.GetZone() )))
			{
				return false;
			}
		}
		catch ( NullPointerException e )
		{
			//Do nothing :D
		}
		
		return true;
	}
	
	public boolean HasBeenVisited()
	{
		return this.visited;
	}
	
	public int WriteToFile(File f,int indent)
	{
		if ( null == f )
		{
			System.out.println( "File argument is null" );
			return -1;
		}
		
		if ( this.HasBeenVisited() )
		{
			return 0;
		}
		
		return 0;
	}
	
	public void MarkAsVisited()
	{
		this.visited = true;
	}
	
	public void MarkAsUnvisited()
	{
		this.visited = false;
	}
	
	public void IncrementCount()
	{
		this.count += 1;
	}
	
	public int GetCount()
	{
		return this.count;
	}
	
	public void PrintNode( int level )
	{
		for ( int i = 0; i < level; i++ )
		{
			System.out.print("\t");
		}
		
		try
		{
			System.out.println("ManDiff = "+this.GetManDiff() +
							   ", GoalDiff = "+this.GetGoalDiff() +
							   ", Period = "+this.GetPeriod() +
							   ", Type = " + this.GetType() + 
							   ( this.GetName().isEmpty() ? "" : ", Name = " + this.GetName() ) +
							   ", Count = " + this.GetCount() );
		}
		catch ( NullPointerException e )
		{
			System.out.println("ManDiff = "+this.GetManDiff() +
					   ", GoalDiff = "+this.GetGoalDiff() +
					   ", Period = "+this.GetPeriod() +
					   ", Type = " + this.GetType() + 
					   ", Count = " + this.GetCount() );
		}
		for ( Node child : children )
		{
			child.PrintNode( level + 1 );
		}
	}
	
	public void SetGameCount( int newCount )
	{
		this.gameCount = newCount;
	}
	
	public int GetGameCount()
	{
		return gameCount;
	}
	
	public void IncrementGameCount()
	{
		gameCount += 1;
	}
	
	public void SetHomeWinCount( int newCount )
	{
		this.homeWinCount = newCount;
	}
	
	public int GetHomeWinCount()
	{
		return homeWinCount;
	}
	
	public void IncrementHomeWinCount()
	{
		homeWinCount += 1;
	}
	
	public void SetAwayWinCount( int newCount )
	{
		this.awayWinCount = newCount;
	}
	
	public int GetAwayWinCount()
	{
		return awayWinCount;
	}
	
	public void IncrementAwayWinCount()
	{
		awayWinCount += 1;
	}
	
	public void SetVisited( boolean isVisited )
	{
		this.visited = isVisited;
	}
	
	public void MarkUnvisited()
	{
		if ( visited )
		{
			visited = false;
			for ( Node child : children )
			{
				child.MarkUnvisited();
			}
		}
	}
}
