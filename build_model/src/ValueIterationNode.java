import java.util.ArrayList;


public class ValueIterationNode
{
	private int nodeId = 0;
	
	private int occurrences = 0;
	
	private double[] lastValue = null;
	private double[] currentValue = null;
	private double[] lastQStar = null;
	private double[] currentQStar = null;
	
	private int reward_expected_wins = 0;
	private int reward_expected_goals = 0;
	private int reward_expected_penalties = 0;
	
	private boolean home_goal = false;
	private boolean away_goal = false;
	private boolean home_penalty = false;
	private boolean away_penalty = false;
	private boolean home_win = false;
	private boolean away_win = false;
	
	private ArrayList<Integer> children = null;
	private ArrayList<Integer> childOccurrences = null;
	
	private ArrayList<Double> iterationValues = null;
	
	public ValueIterationNode( int valueOfNodeId, int valueOfOccurrences )
	{
		nodeId = valueOfNodeId;
		occurrences = valueOfOccurrences;
		
		lastValue = new double[9];
		currentValue = new double[9];
		lastQStar = new double[9];
		currentQStar = new double[9];
		
		for ( int i = 0; i < 9; i++ )
		{
			lastValue[i] = 0.0;
			currentValue[i] = 0.0;
			lastQStar[i] = 0.0;
			currentQStar[i] = 0.0;
		}
		
		children = new ArrayList<Integer>();
		childOccurrences = new ArrayList<Integer>();
		
		iterationValues = new ArrayList<Double>();
	}
	
	public void setRewardExpectedGoals( int value )
	{
		reward_expected_goals = value;
		if ( value > 0 )
		{
			home_goal = true;
		}
		else if ( value < 0 )
		{
			away_goal = true;
		}
	}
	
	public void setRewardExpectedWins( int value )
	{
		reward_expected_wins = value;
		if ( value > 0 )
		{
			home_win = true;
		}
		else if ( value < 0 )
		{
			away_win = true;
		}
	}
	
	public void setRewardExpectedPenalties( int value )
	{
		reward_expected_penalties = value;
		if ( value > 0 )
		{
			home_penalty = true;
		}
		else if ( value < 0 )
		{
			away_penalty = true;
		}
	}
	
	public void setOccurrences( int value )
	{
		occurrences = value;
	}
	
	public void addChild( int childNodeId, int valueOfChildOccurrences )
	{
		children.add( childNodeId );
		childOccurrences.add( valueOfChildOccurrences );
	}
	
	public void setCurrentValue( int index, double value )
	{
		currentValue[index] = value;
	}
	
	public void setIterationValue ( int iteration, double value )
	{
		if ( iteration > 50 )
		{
			return;
		}
		while ( iterationValues.size() < iteration )
		{
			iterationValues.add( 0.0 );
		}
		
		double currentIterationValue = iterationValues.get( iteration - 1);
		iterationValues.set( iteration - 1, currentIterationValue + value );
	}
	
	public String generateInsertStatement( String table )
	{
		String statement = "INSERT INTO CheckingMultipleIterations_" + table + " VALUES ( " + this.nodeId +
				", ";
		int len = iterationValues.size();
		for ( int j = 0; ( j < len ) && ( j < 50 ); j++ )
		{
			statement += iterationValues.get( j );
			
			if ( ( j != len - 1 ) && ( j != 49 ) )
			{
				statement += ", ";
			}
		}
		
		statement += " );";
		return statement;
	}
	
	public void setCurrentQStar( int index, double value )
	{
		currentQStar[index] = value;
	}
	
	public ArrayList<Integer> getChildren()
	{
		return children;
	}
	
	public ArrayList<Integer> getChildrenOccurences()
	{
		return childOccurrences;
	}
	
	public int getRewardExpectedWins()
	{
		return reward_expected_wins;
	}
	
	public int getRewardExpectedGoals()
	{
		return reward_expected_goals;
	}
	
	public int getRewardExpectedPenalties()
	{
		return reward_expected_penalties;
	}
	
	public double getLastValue( int index )
	{
		return lastValue[index];
	}
	
	public double getCurrentValue( int index )
	{
		return currentValue[ index ];
	}
	
	public int getOccurrences()
	{
		return occurrences;
	}
	
	public double getQStar( int index )
	{
		return currentQStar[ index ];
	}
	
	public boolean isHomeGoal()
	{
		return home_goal;
	}
	
	public boolean isAwayGoal()
	{
		return away_goal;
	}
	
	public boolean isHomeWin()
	{
		return home_win;
	}
	
	public boolean isAwayWin()
	{
		return away_win;
	}
	
	public boolean isHomePenalty()
	{
		return home_penalty;
	}
	
	public boolean isAwayPenalty()
	{
		return away_penalty;
	}
	
	public void backupLastValues()
	{
		for ( int i = 0; i < 9; i++ )
		{
			lastValue[i] = currentValue[i];
			lastQStar[i] = currentQStar[i];
		}
	}
}