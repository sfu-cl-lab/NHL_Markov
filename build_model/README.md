To build the tree for this model, you'll run BuildTree.java. You need to modify the database setting in the code according to your database configuration.  
   
To get the Q-values, you'll run FastValueIteration.java. Set dbname to the name of the sequence tree database, and dbname2 to the output database for the Q-values.
  
To get the player valuations, run GetPlayerValues.java. Set databaseData to the original dataset, databaseTree to the sequence tree database, databaseValueIteration to the database with the Q-values, and databasePlayerValues to the output database for the player valuations. Also, set metricTable to the table to get Q-values from.
  
Each row in the output table for GetPlayerValues contains the following:  
+ GameId  
+ EventNumber  
+ PlayerId  
+ FromNodeId  
+ ToNodeId  
+ Action  
+ ActionValue  
  
The main parts you need are GameId, PlayerId, and ActionValue.
  
To get Net Game Impact, run:
  
***CREATE TABLE Net_Game_Impact AS
SELECT GameId, PlayerId, SUM( ActionValue ) AS Net_Game_Impact
FROM <databasePlayerValues>.playervaluations
GROUP BY GameId, PlayerId;***
  
To get Average Game Impact, run:
  
***CREATE TABLE Average_Game_Impact
SELECT GameId, PlayerId, AVG( ActionValue ) AS Average_Game_Impact
FROM <databasePlayerValues>.playervaluations
GROUP BY GameId, PlayerId;***
  
To get the sum of averages over each season & season type, run:
  
***CREATE TABLE Season_Sum_Of_Average_Game_Impact
SELECT A.Season, A.SeasonType, B.PlayerId, SUM( B.Average_Game_Impact ) AS Season_Sum_Of_Average_Game_Impact
FROM <databaseData>.game AS A 
JOIN Average_Game_Impact AS B
ON A.GameId = B.GameId
GROUP BY A.Season, A.SeasonType, B.PlayerId;***
