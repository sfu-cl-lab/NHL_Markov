CREATE TABLE conditional_probability_next_away_goal AS
SELECT A.NodeId, ( A.CurrentValue / ( A.CurrentValue + B.CurrentValue ) ) AS CurrentValue
FROM probability_next_away_goal AS A,
probability_next_home_goal AS B
WHERE A.NodeId = B.NodeId;

CREATE TABLE conditional_probability_next_home_goal AS
SELECT A.NodeId, ( B.CurrentValue / ( A.CurrentValue + B.CurrentValue ) ) AS CurrentValue
FROM probability_next_away_goal AS A,
probability_next_home_goal AS B
WHERE A.NodeId = B.NodeId;

ALTER TABLE conditional_probability_next_home_goal ADD INDEX ( NodeId );
ALTER TABLE conditional_probability_next_away_goal ADD INDEX ( NodeId );

UPDATE conditional_probability_next_away_goal SET CurrentValue = 0.0 WHERE ISNULL( CurrentValue );

UPDATE conditional_probability_next_home_goal SET CurrentValue = 0.0 WHERE ISNULL( CurrentValue );

CREATE TABLE special_cp_next_away_goal AS
Select A.NodeId, A.CurrentValue
FROM conditional_probability_next_away_goal AS A,
conditional_probability_next_home_goal AS B
WHERE A.NodeId = B.NodeId
AND NOT ( A.CurrentValue = 0.0 AND B.CurrentValue = 0.0 );

CREATE TABLE special_cp_next_home_goal AS
Select B.NodeId, B.CurrentValue
FROM conditional_probability_next_away_goal AS A,
conditional_probability_next_home_goal AS B
WHERE A.NodeId = B.NodeId
AND NOT ( A.CurrentValue = 0.0 AND B.CurrentValue = 0.0 );

ALTER TABLE special_cp_next_home_goal ADD INDEX ( NodeId );
ALTER TABLE special_cp_next_away_goal ADD INDEX ( NodeId );

Select SUM(A.Occurrences) FROM nodes AS A,
special_cp_next_home_goal AS B
WHERE A.NodeId = B.NodeId;

#Note: 4052775.0 is the result of the previous query, the value will change
#depending on the transition graph
SELECT ( -1.0 / 4052775.0 ) * ( SUM( A.Occurrences * ( B.CurrentValue * COALESCE( LOG2( B.CurrentValue ), -10000000.0 ) + 
C.CurrentValue * COALESCE( LOG2( C.CurrentValue ), -10000000.0 ) ) ) )
FROM Nodes AS A,
special_cp_next_away_goal AS B,
special_cp_next_home_goal AS C
WHERE A.NodeId = B.NodeId
AND A.NodeId = C.NodeId;