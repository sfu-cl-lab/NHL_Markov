create table nhl_jeffery_tree.player_valuation_state_expectation select A.PlayerId, SUM(A.avga) as Impact from (select AVG(ActionValue) as avga, PlayerId, FromNodeId from nhl_player_valuations_win.playervaluations group by PlayerId, FromNodeId) as A group by PlayerId;

create table nhl_jeffery_tree.player_valuation_state_expectation_div select B.PlayerId as PlayerId, STD(B.Impact) as std_deviation from (select A.PlayerId as PlayerId, SUM(A.avga) as Impact, p from (select AVG(ActionValue) as avga, PlayerId, FromNodeId, Floor(GameId/1000000) as p from nhl_player_valuations_win.playervaluations group by PlayerId, FromNodeId, p) as A group by PlayerId, p) as B group by PlayerId;

create table nhl_jeffery_tree.player_valuation_state select A.PlayerId, A.Impact, B.std_Deviation as Deviation from nhl_jeffery_tree.player_valuation_state_expectation as A, nhl_jeffery_tree.player_valuation_state_expectation_div as B where A.PlayerId=B.PlayerId;







create table nhl_jeffery_tree.player_valuation_state_expectation_sum select A.PlayerId, SUM(A.avga) as Impact from (select SUM(ActionValue) as avga, PlayerId, FromNodeId from nhl_player_valuations_win.playervaluations group by PlayerId, FromNodeId) as A group by PlayerId;

create table nhl_jeffery_tree.player_valuation_state_expectation_div_sum select B.PlayerId as PlayerId, STD(B.Impact) as std_deviation from (select A.PlayerId as PlayerId, SUM(A.avga) as Impact, p from (select SUM(ActionValue) as avga, PlayerId, FromNodeId, Floor(GameId/1000000) as p from nhl_player_valuations_win.playervaluations group by PlayerId, FromNodeId, p) as A group by PlayerId, p) as B group by PlayerId;

create table nhl_jeffery_tree.player_valuation_state_sum select A.PlayerId, A.Impact, B.std_Deviation as Deviation from nhl_jeffery_tree.player_valuation_state_expectation_sum as A, nhl_jeffery_tree.player_valuation_state_expectation_div_sum as B where A.PlayerId=B.PlayerId;





