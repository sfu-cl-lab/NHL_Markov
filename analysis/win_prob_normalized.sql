create table `nhl_jeffery_tree`.`winprob_normalized_percentage` select A.GameId as GameId, A.EventNumber as EventNumber, B.EventTime as EventTime, A.HomeWin*100/(A.HomeWin+A.AwayWin) as HomeWin, A.AwayWin*100/(A.HomeWin+A.AwayWin) as AwayWin from nhl_jeffery_tree.play_by_play_event_with_q_val as A, nhl_final2.play_by_play_events as B where B.GameId>=2007020001 and A.GameId=B.GameId and A.EventNumber=B.EventNumber;
