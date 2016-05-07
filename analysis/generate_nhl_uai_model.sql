create database nhl_uai_model;

create table nhl_uai_model.`full_probability_away_win` select * from `nhl_fast_value_iteration_full`.`probability_away_win`;
create table nhl_uai_model.`full_probability_home_win` select * from `nhl_fast_value_iteration_full`.`probability_home_win`;
create table nhl_uai_model.`full_probability_next_away_goal` select * from `nhl_fast_value_iteration_full`.`probability_next_away_goal`;
create table nhl_uai_model.`full_probability_next_away_penalty` select * from `nhl_fast_value_iteration_full`.`probability_next_away_penalty`;
create table nhl_uai_model.`full_probability_next_home_goal` select * from `nhl_fast_value_iteration_full`.`probability_next_home_goal`;
create table nhl_uai_model.`full_probability_next_home_penalty` select * from `nhl_fast_value_iteration_full`.`probability_next_home_penalty`;
create table nhl_uai_model.`full_edges` select FromNodeId, ToNodeId, Occurrences from `nhl_sequence_tree_full`.`edges`;
create table nhl_uai_model.`full_nodes` select NodeId, NodeType, NodeName, GoalDifferential, ManpowerDifferential, Period, Zone, Occurrences, GameCount, HomeWinCount, AwayWinCount from `nhl_sequence_tree_full`.`nodes`;
create table nhl_uai_model.`full_play_by_play_events_with_node_information` select * from `nhl_sequence_tree_full`.`play_by_play_events_with_node_information`;

create table nhl_uai_model.`local_probability_away_win` select * from `nhl_fast_value_iteration_local`.`probability_away_win`;
create table nhl_uai_model.`local_probability_home_win` select * from `nhl_fast_value_iteration_local`.`probability_home_win`;
create table nhl_uai_model.`local_probability_next_away_goal` select * from `nhl_fast_value_iteration_local`.`probability_next_away_goal`;
create table nhl_uai_model.`local_probability_next_away_penalty` select * from `nhl_fast_value_iteration_local`.`probability_next_away_penalty`;
create table nhl_uai_model.`local_probability_next_home_goal` select * from `nhl_fast_value_iteration_local`.`probability_next_home_goal`;
create table nhl_uai_model.`local_probability_next_home_penalty` select * from `nhl_fast_value_iteration_local`.`probability_next_home_penalty`;
create table nhl_uai_model.`local_edges` select FromNodeId, ToNodeId, Occurrences from `nhl_sequence_tree_local`.`edges`;
create table nhl_uai_model.`local_nodes` select NodeId, NodeType, NodeName, GoalDifferential, ManpowerDifferential, Period, Zone, Occurrences, GameCount, HomeWinCount, AwayWinCount from `nhl_sequence_tree_local`.`nodes`;
create table nhl_uai_model.`local_play_by_play_events_with_node_information` select * from `nhl_sequence_tree_local`.`play_by_play_events_with_node_information`;


create table nhl_uai_model.`penalty_probability_away_win` select * from `nhl_fast_value_iteration_penalty`.`probability_away_win`;
create table nhl_uai_model.`penalty_probability_home_win` select * from `nhl_fast_value_iteration_penalty`.`probability_home_win`;
create table nhl_uai_model.`penalty_probability_next_away_goal` select * from `nhl_fast_value_iteration_penalty`.`probability_next_away_goal`;
create table nhl_uai_model.`penalty_probability_next_away_penalty` select * from `nhl_fast_value_iteration_penalty`.`probability_next_away_penalty`;
create table nhl_uai_model.`penalty_probability_next_home_goal` select * from `nhl_fast_value_iteration_penalty`.`probability_next_home_goal`;
create table nhl_uai_model.`penalty_probability_next_home_penalty` select * from `nhl_fast_value_iteration_penalty`.`probability_next_home_penalty`;
create table nhl_uai_model.`penalty_edges` select FromNodeId, ToNodeId, Occurrences from `nhl_sequence_tree_penalty`.`edges`;
create table nhl_uai_model.`penalty_nodes` select NodeId, NodeType, NodeName, GoalDifferential, ManpowerDifferential, Period, Zone, Occurrences, GameCount, HomeWinCount, AwayWinCount from `nhl_sequence_tree_penalty`.`nodes`;
create table nhl_uai_model.`penalty_play_by_play_events_with_node_information` select * from `nhl_sequence_tree_penalty`.`play_by_play_events_with_node_information`;
