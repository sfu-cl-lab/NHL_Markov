create database nhl_uai_analysis;

create table nhl_uai_analysis.`play_by_play_events_with_penalty_information` select * from `nhl_final`.`play-by-play`;

create table nhl_uai_analysis.`play_by_play_event_with_q_val` select * from `nhl_jeffery_tree`.`play_by_play_event_with_q_val`;

create table nhl_uai_analysis.`full_goal_entropy` select * from `nhl_jeffery_tree`.`goal_entropy_full`;
create table nhl_uai_analysis.`local_goal_entropy` select * from `nhl_jeffery_tree`.`goal_entropy_local`;
create table nhl_uai_analysis.`penalty_goal_entropy` select * from `nhl_jeffery_tree`.`goal_entropy_penalty`;

create table nhl_uai_analysis.`full_penalty_entropy` select * from `nhl_jeffery_tree`.`penalty_entropy_full`;
create table nhl_uai_analysis.`local_penalty_entropy` select * from `nhl_jeffery_tree`.`penalty_entropy_local`;
create table nhl_uai_analysis.`penalty_penalty_entropy` select * from `nhl_jeffery_tree`.`penalty_entropy_penalty`;

create table nhl_uai_analysis.`goal_prob_normalized_percentage` select * from `nhl_jeffery_tree`.`goalprob_normalized_percentage`;
create table nhl_uai_analysis.`penalty_prob_normalized_percentage` select * from `nhl_jeffery_tree`.`penaltyprob_normalized_percentage`;
create table nhl_uai_analysis.`win_prob_normalized_percentage` select * from `nhl_jeffery_tree`.`winprob_normalized_percentage`;

