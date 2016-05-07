# NHL_Markov
A Markov Game Model for Valuing Player Actions in Ice Hockey. This repository includes code to build the model, code to analyze the model and our publications for this project.   
  
##Result Download
It might be quite hard for you to rebuild our model from scratch since the codes are not well-organized and written by several people. But you can download our result (in SQL format)  
+ [Download nhl_raw.sql](https://github.com/sfu-cl-lab/NHL_Markov/releases/download/v1.0/nhl_raw.sql). This database stores data from [NHL official website](https://www.nhl.com/)  
+ [Download nhl_model.sql](https://github.com/sfu-cl-lab/NHL_Markov/releases/download/v1.0/nhl_model.sql). This database stores our model    
+ [Download nhl_analysis.sql](https://github.com/sfu-cl-lab/NHL_Markov/releases/download/v1.0/nhl_analysis.sql). This database stores various analysis results for our model  
  
##Details about this research project  
The high-level idea in this project is to treat sports analytics as a branch of reinforcement learning. (In the sense of AI, not psychology.) We build a big Markov game model for ice hockey as played in the National Hockey League. In this type of model, the game moves to state to state with a certain probability, which depends on the actions taken by both teams. Our model contains over 1.3 million states and is learned from over 8 million events, from 10 NHL seasons. We use a relational database to store both the data and the model.
  
We apply the Markov game model to assign a value to actions . Evaluating the actions of players is a common task for sports analytics. Reinforcement learning has developed the concept of an action-value function, often denoted as the Q-function, that addresses this problem using AI techniques. There are efficient dynamic programming algorithms for computing the Q-function even for large state spaces. For sports analytics, the Q-function has two key advantages.
  
+ Context-dependence. The impact of an action depends on the game context in which it's executed. For example, a goal is worth more if the game is tied than if one time is leading by four. A second shot on goal shortly after a first one is more likely to succeed. Etc.
+ Lookahead. Hockey actions have medium term effects, not only immediate ones. For example, gaining a powerplay does not necessarily lead to a goal immediately or within the next minute.
  
  
We use the action values to rank players: each time a player performs an action, we assign him "points" depending on the action impact as measured by the action value function. The ranks are given by the sum of all points over a season. This is like the well-known +/- score but instead of adding/subtracting one point only when a goal is scored, we assign a continuous range of points for all of a player's actions.

If you are into ice hockey, it's fun and revealing to look at the resulting rankings. For instance, all three members of St. Louis' famed STL line (Schwartz, Tarasenko, Lehtera) are among the top 20 in our list. In fact, Jori Lehtera tops our goal impact list, although his linemate Tarasenko outscored him by far. Our analysis suggests that Lehtera's actions create the opportunities that Tarasenko exploits, leading to a high goal impact ranking for Lehtera and a high goal score for Tarasenko. This pattern fits the traditional division of labor between a center and a wing player. Tarasenko is also the most undervalued player in our list. Starting with the 2015-2016 season, St. Louis has signed him for an annual average more than 7 times higher, which our analysis strongly supports.

Another interesting case is Jason Spezza, who was on top of our goal impact list in the 2013-2014 season. However, his plus-minus score was an awful -26. This is because his team the Ottawa senators was even worse overall (-29 goal differential). The goal impact score rewards players for good decisions even if their teammates do not convert them into goals. After the 2013-2014 season, Spezza requested a trade, which according to our analysis was definitely the right thing for him to do. With the Dallas Stars, his plus-minus score came more in line with his goal impact score, as we would expect (-7 overall, still negative).

We also look at players' impact on penalties. Sadly, two Vancouver Canucks players take spots 2 and 4 when it comes to actions that lead to penalties (Dorsett and Bieksa). Even more sadly, none of the Canucks player make it into the top 20 when it comes to actions that lead to goals. 
