# Stanley Cup & Season NHL Game Predictions

<p align='center'>
  <img src="/images/stanleycup.jpeg"/>
</p>

The uncertainty of sports continues to fascinate me, so with a go at statistical methods and experimentation, I figured I'd take my best educated guess at the team who might come out on top in 2022 based upon prior game history and team performance metrics.


__UPDATE: The model has been changed and split to two separate models predicting away & home game outcomes including the Stanley Cup Final. Variables have been removed to test for feature importance. In other words, the model accuracy may fluctuate.__

Data:
https://www.hockey-reference.com/

## Model Evaluations - Regular Season + Stanley Cup Final
I added the Stanley Cup Final game schedule with imputed averages with the _exception_ of goals scored. These models are predicting the outcome based upon team stats and my metrics without historical goal and win/loss outcomes. 

#### Away & Home Game Split
<p align="center">
  <img src="/images/v2_away_home_performance.png" />
  <img src="/images/v2_confusion_matrix_stanley_cup_away.png" />
  <img src="/images/v2_confusion_matrix_stanley_cup_home.png" />
</p>

#### Predictions
Predictions were based upon unknown data within the Stanley Cup 2022 series, which consisted averages by team, average duration of an NHL game, and average outcome in place of null values. 
<p align="center">
  <img src="/images/sim_stanley_cup_schedule.PNG" />
  <img src="/images/xgb_stanley_cup_predictions.png" />
</p>
