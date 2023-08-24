Project in BC with Miao Liu and Yang Cao. For a new paper.

# Risk Exposure Score Calculating Based on Transcripts

The `ScoreCalculating` is from Hassan et al. 

"Firm-Level Political Risk: Measurement and Effects", 

"Firm-level Exposure to Epidemic Diseases: Covid-19, SARS, and H1N1", 

"The Global Impact of Brexit Uncertainty": https://github.com/mschwedeler/firmlevelrisk. 

We made some We adapted the source code for the use of the algorithm on the _ciq_ trancript datasets, then obtaining the covid and political exposure/scores. 

# Merging Supply Chain Relations Dataset with Scores

The `MergingCRSP` is to merge the _disclosed_ supply chain relations data with scores we calculated before, constructing one meta data we shall use.

The `MergingFactset` is to merge the supply chain relations data from _Factset_ with the same scores, as well as the _S&P_ ESG scores, constructing another meta data we shall use.

The `FuzzyMatch` is for company name fuzzy match, which allow us to link the factset company names with crsp, compustat and etc.

# Main Regression

The two R scripts are to do our main regression on the explanatory power of supply chain risk to stock price changes, divided into two time scales.

# Network Modelling

The `NetworkModel` is to transform the dataframe-like supply chain data into network models by year and quarter, allowing us to do further analysis.
