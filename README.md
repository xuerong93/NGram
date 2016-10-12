# NgramPredictor-MapReduceJob
We write three MapReduce jobs to 1. Extract Ngram from WikiPedia data 2. Predict N+1 gram based on N gram  3. Predict words based on prefix

# File explanation:
Aggregate.java: Extract Ngram words and phrases from wiki pedia data from s3://p41-dataset/wiki-dataset. 
Split words with non-alphabetic characters. Count the appearence of each Ngram

Predictor.java: Calculate the probability of appearence of each Ngram word. The probability is calculated by 

P( word | phrase ) = count(phrase + word)/ count(phrase). 
Store only top 5 words for each phrase.
for example  P("cloud computing" ) = count("cloud computing") / count("cloud")

WordPredictor.java: Calculate the probability of appearence of  words for a given prefix. Store only the top 5 possible words. For example, given prefix "car", I would predict "carnegie" "career" "carpet" "carrot"
