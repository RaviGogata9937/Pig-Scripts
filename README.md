# Pig-Scripts

PIG scripts

loading a file and viewing

A = LOAD '/user/cloudera/users.csv' USING PigStorage(',') AS (user_name:chararray, name:chararray, place:chararray);
B = FOREACH A GENERATE user_name;
DUMP B;

no of users by states
users = LOAD '/user/cloudera/users.csv' USING PigStorage (',') AS (user_id:chararray, name:chararray, state:chararray);
users_group = Group users by state;
countuser = foreach users_group Generate FLATTEN(users.state) , COUNT(users.user_id) as cnt;
total = DISTINCT countuser;
dump total;
5 states with total users
users = LOAD '/user/cloudera/users.csv' USING PigStorage (',') AS (user_id:chararray, name:chararray, state:chararray);
users_group = Group users by state;
countuser = foreach users_group Generate FLATTEN(users.state) , COUNT(users.user_id) as cnt;
total = DISTINCT countuser;
result = ORDER total BY cnt DESC;
finalresult =  LIMIT result 5;
Dump finalresult;

tweets order by tweet_id containing happy
users = LOAD '/user/cloudera/tweets.csv' USING PigStorage (',') AS (tweet_id:chararray, tweet:chararray, username:chararray);
need = filter users By tweet matches 'Happy.+';
result = foreach users generate tweet_id;
finalresult = order result by tweet_id;
Dump result;

Test
tweets = LOAD '/user/cloudera/tweets.csv' USING PigStorage (',') AS (tweet_id:chararray, tweet:chararray, username:chararray);

users = LOAD '/user/cloudera/users.csv' USING PigStorage (',') AS (user_id:chararray, name:chararray, state:chararray);


total = join tweets by username, users by user_id;

get = FILTER total by state == 'CA';

A = group get by username;

C = foreach A generattop e flatten(get.username), COUNT(get.tweet)as cnt;

D = distinct C;

E = FILTER D BY cnt>1;

DUMP E;

Top 20 active users
tweets = LOAD '/user/cloudera/tweets.csv' USING PigStorage (',') AS (tweet_id:chararray, tweet:chararray, username:chararray);
users = LOAD '/user/cloudera/users.csv' USING PigStorage (',') AS (user_id:chararray, name:chararray, state:chararray);
total = join tweets by username, users by user_id;
A = group total by username;
B = foreach A generate (total.username) , COUNT(total.tweet)as cnt;
C = FILTER B by cnt > 1;
D = DISTINCT C;
DUMP D;

users that posted no tweets
tweets = LOAD '/user/cloudera/tweets.csv' USING PigStorage (',') AS (tweet_id:chararray, tweet:chararray, username:chararray);
users = LOAD '/user/cloudera/users.csv' USING PigStorage (',') AS (user_id:chararray, name:chararray, state:chararray);
total = join tweets by username, users by user_id;
A = group total by username;
C = foreach A generate flatten(total.username), COUNT(total.tweet)as cnt;
E = FILTER C BY cnt ==0;
DUMP E;
