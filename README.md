# Twitter Simulator Using Phoenix
Supraba Muruganantham [UFID : 9215-9813], Pradosa Patnaik [UFID: 1288-9584]

# Twitter simulator

In this project we simulate a twitter clone and a multiple twitter client in Elixir that has JSON based Phoenix WebSockets API for client-server communication. 
The Application is a simulator for twitter like application with the capability of 
1.login
2.Follow
2.tweet
3.retweet
4.Search for tweets/Hashtags/Mentions


# Twitter engine:
Twitter engine is the main server of twitter which does the following operatins:
1. Saves the login information of a user.
2. Authenticates when a user logs in.
3. Keeps track of followers of each user.
4. Keeps track of follwed users for each user.
5. sends the tweets to all the follwer of a tweeting user in real time.
6. Sends the retweets to the follwers.
7. processes the search query for tweets/Hashtags/Mentions and sends the result to the requesting clients.

# Twitter clients:
Twitter client simulates the behaviour of a twitter user and does the following operation:
1. Signs in with Twitter.
2. follows other users.
3. Tweets messages containing HashTags/Mentions.
4. Retweets if he likes a tweet from the follwed users.
5. Searches for specific tweets, Hashtags or mentions.
6. Clients can go online/offline.


# How to run?
To run the Twitter simulator, follow the below instructions:
1. Start the Phoenix server:
$ mix phoenix.server
2. Run the endpoint by using:
   http://localhost:4001
3. Enter the number of users to simulate and press enter

Twitter simulation will be with the specified number of users. The UI shows the
activity log of twitter by presenting the number of users available online, the total
count tweets tweeted, retweeted and the number of serch queries made.
Approach:


# Implementation details
We are using Phoenix’s Sockets to communicate with the engine (Server) from clients
Sockets internally using web‐sockets and transfers data in JSON format.


Demo video : https://youtu.be/KzSCe1CRo7k
