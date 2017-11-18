defmodule Server do
    use GenServer

  @doc """
  Server Process is started using start_link
  nUsers - total number of users in the twitter system
  followersList - contains the follower's list for every user
  displayInterval - the interval in which the tweets are tweeted
  #actorsMap - contains the process id for every user
  tweetsQueueMap - consists of list of tweets for every user
  searchMap - consists of #hashtags and @users mapped to the tweets that consists them
  maxTweetCount - total number of tweets that has to be tweeted/retweeted before the program is exited
  """
  def start_link(nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime) do
    GenServer.start_link(__MODULE__, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime],  name: :genMain ) 
  end

  @doc """
   Update the actors map with (process id/user id, user number) as the key-value pair
  """
  def handle_cast({:updateActors, pid}, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    #  IO.puts "updateActors"
    #  IO.inspect pid     
    {:noreply, [nUsers, followersList, actorsList++[pid], displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  # @doc """
  #  Get the user PID with the user id from the actors list
  # """
  def handle_call({:getUserPID, userId}, _from, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    {:reply, Enum.at(actorsList,userId), [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  @doc """
   Get the actors list
  """
  def handle_call(:getActorsList, _from, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    {:reply, actorsList, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  @doc """
   Update the follower's list 
  """
# to do -  call inside zipf 
  def handle_cast({:updateFollowersList, followers}, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    {:noreply, [nUsers, followersList++followers, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

# to do
  def handle_cast({:addToSearchMap, tweet}, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    {:noreply, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end
# to do
  def handle_cast({:removeFromSearchMap, tweet}, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    {:noreply, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

# TweetQueueMap - map if user's process id and tweet queue
 @doc """
  Tweet updates the servers TweetQueueMap
 """
  def handle_cast({:tweet, tweet, userId}, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
      # check if _from works
      IO.puts "**[tweet function] user id: #{userId}, tweet: #{tweet}**"
      
      if Map.has_key?(tweetsQueueMap, userId) do
        #IO.puts "has key"
        tweetQ = Map.get(tweetsQueueMap, userId)
        #IO.inspect tweetQ
        tweetQ = if :queue.len(tweetQ)> 100 do
            :queue.in(tweet, :queue.drop(tweetQ))
            #remove from Search Map
        else
            :queue.in(tweet, tweetQ)
        end
      else
        #IO.puts "no key"
        tweetQ = :queue.new
        tweetQ = :queue.in(tweet, tweetQ)
      end
      new_tweetsQueueMap = Map.put(tweetsQueueMap, userId, tweetQ)
      #IO.puts "tweetsQueueMap: #{tweetsQueueMap}"
      GenServer.cast :genMain, :incrTotalTweetCnt
       #add to Search Map
      IO.puts "   "
    {:noreply, [nUsers, followersList, actorsList, displayInterval, new_tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  def handle_cast({:retweet, tweet, userId}, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    GenServer.cast {:tweet, tweet, userId}
    {:noreply, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end


  @doc """
    Increment the total tweet count. If the total tweet count has reached the max tweet count, then terminate the program and measure the time
  """
  def handle_cast(:incrTotalTweetCnt, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    #IO.puts "incrTotalTweetCnt"
    totalTweetCnt = totalTweetCnt+1
    if totalTweetCnt == maxTweetCnt do
      #terminate program and measure time
      time = (:os.system_time(:millisecond) - stTime)
      IO.puts "The time taken by #{nUsers} users is #{time} milliseconds"
      System.halt(0)
    end
    {:noreply, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  #todo  - zipf
  def handle_call(:getFollowers, _from, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    followersList = [[1],[2,3],[3,4,5]]
    #updateFollowersList
    {:reply, followersList, [nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt+1, maxTweetCnt, stTime]}
  end 
end

defmodule Main do
    def main(args) do
        args |> parse_args  
    end
    
    defp parse_args([]) do
        IO.puts "No arguments given. Enter the number of clients"
    end
    defp parse_args(args) do
        {_, k, _} = OptionParser.parse(args)
        if Enum.count(k) == 1 do
          {nUsers,  _} = Integer.parse(Enum.at(k, 0))
        else
            IO.puts "please provide exactly one argument"
            System.halt(0)
        end    

        # Start Server
        followersList = []
        actorsMap = %{}
        actorsList = []
        displayInterval = 2
        tweetsQueueMap = %{}
        searchMap = %{}
        totalTweetCnt = 0
        maxTweetCnt  = 10
        stTime = :os.system_time(:millisecond)

        Server.start_link(nUsers, followersList, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime)
        
        # start nUser number of client processes
        createNodes(nUsers, 0)
        #Enum.map(1..nUsers,  fn (_) -> Client.start_link("userid", :queue.new, nUsers, 0, 0, 0) end)

        userPIDs = GenServer.call :genMain, :getActorsList
        #zipf - write function inside server, call updateFollowers
        followersList = GenServer.call :genMain, :getFollowers
        callClientHelper(nUsers, 0, followersList, userPIDs)
        :timer.sleep(30000)
    end

    def callClientHelper(nUsers, i, followersList, userPIDs) do
      if i<nUsers do
        pid = Enum.at(userPIDs, i)
        GenServer.cast pid, {:clientHelper, i}
        callClientHelper(nUsers, i+1, followersList, userPIDs)
      end
    end

    def createNodes(nUsers, i) do
      if i<nUsers do
        Client.start_link(i, :queue.new, nUsers, 0, 0, 0)
        createNodes(nUsers, i+1)
      end
    end
   
end



defmodule Client do
  use GenServer
    @doc """
    Client Process is started using Start_Link
    """
    def start_link(userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval) do
      {_, pid} = GenServer.start_link(__MODULE__, [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval])
      # GenServer.cast :genMain, {:hello} 
      GenServer.cast :genMain, {:updateActors, pid} 
    end
    
    def handle_call(:getUserId, _from, [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]) do
      {:reply, userid, [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]}
    end
    @doc """
    Handle_cast for sending Tweet to server
    Simulator will generate  tweets and call this method with client pid to send tweets
    """
    def handle_cast({:sendtweet}, [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]) do
        # IO.puts "#{inspect tweetQueue}"
        tweetMsg = tweetMsgGenerator(nUsers)
        GenServer.cast :genMain, {:tweet, tweetMsg, self()}
        {:noreply,  [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]}
    end

    @doc """
    Generates random tweets
    """
    def tweetMsgGenerator(nUsers) do
        if(nUsers>10) do
            #IO.puts "#{:rand.uniform(10)}"
            Enum.join(["#Hashtag",  :rand.uniform(10), " ", "@user", :rand.uniform(10)])
        else
            Enum.join(["#Hashtag",  :rand.uniform(nUsers), " ", "@user", :rand.uniform(nUsers)])
        end
    end 

    @doc """
    Schedules various functionalities of the client such as sending tweet, searching for specific #hashtags and @users and live connection and disconnection
    """
    def handle_cast({:clientHelper,i}, [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]) do
        sendRepeatedTweets(nUsers, i)
        {:noreply,  [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]}
    end


    @doc """
    Helper function to send tweets at a regular interval to the Server
    runs in a infinite loop
    """
    def sendRepeatedTweets(nUsers, userId)do
        #IO.puts "sendRepeatedTweets #{pid}"
        tweetMsg = tweetMsgGenerator(nUsers)
        ##IO.puts "sendRepeatedTweets #{tweetMsg}"
        GenServer.cast :genMain, {:tweet, tweetMsg, userId}
        :timer.sleep(3)
        #IO.inspect pid
        sendRepeatedTweets(nUsers, userId)        
    end

    @doc """
    receiveTweet receives tweets from the users its following and saves it to it's own tweet queue 
    
    
    #in the Message queue and retweet in regular interval
    runs in a infinite loop
    """
    def handle_cast({:receiveTweet, tweetMsg}, [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]) do
        #The maximum number of tweets that can be in the tweet queue is 100
        tweetQueue=if :queue.len(tweetQueue)> 100 do
            :queue.in(tweetMsg, :queue.drop(tweetQueue))
            # IO.puts "morethan 100 tweet received"
        else
            :queue.in(tweetMsg, tweetQueue)
            # IO.puts "lessthan 100 tweet received"
        end
        #retweets if the retweetcounter is multple of 5
        retweetCount = retweetCount+1
        if(rem(retweetCount, 5)==0) do 
            GenServer.cast :genMain, {:retweet, tweetMsg, userid}
        end
        
        # IO.puts "#{inspect tweetQueue}"
        {:noreply,  [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]}
    end

    # def handle_cast({:query}, [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]) do
    #     GenServer.cast pid, {:sendRepeatedTweets}
    #     {:noreply,  [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]}
    # end

end