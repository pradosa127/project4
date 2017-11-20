defmodule Server do
    use GenServer

  @doc """
  Server Process is started using start_link
  nUsers - total number of users in the twitter system
  followersMap - map of user's id and the follower's list for every user
  displayInterval - the interval in which the tweets are tweeted
  actorsList - contains the process id for every user
  tweetsQueueMap - map of user's id and tweet queue
  searchMap - consists of #hashtags and @users mapped to the tweets that consists them
  maxTweetCount - total number of tweets that has to be tweeted/retweeted before the program is exited
  """
  def start_link(nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime) do
    GenServer.start_link(__MODULE__, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime],  name: :genMain ) 
  end

  @doc """
   Update the actors map with (process id, user number) as the (key, value) pair
  """
  def handle_cast({:updateActors, pid}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
      # IO.puts "updateActors"
      # IO.inspect pid     
    {:noreply, [nUsers, followersMap, actorsList++[pid], displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  # # @doc """
  # #  Get the user PID with the user id from the actors list
  # # """
  # def handle_call({:getUserPID, userId}, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
  #   {:reply, Enum.at(actorsList, userId), [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  # end

  @doc """
   Get the actors list
  """
  def handle_call(:getActorsList, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    {:reply, actorsList, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  @doc """
   Get the user tweet queue
  """
  def handle_call({:getTweetQ, userId}, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    #IO.inspect Map.get(tweetsQueueMap, userId)
    {:reply, Map.get(tweetsQueueMap, userId), [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end


  @doc """
   Update the follower's list 
  """
# to do -  call inside zipf 
  def handle_cast({:updateFollowersMap, followers}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    # have to be altered
    {:noreply, [nUsers, followersMap++followers, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

# to do
  def handle_cast({:addToSearchMap, tweet}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end
# to do
  def handle_cast({:removeFromSearchMap, tweet}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  
 @doc """
  When a user  tweets :tweet updates the users's and it's followers tweet queue
 """
  def handle_cast({:tweet, tweet, userId, tweetOrPopulate}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
      IO.puts "User#{userId} tweeting #{tweet}"
      
      if Map.has_key?(tweetsQueueMap, userId) do
        tweetQ = Map.get(tweetsQueueMap, userId)
        #IO.inspect tweetQ
        tweetQ = if :queue.len(tweetQ)> 100 do
            :queue.in(tweet, :queue.drop(tweetQ))
            #remove from Search Map
        else
            :queue.in(tweet, tweetQ)
        end
      else
        tweetQ = :queue.new
        tweetQ = :queue.in(tweet, tweetQ)
      end
      new_tweetsQueueMap = Map.put(tweetsQueueMap, userId, tweetQ)
      
      userPID = Enum.at(actorsList, userId) 
      GenServer.cast userPID, {:receiveTweet, tweet}

      # update follower's tweet queues
      if tweetOrPopulate == "tweet" do
        userFollowersList = Map.get(followersMap, userId)
        GenServer.cast :genMain, {:updateFollowersTweetQ, userFollowersList, 0, tweet}
      end

      #IO.puts "tweetsQueueMap: #{tweetsQueueMap}"
      GenServer.cast :genMain, :incrTotalTweetCnt
       #add to Search Map
      {:noreply, [nUsers, followersMap, actorsList, displayInterval, new_tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  @doc """
  updates the tweet queue of alll the follower's of a user when the user tweets
  """
  def handle_cast({:updateFollowersTweetQ, userFollowersList, i, tweet}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    # IO.inspect followersMap
    # IO.inspect userId
    #userFollowersList = Map.get(followersMap, i)
    if i < length(userFollowersList) do 
      i = Enum.at(userFollowersList, i)
      userPID = Enum.at(actorsList, i)
      GenServer.cast :genMain, {:tweet, tweet, i, "populate"}
      GenServer.cast :genMain, {:updateFollowersTweetQ, userFollowersList, i+1, tweet}
    end
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  # def updateFollowersTweetQ(tweet, followersMap, i) do
  #   if i < length(followersMap) do 
  #     userId = Enum.at(followersMap, i)
  #     userPID = GenServer.cast :genMain, {:getUserPID, userId}
  #      #$%^
  #     GenServer.cast :genMain, {:tweet, tweet, userId, "populate"}
  #     updateFollowersTweetQ(tweet, followersMap, i+1)
  #   end
  #end

#check if this is required
  def handle_call({:getUserFollowers, userId}, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    {:reply, Map.get(followersMap, userId), [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt+1, maxTweetCnt, stTime]}
  end 


  @doc """
  For retweeting the tweets
  """
  def handle_cast({:retweet, tweet, userId}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    GenServer.cast :genMain, {:tweet, tweet, userId, "tweet"}
    IO.puts "User#{userId} retweeting"
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end


  @doc """
    Increment the total tweet count. If the total tweet count has reached the max tweet count, then terminate the program and measure the time
  """
  def handle_cast(:incrTotalTweetCnt, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    #IO.puts "incrTotalTweetCnt"
    #:timer.sleep(100)
    totalTweetCnt = totalTweetCnt+1
    IO.puts "total Tweet count: #{totalTweetCnt}"
    if totalTweetCnt == maxTweetCnt do
      time = (:os.system_time(:millisecond) - stTime)
      IO.puts "The time taken by #{nUsers} users is #{time} milliseconds"
      System.halt(0)
    end
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end

  #todo  - zipf
  def handle_call(:getFollowers, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    followersMap = %{0=>[1], 1=>[2,3], 2=>[3, 1], 3=>[1,2,4,0], 4=>[0]}
    #updateFollowersMap
    {:reply, followersMap, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt+1, maxTweetCnt, stTime]}
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
        followersMap = %{}
        actorsMap = %{}
        actorsList = []
        displayInterval = 2
        tweetsQueueMap = %{}
        searchMap = %{}
        totalTweetCnt = 0
        maxTweetCnt  = 100
        stTime = :os.system_time(:millisecond)

        Server.start_link(nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime)
        
        # start nUser number of client processes
        createNodes(nUsers, 0)
        #Enum.map(1..nUsers,  fn (_) -> Client.start_link("userid", :queue.new, nUsers, 0, 0, 0) end)
       
        userPIDs = GenServer.call :genMain, :getActorsList
        #zipf - write function inside server, call updateFollowers
        followersMap = GenServer.call :genMain, :getFollowers
        callClientHelper(nUsers, 0, followersMap, userPIDs)
        :timer.sleep(30000)
    end

    def callClientHelper(nUsers, i, followersMap, userPIDs) do
      if i<nUsers do
        pid = Enum.at(userPIDs, i)
        #displayInterval = GenServer.call pid, :getDisplayInterval
        GenServer.cast pid, {:clientHelper, i}
        #Client.clientHelper(i, displayInterval)
        callClientHelper(nUsers, i+1, followersMap, userPIDs)
      end
    end

    def createNodes(nUsers, i) do
      if i<nUsers do
        userId = i
        tweetQueue = :queue.new
        nUsers = nUsers
        retweetCount = 0
        followerMapSize = 0
        displayInterval = 0
        state = 1 # 0 - disconnected , 1 - connected
        Client.start_link(userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state)
        createNodes(nUsers, i+1)
      end
    end
end

  

defmodule Client do
  use GenServer
    @doc """
    Client Process is started using start_link
    """
    def start_link(userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state) do
      {_, pid} = GenServer.start_link(__MODULE__, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state])
      # GenServer.cast :genMain, {:hello} 
      GenServer.cast :genMain, {:updateActors, pid} 
    end
    
    def handle_call(:getUserId, _from, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
      {:reply, userId, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end

    #to do : not used anywhere, check if required in the end
    # def handle_call(:getState, _from, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
    #   IO.puts "#{state}"
    #   {:reply, state, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    # end

    #to do : not used anywhere, check if required in the end
    # def handle_call(:getDisplayInterval, _from, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
    #   {:reply, displayInterval, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    # end

     # def clientHelper(i, displayInterval)do    
    # end

    # def handle_info({:sendRepeatedTweets, userId}, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
    #   #IO.puts "sendRepeatedTweets #{userId}"
    #   if state == 1 do
    #     tweetMsg = tweetMsgGenerator(nUsers)
    #     ##IO.puts "sendRepeatedTweets #{tweetMsg}"
    #     GenServer.cast :genMain, {:tweet, tweetMsg, userId, "tweet"}

    #     #to do : has to be removed after Process.start_after is fixed
    #     #:timer.sleep(3)
    #     #GenServer.cast self(), {:sendRepeatedTweets, userId, temp+1}
    #     #IO.inspect pid
    #   end
    #   {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    # end

    @doc """
    Schedules various functionalities of the client such as sending tweet, searching for specific #hashtags and @users and live connection and disconnection
    """
    #  def handle_info(:print_fruit, state) do
    #     schedule()
    #     print_fruit()
    #     {:noreply, state}
    # end
    # def handle_info(:print_flower, state) do
    #     schedule()
    #     print_flower()
    #     {:noreply, state}
    # end



    #to do - to be called after zipf and update of follower's map size
    @doc """
    Updates the display interval such that greater the follower's size lesser the display interval and more the tweets tweeted
      followersSize >90% => displayInterval = 0 ms
      followersSize 80-90% => displayInterval = 10 ms
      followersSize 70-80% => displayInterval = 20 ms and so on
    """
    def handle_call(:updateDisplayInterval, _from, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
      divf =  round((followerMapSize/nUsers)*100) 
      rem = Integer.mod(divf,10)
      divf = divf-rem
      displayInterval = 90 - divf
      {:reply, displayInterval, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
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
    
    def handle_cast({:clientHelper, i}, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
        #IO.puts "Client helper #{i} #{displayInterval}"

        GenServer.cast self(), {:sendRepeatedTweets, i, 0, :rand.uniform(10)}
        GenServer.cast self(), :disconnect_connect

        # #causing some timeout problem: has to be corrected
        # Process.send_after(self(), {:sendRepeatedTweets, i}, displayInterval)
        # #Process.send_after(self(), :search, 50)
        # Process.send_after(self(), {:disconnect_connect, displayInterval}, 10*displayInterval)
        GenServer.cast self(), {:clientHelper, i}
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end

    @doc """
    Helper function to send tweets at a regular interval to the Server
    runs in a infinite loop
    """
    #to do:has to be changed to handle_info later
    def handle_cast({:sendRepeatedTweets, userId, i, n}, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
        #to do : has to be removed after Process.start_after is fixed
        if state == 0 do
          :timer.sleep(10)
        else 
          if i < n do
            #IO.puts "sendRepeatedTweets #{userId}"
            tweetMsg = tweetMsgGenerator(nUsers)
            ##IO.puts "sendRepeatedTweets #{tweetMsg}"
            GenServer.cast :genMain, {:tweet, tweetMsg, userId, "tweet"}

            #to do : has to be removed after Process.start_after is fixed
            #:timer.sleep(3)
            GenServer.cast self(), {:sendRepeatedTweets, userId, i+1, n}
            #IO.inspect pid
          end
        end
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end

    def handle_cast(:changeState, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
        #IO.puts "beforestate:: #{state}"
        if state == 0 do 
          state = 1
          IO.puts "User#{userId} woke up"
        else
          state = 0
          IO.puts "User#{userId} sleeping"
        end
        #IO.puts "afterstate #{state}"
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end

    def handle_cast({:updateQ, newTweetQ}, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
        {:noreply,  [userId, newTweetQ, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end

    #to do:has to be changed to handle_info later
    def handle_cast(:disconnect_connect, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do      
        GenServer.cast self(), :changeState
        if state == 1 do
          # update the tweet queue after waking up
          newTweetQ = GenServer.call :genMain, {:getTweetQ, userId}
          GenServer.cast self(), {:updateQ, newTweetQ}
        end
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end

    @doc """
    receiveTweet receives tweets and saves it to it's own tweet queue 
    """
    def handle_cast({:receiveTweet, tweetMsg}, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
        if state == 1 do
          #The maximum number of tweets that can be in the tweet queue is 100
          tweetQueue=if :queue.len(tweetQueue)> 100 do
              :queue.in(tweetMsg, :queue.drop(tweetQueue))
          else
              :queue.in(tweetMsg, tweetQueue)
          end
          #retweets if the retweetcounter is multple of 5
          retweetCount = retweetCount+1
          IO.puts "retweetCounter: #{retweetCount}"
          if(rem(retweetCount, 5)==0) do 
              GenServer.cast :genMain, {:retweet, tweetMsg, userId}
          end
        # else
        #   IO.puts "can't update while sleeping"
        end
        # IO.puts "#{inspect tweetQueue}"
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end
end