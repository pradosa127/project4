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
  def zipf(nUSers) do
        s=1.47
        c=1/(Enum.map(1..nUSers, fn(x)->1/x end)
          |>Enum.map(fn(x)-> :math.pow(x,s) end)
          |>Enum.sum())

        zipDist=Enum.map(1..nUSers, fn(x)->c/:math.pow(x,s) end )
        numFollowers= Enum.map(zipDist, fn(x)->round(Float.ceil((x*nUSers))) end ) 
        # follwerSet=createfollowerlist([],Enum.at(numFollowers,1),nUSers)
        for k<- 0..nUSers-1 do
            follwerSet=createfollowerlist([],Enum.at(numFollowers,k),nUSers)
            # IO.puts "#{inspect follwerSet}"  
            GenServer.cast :genMain,{:updateFollowersMap, follwerSet,k}
        end
        # follwerSet=createfollowerlist([],Enum.at(numFollowers,1),nUSers)
        # IO.puts "#{inspect follwerSet}"  
        # GenServer.cast :genMain, {:updateFollowersList, follwerSet}
        IO.puts "update completed"
  end

    def createfollowerlist(list,nFollower,nUSers) do
        if nFollower>0 do
          list=list++[:rand.uniform(nUSers)]
          createfollowerlist(list,nFollower-1,nUSers)
        else
          list 
        end    
    end

# to do -  call inside zipf 
  def handle_cast({:updateFollowersMap, followers,userid}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
    followersMap = Map.put(followersMap, userid, followers)
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
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
      # IO.puts "User#{userId} tweeting #{tweet}"
      
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
      
      totalTweetCnt=totalTweetCnt+1
      # IO.puts "total Tweet count: #{totalTweetCnt}"
      if totalTweetCnt == maxTweetCnt do
        time = (:os.system_time(:millisecond) - stTime)
        IO.puts "The time taken by #{nUsers} users is #{time} milliseconds"
        System.halt(0)
      end
      #IO.puts "tweetsQueueMap: #{tweetsQueueMap}"
      # GenServer.cast :genMain, :incrTotalTweetCnt
       #add to Search Map
      {:noreply, [nUsers, followersMap, actorsList, displayInterval, new_tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]}
  end


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

  def handle_call(:getFollowers, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime]) do
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
        maxTweetCnt  = 1000
        stTime = :os.system_time(:millisecond)

        Server.start_link(nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime)
        
        # start nUser number of client processes
        createNodes(nUsers, 0)
        #Enum.map(1..nUsers,  fn (_) -> Client.start_link("userid", :queue.new, nUsers, 0, 0, 0) end)
       
        userPIDs = GenServer.call :genMain, :getActorsList
        IO.puts "#{inspect userPIDs}"
        Server.zipf(nUsers)
        followersList = GenServer.call :genMain, :getFollowers
        IO.puts "#{inspect followersList}"
        # IO.puts "#{inspect followersList}"  
        # callClientHelper(nUsers, 0, followersList, userPIDs)
        # userPIDs = GenServer.call :genMain, :getActorsList
        #zipf - write function inside server, call updateFollowers
        # followersMap = GenServer.call :genMain, :getFollowers
        # callClientHelper(nUsers, 0, followersMap, userPIDs)
        :timer.sleep(30000)
    end

    def callClientHelper(nUsers, i, followersMap, userPIDs) do
      if i<nUsers do
        pid = Enum.at(userPIDs, i)
        #displayInterval = GenServer.call pid, :getDisplayInterval
        # GenServer.cast pid, {:clientHelper, i,pid}
        Client.ch(i,pid)
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
      else
       IO.puts "users created"
      end 
    end
end


defmodule Client do
  use GenServer
    @doc """
    Client Process is started using start_link
    """
    def init(state) do
        schedule()
        {:ok, state}
    end

    def schedule()do
        Process.send_after(self(), :sendRepeatedTweets, 200)
        Process.send_after(self(), :reTweet, 3000)
        Process.send_after(self(), :changeState, 400)
    end
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
    
    def handle_cast({:clientHelper,i,pid}, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
        #IO.puts "Client helper #{i} #{displayInterval}"

        GenServer.cast pid, {:sendRepeatedTweets, i, 0, :rand.uniform(100)}
        # GenServer.cast self(), :disconnect_connect

        # #causing some timeout problem: has to be corrected
        # Process.send_after(self(), {:sendRepeatedTweets, i}, displayInterval)
        # #Process.send_after(self(), :search, 50)
        # Process.send_after(self(), {:disconnect_connect, displayInterval}, 10*displayInterval)
        # GenServer.cast self(), {:clientHelper, i}
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end
    def ch(i,pid) do
      IO.inspect pid
      GenServer.cast pid, {:sendRepeatedTweets, i, 0, 10000,pid}
    end
    @doc """
    Helper function to send tweets at a regular interval to the Server
    runs in a infinite loop
    """
    #to do:has to be changed to handle_info later
    def handle_cast({:sendRepeatedTweets, userId, i, n,pid}, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
        #to do : has to be removed after Process.start_after is fixed
        # IO.puts "sendRepeatedTweets #{userId}"
        if state == 0 do
          :timer.sleep(100)
        else 
          if i < n do
            schedule()
            IO.puts "sendRepeatedTweets #{userId}"
            tweetMsg = tweetMsgGenerator(nUsers)
            ##IO.puts "sendRepeatedTweets #{tweetMsg}"
            GenServer.cast :genMain, {:tweet, tweetMsg, userId, "tweet"}

            #to do : has to be removed after Process.start_after is fixed
            #:timer.sleep(3)
            GenServer.cast pid, {:sendRepeatedTweets, userId, i+1, n,pid}
            #IO.inspect pid
          end
        end
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end


    def handle_info(:sendRepeatedTweets, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
        #to do : has to be removed after Process.start_after is fixed
        # IO.puts "sendRepeatedTweets #{userId}"
        if state == 0 do
          :timer.sleep(100)
        else 
            schedule()
            # IO.puts "sendRepeatedTweets #{userId}"
            tweetMsg = tweetMsgGenerator(nUsers)
            GenServer.cast :genMain, {:tweet, tweetMsg, userId, "tweet"}
            # IO.puts "hi"
        end
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end

    def handle_info(:reTweet, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
        #to do : has to be removed after Process.start_after is fixed
        # IO.puts "sendRepeatedTweets #{userId}"
        if state == 0 do
          :timer.sleep(100)
        else 
            schedule()
            # IO.puts "sendRepeatedTweets #{userId}"
            if :queue.is_empty(tweetQueue)== false do
              tweetMsg = :queue.get(tweetQueue)
              # IO.puts "retweet #{tweetMsg}"
              GenServer.cast :genMain, {:retweet, tweetMsg, userId}
            end  
        end
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end
    
    

    def handle_info(:changeState, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]) do
        #IO.puts "beforestate:: #{state}"
          schedule()
          if state == 0 do 
               state=1
          # IO.puts "User#{userId} woke up"
          newTweetQ = GenServer.call :genMain, {:getTweetQ, userId}
          IO.puts "#{inspect newTweetQ}"
          # GenServer.cast self(), {:updateQ, newTweetQ}
        else
               state=0
          # IO.puts "User#{userId} sleeping"
        end
        #IO.puts "afterstate #{state}"
        {:noreply,  [userId, newTweetQ, nUsers, retweetCount, followerMapSize, displayInterval, state]}
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

    # def handle_cast({:query}, [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]) do
    #     GenServer.cast pid, {:sendRepeatedTweets}
    #     {:noreply,  [userid, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval]}
    # end
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
          # retweetCount = retweetCount+1
          # IO.puts "retweetCounter: #{retweetCount}"
          # if(rem(retweetCount, 5)==0) do 
          #     GenServer.cast :genMain, {:retweet, tweetMsg, userId}
          # end
        # else
        #   IO.puts "can't update while sleeping"
        end
        # IO.puts "#{inspect tweetQueue}"
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state]}
    end
end