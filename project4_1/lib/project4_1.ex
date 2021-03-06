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

  def init(state) do
      schedule()
      {:ok, state}
  end

  def schedule()do
        Process.send_after(self(), :diplaySimulatorOP, 3000)
  end
  def start_link(nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn) do
    GenServer.start_link(__MODULE__, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn],  name: :gen_main ) 
  end


  def handle_info(:diplaySimulatorOP, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
           schedule()
           time = (:os.system_time(:millisecond) - stTime)
           tweetsPersecond=((totalTweetCnt-reTweetcnt)/time)*1000
           reTweetPersecond=(reTweetcnt/time)*1000
           searchPresecond=(searchCount/time)*1000
           maximumFollower=Map.get(followersMap,0) |>length()
           totalRegisterdUser=length(actorsList)
           
           IO.puts "---------------"
           IO.puts "User Statisics:"
           IO.puts "---------------"
           IO.puts "Total Users            : #{nUsers}"
           IO.puts "Total registered Users : #{totalRegisterdUser}"
           IO.puts "Total Online Users     : #{totalRegisterdUser-noOfdisConn}"
           IO.puts "Maximu subscriber count as per Zipf distibution: #{maximumFollower}"
           IO.puts " "
           IO.puts "---------------"
           IO.puts "Tweet Statisics:"
           IO.puts "---------------"
           IO.puts "Total Tweet count   : #{totalTweetCnt}"
           IO.puts "Tweets persecond    : #{tweetsPersecond}"
           IO.puts "Total ReTweet count : #{reTweetcnt}"
           IO.puts "Retweets per second : #{reTweetPersecond}"
           IO.puts " "
           IO.puts "---------------"
           IO.puts "Search Statisics:"
           IO.puts "---------------"
           IO.puts "Total Search count                          : #{searchCount}"
           IO.puts "Searches(Tweets/Hashtag/Mentions) per second: #{searchPresecond}"
           IO.puts "The time taken by #{nUsers} users is #{time} milliseconds"
           IO.puts ""
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end

  @doc """
   Update the actors map with (process id, user number) as the (key, value) pair
  """
  def handle_cast({:updateActors, pid}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do   
    {:noreply, [nUsers, followersMap, actorsList++[pid], displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end

  def handle_cast({:updateNoofDisconn, onlineOrOffline}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do   
      noOfdisConn=if onlineOrOffline==0 do
        noOfdisConn+1
      else
        noOfdisConn-1
      end
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end

  def handle_cast({:updateNoSearch}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do   
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount+1,noOfdisConn]}
  end

  # # @doc """
  # #  Get the user PID with the user id from the actors list
  # # """
  # def handle_call({:getUserPID, userId}, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
  #   {:reply, Enum.at(actorsList, userId), [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  # end

  @doc """
   Get the actors list
  """
  def handle_call(:getActorsList, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
    {:reply, actorsList, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end

  @doc """
   Get the user tweet queue
  """
  def handle_call({:getTweetQ, userId}, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
    {:reply, Map.get(tweetsQueueMap, userId), [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
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
            GenServer.cast :gen_main,{:updateFollowersMap, follwerSet,k}
        end
        IO.puts "update completed"
  end




  def handle_cast({:zipf,nUsers},[nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
        s=1.47
        c=1/(Enum.map(1..nUsers, fn(x)->1/x end)
          |>Enum.map(fn(x)-> :math.pow(x,s) end)
          |>Enum.sum())

        zipDist=Enum.map(1..nUsers, fn(x)->c/:math.pow(x,s) end )
        numFollowers= Enum.map(zipDist, fn(x)->round(Float.ceil((x*nUsers))) end ) 
        # follwerSet=createfollowerlist([],Enum.at(numFollowers,1),nUSers)
        for k<- 0..nUsers-1 do
            follwerSet=createfollowerlist([],Enum.at(numFollowers,k),nUsers)
            # IO.puts "#{inspect follwerSet}"  
            GenServer.cast :gen_main,{:updateFollowersMap, follwerSet,k}
        end
        IO.puts "update completed"
        {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end

    def createfollowerlist(list,nFollower,nUsers) do
        if nFollower>0 do
          list=list++[:rand.uniform(nUsers)]
          createfollowerlist(list,nFollower-1,nUsers)
        else
          list 
        end    
    end

# to do -  call inside zipf 
  def handle_cast({:updateFollowersMap, followers,userid}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
    followersMap = Map.put(followersMap, userid, followers)
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end

 @doc """
  For extracting hashtags and mentions from the tweets and updating the search map
 """
  def extractHashTagFromTweets(tweet, searchMap, strArr, i, operation) do
    if i<length(strArr) do
      str  = Enum.at(strArr,i)
      if String.match?(str, ~r/(#).*/) or String.match?(str, ~r/(@).*/) do
        #IO.puts "HashTag/mention #{str}"
        if Map.has_key?(searchMap, str) do
          hashtagSet = Map.get(searchMap, str)
          #IO.inspect tweetQ
          if operation == 1 do
            hashtagSet = if MapSet.size(hashtagSet)<= 100 do
                MapSet.put(hashtagSet, tweet)
            end
          else
            hashtagSet = MapSet.delete(hashtagSet, tweet)
          end
        else
            hashtagSet = MapSet.new()
            hashtagSet =  MapSet.put(hashtagSet, tweet)
        end
        # IO.inspect hashtagSet
        newSearchMap = Map.put(searchMap, str, hashtagSet)
        extractHashTagFromTweets(tweet, newSearchMap, strArr, i+1, operation)
      end
    else
      #IO.inspect searchMap
      searchMap
    end
  end


 @doc """
  For updating hashtags and mentions in the searchMap
 """
  def handle_cast({:updateSearchMap, tweet, operation}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
    strArr = String.split(tweet, " ")
    if operation == 1 do
      newSearchMap = extractHashTagFromTweets(tweet, searchMap, strArr, 0, 1)
    else
      newSearchMap = extractHashTagFromTweets(tweet, searchMap, strArr, 0, 0)
    end
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, newSearchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end

 @doc """
  For searching tweets with specific hashtags and mentions
 """
  def handle_call({:searchHashTag, hashTagOrMention}, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
    #hashTagOrMention = "#HashTag1"
    if Map.has_key?(searchMap, hashTagOrMention) do
      tweetSet = Map.get(searchMap, hashTagOrMention)
    else
      tweetSet = MapSet.new()
    end
    # IO.puts "Result of searching for #{hashTagOrMention}"
    # IO.inspect tweetSet
    {:reply, tweetSet, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end
    
  
 @doc """
  When a user  tweets :tweet updates the users's and it's followers tweet queue
 """
  def handle_cast({:tweet, tweet, userId, tweetOrPopulate}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
      # IO.puts "User#{userId} tweeting #{tweet}"
      if Map.has_key?(tweetsQueueMap, userId) do
        tweetQ = Map.get(tweetsQueueMap, userId)
        #IO.inspect tweetQ
        if :queue.len(tweetQ)> 100 do
            tweetQ = :queue.in(tweet, :queue.drop(tweetQ))
            #remove tweet from Search Map
            GenServer.cast :gen_main, {:updateSearchMap, tweet, 0}
        else
            :queue.in(tweet, tweetQ)
        end
      else
        tweetQ = :queue.new
        tweetQ = :queue.in(tweet, tweetQ)
      end
      if(tweetQ==nil)do
        IO.puts "dfvn,nmvkdfnvmbk/ndfb"
        System.halt(0)
      end
      
      new_tweetsQueueMap = Map.put(tweetsQueueMap, userId, tweetQ)
      # if Map.get(new_tweetsQueueMap, userId)== nil do
      #   IO.inspect userId
      #   IO.inspect new_tweetsQueueMap
      #   IO.inspect Map.get(tweetsQueueMap, userId)
      #   IO.puts "mldsclsjcmasvkdjfvm/kvjfioefkvjwivrwtip'sbrtg'ibrt'btr"
      #   System.halt(0)
      # end
      userPID = Enum.at(actorsList, userId) 
      GenServer.cast userPID, {:receiveTweet, tweet}

      # update searchMap and follower's tweet queues
      if tweetOrPopulate == "tweet" do
        #add tweet from Search Map
        GenServer.cast :gen_main, {:updateSearchMap, tweet, 1}
        userFollowersList = Map.get(followersMap, userId)
        GenServer.cast :gen_main, {:updateFollowersTweetQ, userFollowersList, 0, tweet}
      end
      
      totalTweetCnt=totalTweetCnt+1

      # if(Integer.mod(totalTweetCnt,100000)==0)do
      #      time = (:os.system_time(:millisecond) - stTime)
      #      tweetsPersecond=((totalTweetCnt-reTweetcnt)/time)*1000
      #      reTweetPersecond=(reTweetcnt/time)*1000
      #      searchPresecond=(searchCount/time)*1000
      #      IO.puts "total tweet handled by server persecond: #{tweetsPersecond}"
      #      IO.puts "total retweet handled by server per second: #{reTweetPersecond}"
      #      IO.puts "total Searches handled by server per second: #{searchCount}"
      #      IO.puts "total live connecteions: #{nUsers-noOfdisConn}"
      #      IO.puts "The time taken by #{nUsers} users is #{time} milliseconds"
      #      IO.puts ""
      # end
     
      {:noreply, [nUsers, followersMap, actorsList, displayInterval, new_tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end


  def handle_cast({:updateFollowersTweetQ, userFollowersList, i, tweet}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do

    if i < length(userFollowersList) do 
      k = Enum.at(userFollowersList, i)
      userPID = Enum.at(actorsList, k)
      GenServer.cast :gen_main, {:tweet, tweet, k, "populate"}
      GenServer.cast :gen_main, {:updateFollowersTweetQ, userFollowersList, i+1, tweet}
    end
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end

  #check if this is required
  def handle_call({:getUserFollowers, userId}, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
    {:reply, length(Map.get(followersMap, userId)), [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt+1, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
  end 


  @doc """
  For retweeting the tweets
  """
  def handle_cast({:retweet, tweet, userId}, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
    GenServer.cast :gen_main, {:tweet, tweet, userId, "tweet"}
    # IO.puts "User#{userId} retweeting"
    {:noreply, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt+1,searchCount,noOfdisConn]}
  end

  def handle_call(:getFollowers, _from, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]) do
    {:reply, followersMap, [nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt+1, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn]}
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

          if Enum.at(k, 0)=="client" and Enum.count(k) == 2 do
            Connection.initClientNode
            {nUsers,  _} = Integer.parse(Enum.at(k,1))
            serverId=:global.whereis_name(:gen_main)
            # IO.inspect pid
            IO.puts "Simulation started..."
            IO.puts "View simulation statistics on server window..."
            IO.puts "User creation started.."
            createNodes(nUsers, 0,serverId)
          else
            if Enum.at(k, 0)=="server" and Enum.count(k) == 2 do
              {nUsers,  _} = Integer.parse(Enum.at(k, 1))
              Connection.initServerNode
              IO.puts "Server node created"
              followersMap = %{}
              actorsMap = %{}
              actorsList = []
              displayInterval = 100
              tweetsQueueMap = %{}
              searchMap = %{}
              totalTweetCnt = 0
              maxTweetCnt  = 10000
              reTweetcnt=0
              searchCount=0
              noOfdisConn=0
              stTime = :os.system_time(:millisecond)
              {:ok,server_pid}=Server.start_link(nUsers, followersMap, actorsList, displayInterval, tweetsQueueMap, searchMap, totalTweetCnt, maxTweetCnt, stTime,reTweetcnt,searchCount,noOfdisConn)
              IO.puts "Simulator started..."
              IO.puts "you will see simulator satisics every three seconds..."
              # GenServer.call serverId,:hell
              :global.register_name(:gen_main, server_pid)
              IO.inspect server_pid
              Server.zipf(nUsers)
              # GenServer.cast server_pid,{:zipf,nUsers}
            else
              IO.puts "please provide 'server' or 'client' with nUSers"
              System.halt(0)
            end  
          end
        loop()
    end
    def loop do
      loop()
    end
    
    def createNodes(nUsers, i,serverId) do
      if i<nUsers do
        userId = i
        tweetQueue = :queue.new
        nUsers = nUsers
        retweetCount = 0
        # if Process.alive?serverId do
        #   followerMapSize = GenServer.call serverId, {:getUserFollowers,userId}
        # else
        #   IO.puts "Server not up"
        #   System.halt(0)
        # end  
        followerMapSize = GenServer.call serverId, {:getUserFollowers,userId}
        divf =  round(Float.ceil((followerMapSize/nUsers)*100))
        displayInterval=cond do
          divf<=1 -> 1000
          divf<=5 -> 500
          divf<=10 ->300
          divf<=20 ->150
          divf<=30 -> 30
          divf<=40 -> 5
          divf<=50 -> 3
          true -> 1
        end
        # if((i/nUsers)==0.25)do
        #    IO.puts "25 % users created..."
        # end
        # if((i/nUsers)==0.5)do
        #    IO.puts "50 % users created..."
        # end
        # if((i/nUsers)==0.75)do
        #    IO.puts "75 % users created..."
        # end
        # if((i/nUsers)==1.0)do
        #    IO.puts "100 % users created..."
        # end
        
        state = 1 # 0 - disconnected , 1 - connected
        Client.start_link(userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId)
        createNodes(nUsers, i+1,serverId)
      else
       IO.puts "100% users created"
       IO.puts "simulation in progress..."
      end 
    end
end


defmodule Client do
  use GenServer
    @doc """
    Client Process is started using start_link
    """
    def init(state) do
        schedule(10)
        {:ok, state}
    end

    def schedule(di)do
        # di=GenServer.call self, :getDisplayInterval
        # IO.puts "#{di}"
        Process.send_after(self(), :sendRepeatedTweets, di)
        Process.send_after(self(), :reTweet, di+10)
        Process.send_after(self(), :searchHashTagOrMentions,di+5)
        Process.send_after(self(), :changeState,di+10)
    end
    
    
    def start_link(userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId) do
      {_, pid} = GenServer.start_link(__MODULE__, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId])
      # GenServer.cast serverId, {:hello} 
       GenServer.cast serverId, {:updateActors, pid} 
      # if Process.alive?(serverId)do
      #   GenServer.cast serverId, {:updateActors, pid} 
      # else
      #   System.halt(0)
      # end  
    end
    
    def handle_call(:getUserId, _from, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]) do
      {:reply, userId, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]}
    end

    
    def handle_cast(:updateDisplayInterval, _from, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]) do
      divf =  round((followerMapSize/nUsers)*100) 
      rem = Integer.mod(divf,10)
      divf = divf-rem
      displayInterval = 90 - divf
      {:reply, displayInterval, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]}
    end

    def handle_call(:getDisplayInterval, _from, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]) do
      {:reply, displayInterval, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]}
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
    Helper function to send tweets at a regular interval to the Server
    runs in a infinite loop
    """
    def handle_info(:sendRepeatedTweets, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]) do
        # IO.puts "sendRepeatedTweets #{userId}"
        if state == 0 do
          # :timer.sleep(1000)
          # Process.sleep(10000)
        else 
          schedule(displayInterval)
          # IO.puts "sendRepeatedTweets #{userId}"
          tweetMsg = tweetMsgGenerator(nUsers)
          GenServer.cast serverId, {:tweet, tweetMsg, userId, "tweet"}
        end
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]}
    end

    def handle_info(:reTweet, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]) do
        #to do : has to be removed after Process.start_after is fixed
        # IO.puts "sendRepeatedTweets #{userId}"
        if state == 0 do
          # :timer.sleep(100)
          # Process.sleep(1000)          
        else 
            schedule(displayInterval)
            # IO.puts "sendRepeatedTweets #{userId}"
            if :queue.is_empty(tweetQueue)== false do
              tweetMsg = :queue.get(tweetQueue)
              # IO.puts "retweet #{tweetMsg}"
              GenServer.cast serverId, {:retweet, tweetMsg, userId}
            end  
        end
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]}
    end
    
    
    @doc """
    Makes a connection live or dead at a regular interval
    """  
    def handle_info(:changeState, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]) do
        #IO.puts "beforestate:: #{state}"
          schedule(displayInterval+10)
          if state == 0 do 
               state=1
              #  IO.puts "User#{userId} woke up"
               newTweetQ = GenServer.call serverId, {:getTweetQ, userId}
               GenServer.cast serverId,{:updateNoofDisconn,1}
              #  if(newTweetQ==nil)do
              #   IO.puts "nil fonksdakv.csdaadsfefhe.lkjfqelkf"
              #   System.halt(0)
              #  end
          # IO.puts "#{inspect newTweetQ}"
          else
               state=0
               GenServer.cast serverId,{:updateNoofDisconn,0}

              #  IO.puts "User#{userId} sleeping"
          end
        #IO.puts "afterstate #{state}"
        {:noreply,  [userId, newTweetQ, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]}
    end

    @doc """
    Searches random hashtag or mentions at regular inteval
    """    
    def handle_info(:searchHashTagOrMentions, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]) do
      # IO.puts "sendRepeatedTweets #{userId}"
      schedule(displayInterval+10) 
      if state !=0 do
        list=[Enum.join(["#Hashtag",:rand.uniform(10)]),Enum.join(["@user",:rand.uniform(10)])]
        hashTagOrMention =Enum.at(list,:rand.uniform(2)-1)
        GenServer.call serverId,{:searchHashTag, hashTagOrMention}
        GenServer.cast serverId,{:updateNoSearch}
      end  
      {:noreply, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]}
    end


    @doc """
    receiveTweet receives tweets and saves it to it's own tweet queue 
    """
    def handle_cast({:receiveTweet,tweetMsg}, [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]) do
        if state != 0 do
          #The maximum number of tweets that can be in the tweet queue is 100
          tweetQueue=if :queue.len(tweetQueue)> 100 do
              :queue.in(tweetMsg, :queue.drop(tweetQueue))
          else
              :queue.in(tweetMsg, tweetQueue)
          end
        end
        # IO.puts "#{inspect tweetQueue}"
        {:noreply,  [userId, tweetQueue, nUsers, retweetCount, followerMapSize, displayInterval, state,serverId]}
    end
end


defmodule Connection do
  def initClientNode() do
      {_,addresses}=:inet.getif()
      # IO.inspect addresses
      head_adress=to_string(elem(elem(Enum.at(addresses,0),0),0))<>"."<> to_string(elem(elem(Enum.at(addresses,0),0),1))<>"."<>to_string(elem(elem(Enum.at(addresses,0),0),2))<>"."<> to_string(elem(elem(Enum.at(addresses,1),0),3))
      server_address=to_string(elem(elem(Enum.at(addresses,0),0),0))<>"."<> to_string(elem(elem(Enum.at(addresses,0),0),1))<>"."<>to_string(elem(elem(Enum.at(addresses,0),0),2))<>"."<> to_string(elem(elem(Enum.at(addresses,0),0),3))
      fullname="client1"<>"@"<>to_string(head_adress)
      # IO.inspect head_adress
      # fullname="client1"
      Node.start(:"#{fullname}")
      Node.set_cookie :hello
      # servername="server1@192.168.0.2"
      servername="server1"<>"@"<>to_string(server_address)
      Node.connect :"#{servername}"
      :global.sync()  
  end 
  def initServerNode do
      {_,addresses}=:inet.getif()
      # IO.inspect addresses
      head_adress=to_string(elem(elem(Enum.at(addresses,0),0),0))<>"."<> to_string(elem(elem(Enum.at(addresses,0),0),1))<>"."<>to_string(elem(elem(Enum.at(addresses,0),0),2))<>"."<> to_string(elem(elem(Enum.at(addresses,0),0),3))
      # IO.inspect head_adress
      fullname="server1"<>"@"<>to_string(head_adress)
      # IO.inspect fullname
      Node.start(:"#{fullname}")
      Node.set_cookie :hello
  end   
end