defmodule Main do

    def main(args) do
        args |> parse_args  
    end
    
    defp parse_args([]) do
        IO.puts "No arguments given. Enter the number of clients"
    end
    defp parse_args(args) do
        {_,k,_} = OptionParser.parse(args)
        if Enum.count(k) == 1 do
          {nUsers, _} = Integer.parse(Enum.at(k,0))
        else
            IO.puts "please provide exactly one argument"
            System.halt(0)
        end    


        # Start server
        # Server.start_link()
        Test2.zipf(nUsers)
        # Test.start_link()
        
        # start nUser number of client processes
        # client_pids=Enum.map(1..100000, fn (_) -> Client.start_link("userid",:queue.new,nUsers,0,0,0) end)
        # IO.puts "#{inspect client_pids}"

        # after the clients are created use this to start sending repeated tweets
        # GenServer.cast pid,{:sendRpeatedtweets} 
        



        # Server need to call this to send tweets to every follower
        # need to call with client pid
        # Enum.each client_pids , fn {_,pid} -> GenServer.cast pid,{:receivetweet,"tweetMsg"} end

        :timer.sleep(30000)
    end
   
end

defmodule Server do
    use GenServer

  @doc """
  Client Process is started using Start_Link
  """
  def start_link() do
    GenServer.start_link(__MODULE__,[], name: :genMain ) 
  end

 
 def handle_cast({:tweet,tweetMsg},[])do
    # IO.puts "hi"
    {:noreply,[]}
 end
  #Server need to have handle_cast with {:tweet,tweetMsg} for the tweet opration
  #This will be called from client while sending tweet and retweet
  
end

defmodule Client do
  use GenServer
    def init(state) do
        schedule()
        {:ok, state}
    end
    def schedule()do
        Process.send_after(self(), :sendtweet1, 20)
    end
    @doc """
    Client Process is started using Start_Link
    """
    def start_link(userid,msgQueue,nUsers,retweetCount,followerMapSize,displayInterval) do
     GenServer.start_link(__MODULE__,[userid,msgQueue,nUsers,retweetCount,followerMapSize,displayInterval]) 
    end
    



    @doc """
    Handle_cast for sending Tweet to server
    Simulator will generate and desired tweets and call this method with client pid to send tweets
    """
    def handle_cast({:sendtweet},[userid,msgQueue,nUsers,retweetCount,followerMapSize,displayInterval]) do
        # GenServer.call :genMain,{:tweet}
        # IO.puts "#{inspect msgQueue}"
        tweetMsg=tweetMsgGenerator(5)
        GenServer.cast :genMain,{:tweet,tweetMsg}
        {:noreply, [userid,msgQueue,nUsers,retweetCount,followerMapSize,displayInterval]}
    end

    def handle_info(:sendtweet1, state) do
        schedule()
        tweetMsg=tweetMsgGenerator(5)
        GenServer.cast :genMain,{:tweet,tweetMsg}
        {:noreply, state}
    end


    @doc """
    Method to generate random tweets
    """
    def tweetMsgGenerator(nUsers) do
        if(nUsers>10) do
            IO.puts "#{:rand.uniform(10)}"
            Enum.join(["#Hashtag", :rand.uniform(10)," ","@user",:rand.uniform(10)])
        else
            Enum.join(["#Hashtag", :rand.uniform(nUsers)," ","@user",:rand.uniform(nUsers)])
        end
    end 




    @doc """
    Sends repeated tweets in regular interval
    """
    def handle_cast({:sendRpeatedtweets},[userid,msgQueue,nUsers,retweetCount,followerMapSize,displayInterval]) do
        sendRpeatedtweets(tweetMsgGenerator(5),8)
        {:noreply, [userid,msgQueue,nUsers,retweetCount,followerMapSize,displayInterval]}
    end



    
    @doc """
    Helper function to send tweets at a regular interval to the Server
    runs in a infinite loop
    """
    def sendRpeatedtweets(tweetMsg,n)do
            # IO.puts "hello"
        GenServer.cast :genMain,{:tweet,tweetMsg}
        :timer.sleep(20)
        tweetMsg=tweetMsgGenerator(5)
        sendRpeatedtweets(tweetMsg,n)        
    end



    @doc """
    Handle_cat to save received tweets in the Message queue and retweet in regular interval
    runs in a infinite loop
    """


    def handle_cast({:receivetweet,tweetMsg},[userid,msgQueue,nUsers,retweetCount,followerMapSize,displayInterval]) do
        msgQueue=if :queue.len(msgQueue)> 10 do
            :queue.in(tweetMsg,:queue.drop(msgQueue))
            # IO.puts "morethan 10 tweet received"
        else
            :queue.in(tweetMsg,msgQueue)
            # IO.puts "lessthan 10 tweet received"
        end
        #retweets if the retweetcounter is multple of 5
        retweetCount=retweetCount+1
        # if(rem(retweetCount,5)==0)do
        #     GenServer.cast :genMain,{:tweet,tweetMsg}
        # end
        
        # IO.puts "#{inspect msgQueue}"
        {:noreply, [userid,msgQueue,nUsers,retweetCount,followerMapSize,displayInterval]}
    end

    # def handle_cast({:query},[userid,msgQueue,nUsers,retweetCount,followerMapSize,displayInterval]) do
    #     GenServer.cast pid,{:sendRpeatedtweets}
    #     {:noreply, [userid,msgQueue,nUsers,retweetCount,followerMapSize,displayInterval]}
    # end



end