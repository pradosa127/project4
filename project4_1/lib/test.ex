defmodule Test do
    use GenServer
    def init(state) do
        schedule()
        {:ok, state}
    end
    def start_link()do
        GenServer.start_link(__MODULE__, [])
    end

    def print_fruit() do
        IO.puts "hello"
    end
    def print_flower() do
        IO.puts "fl"
    end
    
    def handle_info(:print_fruit, state) do
        schedule()
        print_fruit()
        {:noreply, state}
    end
    def handle_info(:print_flower, state) do
        schedule()
        print_flower()
        {:noreply, state}
    end
    

    def schedule()do
        Process.send_after(self(), :print_fruit, 20)
        Process.send_after(self(), :print_flower, 50)
    end
end



defmodule Test2 do
    @doc """
    implemnets zipf distibution for the number of follwers
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
        end
        
    end

    def createfollowerlist(list,nFollower,nUSers) do
         if nFollower>0 do
            list=list++[:rand.uniform(nUSers)]
            createfollowerlist(list,nFollower-1,nUSers)
         else
            list 
         end    
    end
end