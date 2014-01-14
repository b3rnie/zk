#!/usr/bin/ruby2.0
require 'zookeeper'
require 'json'

class AsMaster
  def initialize(service)
    @service = service
    @zk      = Zookeeper.new('localhost:2181')
    @zk.create(:path => '/ELECTION')
    res = @zk.create(:path => "/ELECTION/#{@service}_", :ephemeral => true, :sequence => true)
    @seqno_me = res[:path].split('_')[1]
  end

  def run(&x)
    while true
      seqno_mon = nil
      @zk.get_children(:path => '/ELECTION')[:children].each{|n|
        (service, seqno) = n.split('_')
        if service==@service && seqno_mon!=nil && seqno > seqno_mon && seqno < @seqno_me
          seqno_mon = seqno
        elsif service==@service && seqno_mon==nil && seqno < @seqno_me
          seqno_mon = seqno
        end
      }

      if seqno_mon
        puts "monitoring " + seqno_mon
        s = "/ELECTION/#{@service}_" + seqno_mon
        q = Queue.new
        w = Zookeeper::Callbacks::WatcherCallback.new{
          q.push(:done)
        }
        res = @zk.stat(:path => s, :watcher => w)
        if res[:stat].exists?
          q.pop()
        end
      else
        Thread.abort_on_exception = true
        t = Thread.new do
          x.call
        end
        while res = @zk.stat(:path => "/ELECTION/#{@service}_" + @seqno_me)
          break unless res[:stat].exists?
          sleep 1
        end
      end
    end
  end
end


m = AsMaster.new('service')
m.run do
  puts "I am master!"
  sleep 5
end
