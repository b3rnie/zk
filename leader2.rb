#!/usr/bin/ruby2.0
require 'zookeeper'
require 'json'

class AsMaster
  @@root = '/ELECTION'

  def initialize(service)
    @service = service
    @zk      = connect()
    @zk.create(:path => '/ELECTION')
  end

  def run(&x)
    my_seqno = create_me()
    become_leader(my_seqno)
    Thread.abort_on_exception = true
    t = Thread.new do
      x.call
    end
    while res = @zk.stat(:path => "#{@@root}/#{@service}_#{my_seqno}")
      break unless res[:stat].exists?
      break unless t.alive?
      sleep 1
    end
  end

  private

  def become_leader(my_seqno)
    while true
      waiting = @zk.get_children(:path => @@root)[:children].select { |e|
        e.split('_')[0] == @service
      }
      monitor = waiting.inject(nil) { |monitor, e|
        seqno = e.split('_')[1]
        if monitor && seqno > monitor && seqno < my_seqno
          seqno
        elsif !monitor && seqno < my_seqno
          seqno
        else
          monitor
        end
      }

      if monitor
        q   = Queue.new
        res = @zk.stat(:path    => "#{@@root}/#{@service}_#{monitor}",
                       :watcher => Zookeeper::Callbacks::WatcherCallback.new{
                         q.push(:done)
                       })
        q.pop() if res[:stat].exists?
      else
        break
      end
    end
  end

  def connect()
    Zookeeper.new('192.168.0.20:2181')
  end

  def create_me()
    r = @zk.create(:path => "/ELECTION/#{@service}_",
                   :ephemeral => true,
                   :sequence  => true)
    r[:path].split('_')[1]
  end
end

m = AsMaster.new('service')
m.run do
  puts "I am master!"
  sleep 5
end
