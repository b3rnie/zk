#!/usr/bin/ruby2.0
require 'zk'
require 'json'

class AsMaster
  def initialize(service)
    @service = service
    @zk      = ZK.new('localhost:2181')
    unless @zk.exists?('/ELECTION')
      @zk.create('/ELECTION') rescue ZK::Exceptions::NodeExists
    end
    res = @zk.create("/ELECTION/#{@service}_", '', :mode => :ephemeral_sequential)
    @seqno_me = res.split('_')[1]
  end

  def run(&x)
    while true
      seqno_mon = nil
      @zk.children('/ELECTION').each{|n|
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
        while @zk.exists?(s)
          sleep 1
        end
      else
        Thread.abort_on_exception = true
        t = Thread.new do
          x.call
        end
        while @zk.exists?("/ELECTION/#{@service}_" + @seqno_me)
          break unless t.alive?
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
