#!/usr/bin/ruby2.0
require 'zk'
require 'json'

zk = ZK.new('localhost:2181')

unless zk.exists?('/ELECTION')
  zk.create('/ELECTION')
  # race..
end

res = zk.create('/ELECTION/service_', '', :mode => :ephemeral_sequential)

seqno_me = res.split('_')[1]

while true
  seqno_mon = nil
  zk.children('/ELECTION').each{|n|
    (service, seqno) = n.split('_')
    if service=='service' && seqno_mon!=nil && seqno > seqno_mon && seqno < seqno_me
      seqno_mon = seqno
    elsif service=='service' && seqno_mon==nil && seqno < seqno_me
      seqno_mon = seqno
    end
  }

  if seqno_mon
    puts "monitoring " + seqno_mon
    s = '/ELECTION/service_' + seqno_mon
    while zk.exists?(s)
      sleep 1
    end
  else
    # shitty heartbeat..
    Thread.abort_on_exception = true
    Thread.new do
      while zk.exists?('/ELECTION/service_' + seqno_me)
        sleep 1
      end
      raise "wtf"
    end
    puts "I AM LEADER"
    sleep(300)
    exit(0)
  end
end
