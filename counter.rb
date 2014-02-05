#!/usr/bin/ruby2.0
require 'zookeeper'
require 'json'

class Counter
  @@root = '/COUNTER'

  def initialize(name, data=0)
    @path = "#{@@root}/#{name}"
    @zk   = connect()
    @zk.create(:path => @@root)
    @zk.create(:path => @path, :data => data.to_s)
  end

  def inc()
    update(+1)
  end

  def dec()
    update(-1)
  end

  private

  def update(n)
    begin
      znode = @zk.get(:path => @path)
      raise "node does not exist" unless znode[:stat].exists
      res   = @zk.set(:path    => @path,
                      :version => znode[:stat].version,
                      :data    => (znode[:data].to_i + n).to_s)
    end while not res[:stat].exists
    znode[:data].to_i
  end

  def connect()
    Zookeeper.new('localhost:2181')
  end
end

c = Counter.new('blah')
puts c.inc
puts c.dec
