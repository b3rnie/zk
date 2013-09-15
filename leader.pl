#!/usr/bin/perl -w
##
## Leader election from the ZooKeeper cookbook
##
use strict;
use Net::ZooKeeper qw(:node_flags :acls :errors :log_levels);
$|++;

#my $HOST    = "192.168.1.169";
my $HOSTS   = "localhost:2181";
my $ROOT    = '/ELECTION';
my $SERVICE = "meh";

## Net::ZooKeeper::set_log_level(ZOO_LOG_LEVEL_DEBUG);

# Connection and 10 second heartbeat
my $zkh = Net::ZooKeeper->new($HOSTS,
                              watch_timeout   => 10000,
                              session_timeout => 10000
                             ) or die "Connection failed: $!";

# Make sure root exists
unless($zkh->exists($ROOT)){
  unless($zkh->create($ROOT, '','acl' => ZOO_OPEN_ACL_UNSAFE)){
    if($zkh->get_error() != ZNODEEXISTS){
      die "Creating $ROOT failed: " . $zkh->get_error();
    }
  }
}

# Tell the world I want to be the leader
my $z = $zkh->create($ROOT.'/'.$SERVICE.'_',
                     '',
                     'flags' => ZOO_EPHEMERAL | ZOO_SEQUENCE,
                     'acl'   => ZOO_OPEN_ACL_UNSAFE
                    )
  or die "Creating ". $ROOT.'/'.$SERVICE.'_ failed: '.$zkh->get_error();
my $my_seqno = (split(/\/|_/, $z))[3];

print "Got sequence number " . $my_seqno . "\n";

# Monitor largest seqno smaller than my own
for(;;){
  my $monitor_seqno = undef;
  my @paths         = $zkh->get_children($ROOT) or die
      "Get children failed: " . $zkh->get_error();
  foreach my $path(@paths){
    my($service, $seqno) = split(/_/, $path);
    # Dont look at other services
    next if $service ne $SERVICE;
    if(defined $monitor_seqno &&
       $seqno > $monitor_seqno &&
       $seqno < $my_seqno){
      $monitor_seqno = $seqno;
    }elsif(!defined $monitor_seqno && $seqno < $my_seqno){
      $monitor_seqno = $seqno;
    }
  }
  if(defined $monitor_seqno){
    print "Monitoring sequence number" . $monitor_seqno . "\n";
    my $watch = $zkh->watch(timeout => 60000);
    if($zkh->exists($ROOT . '/' . $SERVICE . '_' . $monitor_seqno,
                    'watch' => $watch)){
      $watch->wait(timeout => 60000);
      # Timed out or something happened
    }
  }else{
    # I am the leader
    last;
  }
}

print "Becoming leader\n";

##
## This is very fucked up
##
## If the connection is lost the C library obviously knows it (and tries
## to reconnect which may or may not be what one wants..)
## The lost connection is not passed to me from what I can understand
## therefore I am left to poll and look for errors...
for(;;){
  my $watch = $zkh->watch('timeout' => 1000);
  if($zkh->exists($z, 'watch' => $watch)){
    $watch->wait(timeout => 1000);
  }else{
    print "Stepping down as leader: " . print $zkh->get_error();
    exit(-1);
  }
}
