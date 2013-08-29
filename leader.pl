#!/usr/bin/perl -w
##
## Leader election from the ZooKeeper cookbook
##
use strict;
use Net::ZooKeeper qw(:node_flags :acls :errors);

my $HOST    = "localhost";
my $PORT    = "2181";
my $ROOT    = '/ELECTION';
my $SERVICE = "meh";

# Connection and 10 second heartbeat
my $zkh = Net::ZooKeeper->new($HOST.':'.$PORT,
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
  foreach my $path($zkh->get_children($ROOT)){
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
      $watch->wait();
      # Timed out or something happened
    }
  }else{
    # I am the leader
    last;
  }
}

print "I am the leader!\n";
sleep(20000000000);

