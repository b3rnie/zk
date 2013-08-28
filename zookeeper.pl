#!/usr/bin/perl -w
##
##
use strict;
use Net::ZooKeeper qw(:node_flags :acls);


## path
my $ROOT    = '/ELECTION';

## service name
my $SERVICE = "meh";


my $zkh = Net::ZooKeeper->new('localhost:2181') or die "connection failed: $!";

$zkh->create($ROOT, '','acl' => ZOO_OPEN_ACL_UNSAFE);

my $z = $zkh->create($ROOT.'/'.$SERVICE.'_',
                     '',
                     'flags' => ZOO_EPHEMERAL | ZOO_SEQUENCE,
                     'acl'   => ZOO_OPEN_ACL_UNSAFE
                    ) or die "unable to create " . $ROOT . '/' . $SERVICE . '_: '. $zkh->get_error();

my($root, $service, $i) = split(/_\//, $z);

print "i = ". $i . "\n";

for(;;){
        ## Other
        my $monitor_seqno = undef;
        foreach my $path($zkh->get_children($ROOT)){
                my $fullpath = '/ELECTION/' . $path;
                next unless $fullpath=~/^$service_path(\d+)$/;
                my $seqno = $1;
                if(defined $monitor_seqno){
                        if($seqno > $monitor_seqno && $seqno < $i){
                                $monitor_seqno = $seqno;
                        }
                }else{
                        if($seqno < $i){
                                $monitor_seqno = $seqno;
                        }
                }
        }
        
        if($monitor_seqno){
                print "monitoring " . $monitor_seqno . "\n";
                my $watch = $zkh->watch('timeout' => 10000);
                $zkh->exists($service_path . $monitor_seqno, 'watch' => $watch);
                if ($watch->wait()) {
                        print "watch triggered on node /foo:\n";
                        print "  event: $watch->{event}\n";
                        print "  state: $watch->{state}\n";
                }
        }else{
                print "IS LEADER!!\n";
        }
}






