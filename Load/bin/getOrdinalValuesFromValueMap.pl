#!/usr/bin/env perl
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ClinEpiData::Load::Utilities::File qw/csv2array tabWriter/;
use Scalar::Util qw/looks_like_number/;
use Data::Dumper;
use JSON qw/to_json/;

my ($infile) = @ARGV;

my $valueMap = csv2array($infile, "\t");

my %perms;

my $_line = 0;
foreach my $row( @$valueMap ){
  $_line++;
  my ($var, $iri, $orig, $val, $order) = @$row;
  next unless (defined($iri) && defined($val) && defined($order) && $order ne "");
  unless(looks_like_number($order)){
    printf STDERR ("ERROR: LINE $_line: '$order' is not a number, skipping\n");
    next;
  }
  # warn if already inserted with a different ordering
  if(defined($perms{$iri}->{ $val }) && $perms{$iri}->{ $val } != $order){
    printf STDERR ("ERROR: LINE $_line: $iri $val does not match previous: %s\n", $perms{$iri}->{$val});
  }
  $perms{$iri}->{$val} = $order;
}

if(0 < keys %perms){
  printf("%s\n", join("\t", qw/SOURCE_ID ordinal_values/));
  while( my ($iri, $hash) = each %perms ){
    my @perm =  sort { $hash->{$a} <=> $hash->{$b} } keys %$hash;
    my $json = to_json(\@perm);
    printf("%s\t%s\n", $iri, $json);
  }
}
  


