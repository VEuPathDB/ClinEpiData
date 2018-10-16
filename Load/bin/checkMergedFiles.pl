#!/usr/bin/env perl
#
# ontologyfile.xml output_file1 [output_file2] ...
#
#
#
#
use strict;
use warnings;
use XML::Simple;
use Data::Dumper;
use File::Basename;
use Getopt::Long;

unless (@ARGV){
	printf("%s", join("\t\n", "Usage:", basename($0) . " ontologyMapping.xml merged-file1 merged-file2 ...", "scans for missing or duplicated fields in merged files", ""));
	exit;
}

my $VERBOSE=0;
GetOptions('v|verbose'=>\$VERBOSE);


my ($ontofile, @outfiles) = @ARGV;

my $xml = XMLin($ontofile, KeepRoot => 1);

my %mapped;

my $data = $xml->{ontologymappings}->{ontologyTerm};

if(ref($data) eq 'HASH'){
  while(my ($id, $term) = each %{$data}){
    next unless defined($id) && defined($term->{source_id}) && ($term->{type} eq 'characteristicQualifier');
    $mapped{lc($id)} = $term->{source_id};
  }
}
elsif(ref($data) eq 'ARRAY'){
  foreach my $term ( @{$data}){
    next unless defined($term->{name}) && defined($term->{source_id}) && ($term->{type} eq 'characteristicQualifier');
    if(ref($term->{name}) eq 'ARRAY'){
      map { $mapped{lc($_)} = $term->{source_id} } @{$term->{name}};
    }
    else {
      $mapped{lc($term->{name})} = $term->{source_id};
    }
  }
}
else{
  die "Cannot parse $ontofile :(\n";
}


my %found;
foreach my $file (@outfiles){
  print STDERR "Reading $file\n";
  open(FH, "<$file");
  my $head = <FH>;
  close(FH);
  chomp $head;
  my @cols = split(/\t/, $head);
  printf STDERR ("%d columns\n", scalar @cols);
  foreach my $col (@cols){
    $col = lc($col);
    $found{$col} ||= [];
    push(@{$found{$col}}, $file);
    unless(defined($mapped{$col})){
      push(@{$found{$col}}, 'UNMAPPED');
    }
    elsif($VERBOSE){
      printf STDERR ("%s mapped in %s\n", $col, $file);
    }
  }
}

foreach my $field (sort keys %mapped){
  if(defined($found{$field})){
		if( 1 < @{$found{$field}} ){
			printf("%s\tDUPLICATED\t%s\n", $field, join("\t", @{$found{$field}}));
		}
  }
  else{
    printf("%s\tMISSING\n", $field);
  }
}
  
  
