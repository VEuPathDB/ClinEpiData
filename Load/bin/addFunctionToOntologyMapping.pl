#!/usr/bin/env perl
#
# addFunctionToOntologyMapping.pl --ontologyXmlFile=ontologyMapping.xml --sourceIdFile=sourceIds.txt --functionName=myFooFunction
#
#  --sourceIdFile: 1-column list of ontology source IDs
#  
#  Inserts <function>functionName</function into <ontologyTerm> items and prints result to stdout
#
use strict;
use warnings;
use XML::Simple;
use Getopt::Long;
use Data::Dumper;
use File::Basename;

unless(0 < @ARGV){
	my $scr = basename($0);
	print join(" ", $scr, 
  'o|ontologyXmlFile',
  's|sourceIdFile',
  'f|functionName') . "\n";
	exit;
}


my ($xmlFile,$sourceIdFile,$functionName);
GetOptions(
  'o|ontologyXmlFile=s' => \$xmlFile,
  's|sourceIdFile=s' => \$sourceIdFile,
  'f|functionName=s' => \$functionName,
);

$xmlFile ||= '-';

my $xml = XMLin($xmlFile, ForceArray => 1, KeepRoot => 1);

unless($sourceIdFile || $functionName){ ## print functions and exit
	foreach my $root ( @{$xml->{ontologymappings}} ) {
	  foreach my $term ( @{$root->{ontologyTerm}} ) {
			printf("%s\t%s\n", $term->{source_id}, join(",", @{$term->{function} || [] }));
		}
	}
	exit;
}
	

my %sourceIds;
my %missingIds;
my %funcToAdd;
open(FH, "<$sourceIdFile") or die "Cannot read $sourceIdFile:$!\n";
while(my $line = <FH>){
  chomp $line;
  my($sid, @func) = split(/\t/, $line);

  $sid = lc $sid;
  if(0 < @func){ $funcToAdd{$sid} = \@func; }
  elsif ($functionName) {
    $funcToAdd{$sid} = [ $functionName ];
  }
  $sourceIds{$sid} = 1; 
  $missingIds{$sid} = 1;
}
close(FH);

foreach my $root ( @{$xml->{ontologymappings}} ) {
  foreach my $term ( @{$root->{ontologyTerm}} ) {

    my $sourceId = $term->{source_id};

    my $names = $term->{name};
    my @names = @$names;

    push @names, $sourceId;

    my @lcNames = map {lc $_} @names;

    foreach my $sid (@lcNames) {
      if($sourceIds{$sid}){
        if($funcToAdd{$sid}){

          my $list = uniq( $term->{function} || [], $funcToAdd{$sid});
          $term->{function} = $list;
        }
        delete $missingIds{$term->{source_id}};
      }
    }
  }
}

my @sorted;
foreach my $type (qw/materialType protocol characteristicQualifier/){
	my @types;
	foreach my $term ( @{ $xml->{ontologymappings}->[0]->{ontologyTerm} } ){
		if($term->{type} eq $type){ push(@types, $term) }
	}
	push(@sorted, sort { $a->{name}->[0] cmp $b->{name}->[0] } @types);
}
$xml->{ontologymappings}->[0]->{ontologyTerm} = \@sorted;
# print Dumper $xml;
print XMLout($xml, KeepRoot => 1, AttrIndent => 0);
printf STDERR ("Not found in %s:\n%s\n", $xmlFile, join("\n", map {"$_\t$missingIds{$_}"} sort keys %missingIds)) if (0 < scalar keys %missingIds);





## return a flat list, preserving order
sub uniq {
	my %h;
	my $i = 0; 
	foreach my $list ( @_ ){
		foreach my $item ( @$list ){
			$h{$item} ||= $i;
			$i++;
		}
	}
	my @final = sort { $h{$a} <=> $h{$b} } keys %h;
	return \@final;
}
