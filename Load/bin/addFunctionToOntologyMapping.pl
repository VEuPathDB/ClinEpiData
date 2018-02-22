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



my ($xmlFile,$sourceIdFile,$functionName);
GetOptions(
  'ontologyXmlFile=s' => \$xmlFile,
  'sourceIdFile=s' => \$sourceIdFile,
  'functionName=s' => \$functionName,
);


my %sourceIds;
my %missingIds;
open(FH, "<$sourceIdFile") or die "Cannot read $sourceIdFile:$!\n";
while(my $line = <FH>){
  chomp $line;
  my @row = split(/\s*,\s*/, $line);
  if(1<@row){
    $sourceIds{$row[1]} = $row[0];
    $missingIds{$row[1]} = $row[0];
  }
  else {
    $sourceIds{$row[0]} = 1;
    $missingIds{$row[0]} = 1;
  }
}
close(FH);

my $xml = XMLin($xmlFile, ForceArray => 1, KeepRoot => 1);

foreach my $root ( @{$xml->{ontologymappings}} ) {
  foreach my $term ( @{$root->{ontologyTerm}} ) {
    my $sid = $term->{source_id};
    if($sourceIds{$sid}){
      if( $term->{name} ){
        my ($name) = @{$term->{name}};
        if($sourceIds{$sid} ne $name){
          printf STDERR ("Discrepency %s %s(from %s) != %s(from %s)\n", $sid, $name, $xmlFile, $sourceIds{$sid}, $sourceIdFile);
        }
        push(@{$term->{function}}, $functionName );
        delete $missingIds{$term->{source_id}};
      }
    }
  }
}
# print Dumper $xml;
print XMLout($xml, KeepRoot => 1, AttrIndent => 0);
printf STDERR ("Not found in %s:\n%s\n", $xmlFile, join("\n", map {"$_\t$missingIds{$_}"} sort keys %missingIds)) if (0 < scalar keys %missingIds);





