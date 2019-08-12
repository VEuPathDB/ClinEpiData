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
  'f|functionName',
	'd|dump (print sorted xml)') . "\n";
	exit;
}


my ($xmlFile,$sourceIdFile,$functionName,$dump);
GetOptions(
  'o|ontologyXmlFile=s' => \$xmlFile,
  's|sourceIdFile=s' => \$sourceIdFile,
  'f|functionName=s' => \$functionName,
	'd|dump' => \$dump
);

$xmlFile ||= '-';

my $xml = XMLin($xmlFile, ForceArray => 1, KeepRoot => 1);

if($dump){ ## print functions and exit
	foreach my $root ( @{$xml->{ontologymappings}} ) {
	  foreach my $term ( @{$root->{ontologyTerm}} ) {
			printf("%s\t%s\n", $term->{name}->[0], join("\t", @{$term->{function} || [] })) if $term->{function};
		}
	}
	exit;
}
	

my %sourceIds;
my %missingIds;
my %funcToAdd;
if($sourceIdFile){
	open(FH, "<$sourceIdFile") or die "Cannot read $sourceIdFile:$!\n";
	while(my $line = <FH>){
	  chomp $line;
	  my($sid, @funcs) = split(/\t/, $line);
	
	  $sid = lc $sid;
	  if(0 < @funcs){
			$funcToAdd{$sid} ||= {};
			foreach my $func (@funcs){
				$funcToAdd{$sid}->{$func} = 1;
			}
		}
	  elsif ($functionName) {
			$funcToAdd{$sid} ||= {};
	    $funcToAdd{$sid}->{$functionName} = 1;
	  }
	  $sourceIds{$sid} = 1; 
	  $missingIds{$sid} = 1;
	}
	close(FH);
}

foreach my $root ( @{$xml->{ontologymappings}} ) {
  foreach my $term ( @{$root->{ontologyTerm}} ) {

    my $sourceId = $term->{source_id};

    my $names = $term->{name};
    my @names = sort map { lc } @$names;
		$term->{name} = \@names;

    my @ids = @names;
		push(@ids, lc($sourceId));

    foreach my $id (@ids) {
      if($sourceIds{$id}){
        if($funcToAdd{$id}){
					my @funcs = keys %{$funcToAdd{$id}};
          my $list = uniq( $term->{function} || [], \@funcs );
          $term->{function} = $list;
        }
        delete $missingIds{$term->{source_id}};
      }
    }
  }
}

my @sorted;
foreach my $type (qw/materialType protocol characteristicQualifier/){
	my %seen; ## hold references to terms indexed by source_id
	my @types;
	my $index = 0;
	foreach my $term (  @{ $xml->{ontologymappings}->[0]->{ontologyTerm} } ){
		unless($term->{type} eq $type){
			$index++;
			next;
		}
		my $sid = $term->{source_id};
		my @names = $term->{name};
		unless(defined($seen{$sid})){
			$seen{$sid} = $index;
			if($term->{type} eq $type){
				push(@types, $index);
			}
		}
		else{ ## remove multiple instances of a source id, add its names to the seen one 
			# print STDERR "Sid = $sid, $seen{$sid}\n";
			my %hash;
			map { $hash{$_} = 1 } @{ $xml->{ontologymappings}->[0]->{ontologyTerm}->[$seen{$sid}]->{name} };
			map { $hash{$_} = 1 } @{ $term->{name} };
			my @names = sort keys %hash;
			# $term->{name} = \@names; ## should work because $term is a reference
			$xml->{ontologymappings}->[0]->{ontologyTerm}->[$seen{$sid}]->{name} = \@names;	
		}
		$index++;
	}
	printf STDERR ("Found %d of type %s\n", scalar keys %seen, $type);
	push(@sorted, sort { $a->{name}->[0] cmp $b->{name}->[0] } map { $xml->{ontologymappings}->[0]->{ontologyTerm}->[$_] } @types);
}
$xml->{ontologymappings}->[0]->{ontologyTerm} = \@sorted;
# print Dumper $xml;
print XMLout($xml, KeepRoot => 1, AttrIndent => 0);
# printf STDERR ("Not found in %s:\n%s\n", $xmlFile, join("\n", map {"$_\t$missingIds{$_}"} sort keys %missingIds)) if (0 < scalar keys %missingIds);





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
