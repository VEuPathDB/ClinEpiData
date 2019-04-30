#!/usr/bin/env perl
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ApiCommonData::Load::OwlReader;

use File::Basename qw/basename dirname/;
use Env qw/PROJECT_HOME/;
use XML::Simple;
use Getopt::Long;

my ($owlFile,$functionsFile,%funcToAdd);
unless(0 < @ARGV){
	my $scr = basename($0);
	print join(" ", $scr, 
  'o|owlFile',
  'f|functionsFile') . "\n";
	exit;
}

GetOptions(
  'o|owlFile=s' => \$owlFile,
  'f|functionsFile=s' => \$functionsFile
);

unless( -d $PROJECT_HOME){
	print "\$PROJECT_HOME must be set\n";
	exit;
}

unless( -f $owlFile ){
	my $tmp = "$PROJECT_HOME/ApiCommonData/Load/ontology/release/production/$owlFile.owl";
	if(-f $tmp){
		$owlFile = $tmp;
	}
	else{
		opendir(DH, dirname($tmp));
		my @owls = grep { /\.owl$/i } readdir(DH);
		close(DH);
		print STDERR "Error: $owlFile does not exist\n";
		printf STDERR ("Error: %s does not exist\nAvailable owl files in %s:\n%s\n",
			$owlFile, dirname($tmp), join("\n", @owls));
		exit;
	}
}

if($functionsFile){
	open(FH, "<$functionsFile") or die "Cannot read $functionsFile:$!\n";
	my $rank = 1;
	while(my $line = <FH>){
	  chomp $line;
	  my($sid, @funcs) = split(/\t/, $line);
	  $sid = lc $sid;
	  if(0 < @funcs){
			$funcToAdd{$sid} ||= {};
			foreach my $func (@funcs){
				$funcToAdd{$sid}->{$func} = $rank;
				$rank += 1;
			}
		}
	}
	close(FH);
}

my $owl = ApiCommonData::Load::OwlReader->new($owlFile);
my $it = $owl->execute('get_column_sourceID');
my %terms;
while (my $row = $it->next) {
	my $iri = $row->{entity}->as_hash()->{iri}|| $row->{entity}->as_hash()->{URI};
	my $names = $row->{vars}->as_hash()->{literal};
	my $name = "";
	if(ref($names) eq 'ARRAY'){
		$name = lc($names->[0]);
	}
	else {
		$name = lc($names);
		if($name =~ /,/){
			my @splitnames = split(/\s*,\s*/, $name);
			$names = \@splitnames;
		}
		else {
			$names = [ $name ];
		}
	}
	my $sid = $owl->getSourceIdFromIRI($iri); 	
	if(defined($terms{$sid})){
		my %allnames;
		for my $n (@$names){
			$allnames{$n} = 1;
		}
		for my $n (@{ $terms{$sid}->{name} } ){ 
			$allnames{$n} = 1;
		}
		@$names = sort keys %allnames;
	}
	my %funcHash;
	my $rank = 1;
	foreach my $id (map { lc } ($sid, @$names)){
    if($funcToAdd{$id}){
			foreach my $func ( keys %{$funcToAdd{$id}} ){
				$funcHash{$func} = $funcToAdd{$id}->{$func};
			}
    }
	}
	my @funcs = sort { $funcHash{$a} <=> $funcHash{$b} } keys %funcHash;
  $terms{$sid} = { 'source_id' => $sid, 'name' =>  $names, 'type' => 'characteristicQualifier', 'parent'=> 'ENTITY', 'function' => \@funcs };
}
my @sorted = sort { $a->{name}->[0] cmp $b->{name}->[0] } values %terms;

## Protocols are "edges" between an entity type and its parent, as in;
## Participant is an output of Household (participant->household).
## Add all known protocols, though all may not be needed
## Protocol for participant->household:
unshift(@sorted, { source_id => 'OBI_0600004', type => 'protocol', name => [ 'enrollment' ] }); 
## Protocol for observation->participant:
unshift(@sorted, { source_id => 'BFO_0000015', type => 'protocol', name => [ 'observationProtocol' ] }); 
## Protocol for sample->observation:
unshift(@sorted, { source_id => 'OBI_0000659', type => 'protocol', name => [ 'specimen collection' ] }); 
unshift(@sorted, { source_id => 'EUPATH_0000055', type => 'protocol', name => [ 'lightTrap' ] }); 

## Add top level  entities as 'materialType' ontologyTerms (Household, Participant, etc)
$it = $owl->execute('top_level_entities');

while (my $row = $it->next) {
	my $iri = $row->{entity}->as_hash()->{iri};
	my $sid = basename($iri); 	
	my $name = $row->{label} ? $row->{label}->as_hash()->{literal} : "";
	unshift(@sorted, 
		{ 'source_id' => $sid, 'name'=>  [ lc($name) ], 'type'=> 'materialType' }
	);
}


my @manualAdditions = (["INTERNAL_X","materialType", "INTERNAL"],
    );
foreach my $row (@manualAdditions) {
    my $sourceId = $row->[0];
    my $type = $row->[1];
    my $name = $row->[2];

    unshift(@sorted, { source_id => $sourceId, type => $type, name => [ lc($name) ] }); 
    
}



my $xml = {
  ontologymappings => [
    {
      ontologyTerm => \@sorted
    }
  ]
};
print XMLout($xml, KeepRoot => 1, AttrIndent => 0);

