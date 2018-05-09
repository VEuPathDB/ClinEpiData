#!/usr/bin/env perl
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ClinEpiData::Load::Owl;

use File::Basename qw/basename/;
use Env qw/PROJECT_HOME/;
use XML::Simple;
my $dataset = shift @ARGV;

my $owlFile = "$PROJECT_HOME/ApiCommonData/Load/ontology/release/development/$dataset.owl";

my $owl = ClinEpiData::Load::Owl->new($owlFile);
my $it = $owl->execute('get_column_sourceID');
my %terms;
while (my $row = $it->next) {
	my $iri = $row->{entity}->as_hash()->{iri};
	my $names = $row->{vars}->as_hash()->{literal};
	my $name = "";
	if(ref($names) eq 'ARRAY'){
		$name = $names->[0];
	}
	else {
		$name = $names;
		$names = [ $name ];
	}
	my $sid = basename($iri); 	
  $terms{$name} = { 'source_id' => $sid, 'name' =>  $names, 'type' => 'characteristicQualifier', 'parent'=> 'ENTITY' };
}
my @sorted = map { $terms{$_} } sort keys %terms;
## add top level 
unshift(@sorted, { source_id => 'OBI_0600004', type => 'protocol', name => [ 'enrollment' ] }); 
unshift(@sorted, { source_id => 'BFO_0000015', type => 'protocol', name => [ 'observationProtocol' ] }); 

$it = $owl->execute('top_level_entities');

while (my $row = $it->next) {
	my $iri = $row->{entity}->as_hash()->{iri};
	my $sid = basename($iri); 	
	my $name = $row->{label} ? $row->{label}->as_hash()->{literal} : "";
	unshift(@sorted, 
		{ 'source_id' => $sid, 'name'=>  [ $name ], 'type'=> 'materialType', 'parent' => 'ENTITY' }
	);
}

my $xml = {
  ontologymappings => [
    {
      ontologyTerm => \@sorted
    }
  ]
};
print XMLout($xml, KeepRoot => 1, AttrIndent => 0);


