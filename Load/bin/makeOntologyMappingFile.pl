#!/usr/bin/env perl
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ApiCommonData::Load::OwlReader;

use File::Basename qw/basename/;
use Env qw/PROJECT_HOME/;
use XML::Simple;
my $dataset = shift @ARGV;

unless( -d $PROJECT_HOME){
	print "\$PROJECT_HOME must be set\n";
	exit;
}
unless($dataset){
	print "Usage: makeOntoloyMappingFile.pl [ontology] > ontologyMapping.xml\n\twhere the file exists \$PROJECT_HOME/ApiCommonData/Load/ontology/release/development/[ontology].owl\n";
	exit;
}
my $owlFile = $dataset;
unless( -f $owlFile ){
	$dataset =  "$PROJECT_HOME/ApiCommonData/Load/ontology/release/development/$dataset.owl";
}
unless(-f $owlFile){
	print "Error: $owlFile does not exist\n";
	exit;
}

my $owl = ApiCommonData::Load::OwlReader->new($owlFile);
my $it = $owl->execute('get_column_sourceID');
my %terms;
while (my $row = $it->next) {
	my $iri = $row->{entity}->as_hash()->{iri};
	my $names = $row->{vars}->as_hash()->{literal};
	my $name = "";
	if(ref($names) eq 'ARRAY'){
		$name = lc($names->[0]);
	}
	else {
		$name = lc($names);
		$names = [ $name ];
	}
	my $sid = basename($iri); 	
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
  $terms{$sid} = { 'source_id' => $sid, 'name' =>  $names, 'type' => 'characteristicQualifier', 'parent'=> 'ENTITY' };
}
my @sorted = sort { $a->{name}->[0] cmp $b->{name}->[0] } values %terms;
## add top level 
unshift(@sorted, { source_id => 'OBI_0600004', type => 'protocol', name => [ 'enrollment' ] }); 
unshift(@sorted, { source_id => 'BFO_0000015', type => 'protocol', name => [ 'observationProtocol' ] }); 

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

