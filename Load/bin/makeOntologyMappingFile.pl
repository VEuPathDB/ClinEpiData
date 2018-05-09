#!/usr/bin/env perl
use strict;
use warnings;

use RDF::Trine;
use RDF::Query;
use File::Basename qw/basename/;
use Env qw/PROJECT_HOME/;
use XML::Simple;
use Data::Dumper;
use File::Temp qw/tempfile/;
my $dataset = shift @ARGV;

my $base='http://purl.obolibrary.org/obo/';
my $owl = "$PROJECT_HOME/ApiCommonData/Load/ontology/release/development/$dataset.owl";
# my $owl = '/home/toaster/workspace/project_home/ApiCommonData/Load/ontology/release/development/icemr_indian.owl';
# my $owl = '/home/toaster/workspace/project_home/ApiCommonData/Load/ontology/release/development/gates_maled.owl';
my $sparql;
#my $rqfile = "$PROJECT_HOME/ApiCommonData/Load/ontology/release/SPARQL/get_terms.rq";
my $rqfile = "$PROJECT_HOME/ApiCommonData/Load/ontology/release/SPARQL/get_column_sourceID.rq";
open(FH, "<$rqfile");
my @rq = <FH>;
close(FH);
$sparql = join("", @rq);
my $query = RDF::Query->new($sparql);
my $name = basename($owl, '.owl');
#my $dbfile = sprintf('/tmp/%s.sqlite', $name);
my ($dbfn, $dbfile) = tempfile($name. "XXXX", SUFFIX => '.sqlite', DIR => '/tmp', UNLINK => 1);

my $model = RDF::Trine::Model->new(
    RDF::Trine::Store::DBI->new(
        $name,
        "dbi:SQLite:dbname=$dbfile",
        '',  # no username
        '',  # no password
    ),
);
print STDERR ("model created\n");
my $parser = RDF::Trine::Parser->new('rdfxml');
print STDERR ("parser created\n");
$parser->parse_file_into_model($base, $owl, $model);
print STDERR ("db created\n");

print STDERR ("query created\n");
# print STDERR Dumper $sparql;
my $it = $query->execute( $model );
my %terms;
while (my $row = $it->next) {
   # $row is a HASHref containing variable name -> RDF Term bindings
#	printf("%s\n", join("\t", $row->{sid}, $row->{vars}));
# print STDERR Dumper $row;
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
#print "$sid\t$name\n";
  $terms{$name} = { 'source_id' => $sid, 'name' =>  $names, 'type' => 'characteristicQualifier', 'parent'=> 'ENTITY' };
}
# printf("%s\n", join("\n", map { join("\t", $r{$_}->{sid}, $r{$_}->{vars})} sort keys %r)); 

my @sorted = map { $terms{$_} } sort keys %terms;

## add top level 
unshift(@sorted, { source_id => 'OBI_0600004', type => 'protocol', name => [ 'enrollment' ] }); 
unshift(@sorted, { source_id => 'BFO_0000015', type => 'protocol', name => [ 'observationProtocol' ] }); 

$sparql = '
  PREFIX owl: <http://www.w3.org/2002/07/owl#>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

  SELECT DISTINCT ?entity ?label
  WHERE {
    ?entity rdfs:subClassOf <http://www.w3.org/2002/07/owl#Thing> .
    ?entity rdfs:label ?slabel.
    BIND (str(?slabel) AS ?label) .
  }
	';
$query = RDF::Query->new($sparql) or die "$!\n";
$it = $query->execute( $model );
while (my $row = $it->next) {
	print STDERR Dumper $row;
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


