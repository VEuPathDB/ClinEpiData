#!/usr/bin/env perl
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ClinEpiData::Load::Owl;
use Env qw/PROJECT_HOME SPARQLPATH/;
use File::Basename;
use Getopt::Long;
use Data::Dumper;

my ($dataset, $filter, @files, $inverse);
GetOptions(
	'o|owl=s' => \$dataset,
	'f|filter=s' => \$filter,
	'i|input=s' => \@files,
	'v|inverse' => \$inverse
);


unless($dataset){
	printf(join("\n\n",
		"Usage:\n\t%s -o|owl [owl] -f|filter [[filter]] -i|input [data file] [ -i [data file ] ] [-v|inverse]",
		"Owl file must exist:\$PROJECT_HOME/ApiCommonData/Load/ontology/release/development/[owl].owl",
		"Run without [[filter]] to see a list of options for this dataset",
		"Run without -i[[data files ...]] to print only columns that are mapped in this dataset",
		"Run with -v to get only columns in [[filter]]\n"
	), basename($0));
	exit;
}

my %columns; ## all columns in data files
my %index; ## col => file
foreach my $file (@files){
	unless(-f $file){
		print STDERR "File does not exist: $file\n";
		next;
	}
	open(FH, "<$file") or die "Cannot open $file:$!\n";
	my $head = <FH>;
	close(FH);
	chomp $head;
	$head =~ s/\r$//;
	my $delim = ",";
	$delim = "\t" if($head =~ /\t/);
	for my $col (split(/$delim/, $head)){
		$columns{pp($col)} = 1;
		$index{pp($col)} = $file;
	} 
}

my $owlFile = "$PROJECT_HOME/ApiCommonData/Load/ontology/release/development/$dataset.owl";

my $owl = ClinEpiData::Load::Owl->new($owlFile);

my $it = $owl->execute('top_level_entities');

my @entities;
my $filterEntity;
my %map;
while (my $row = $it->next) {
	my $label = pp($row->{label}->as_sparql);
	my $entity = $row->{entity}->as_sparql;
	if(defined($filter)){
		if($label =~ /$filter/i){
			$filterEntity = $entity;
			printf STDERR ("$label will be saved\n");
		}
		else{
			push(@entities, $entity);
			printf STDERR ("$label will be excluded\n");
		}
	}
	$map{$label} = $entity;
}

unless($filter){
	printf STDERR ("Choose one of these top-level entities:\n\t%s\n", join("\n\t", sort keys %map));
	exit;
} 

die "Entity not found for $filter\nAvailable:\n\t" . join("\n\t", keys %map) . "\n" unless $filterEntity;

# my @excludes;

# foreach my $entity(@entities){
# 	my $itr = $owl->execute('all_subclasses', { ENTITY => $entity });
# 	my @keys = $itr->binding_names;
# 	#printf ("%s\n", join("\t", @keys)) if @keys;
# 	while (my $row = $itr->next) {
# 		my $col = pp($row->{col}->as_sparql);
# 		# push(@excludes, $row->{col}->as_sparql);
# 		$columns{$col} = 1;
# 	}
# }

my %saved;
if($filterEntity){
	my $itr = $owl->execute('all_subclasses', { ENTITY => $filterEntity });
	#my @keys = $itr->binding_names;
	#printf ("%s\n", join("\t", @keys)) if @keys;
	while (my $row = $itr->next) {
		my $col = pp($row->{col}->as_sparql);
		if(defined($columns{$col})){
			delete($columns{$col});
			$saved{$col} = 1;
		}
	}
}

print STDERR ("-------------------\n");

if($inverse){
	foreach my $k (sort keys %saved){
		print "$k\n";
	}
	print "___UNMAPPED_COLS___\n";
	foreach my $col (sort keys %index){
		print "$col\n" unless $saved{$col};
	}
}
else{
	print "$_\n" for sort keys %columns;
	## also exclude any columns not found in owl
	print "___UNMAPPED_COLS___\n";
	foreach my $col (sort keys %index){
		print "$col\n" unless $columns{$col};
	}
}

exit;


sub pp {
	my ($val) = @_;
	$val =~ s/^"|"$//g;
	return lc($val);
}

1;
