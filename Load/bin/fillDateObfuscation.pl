#!/usr/bin/perl

use strict;
use warnings;
use lib "$ENV{GUS_HOME}/lib/perl";

use CBIL::ISA::InvestigationSimple;
# use Data::Dumper;
use File::Basename;

my ($invFile, $ontologyMappingFile, $dateObfuscationFile, $test) = @ARGV;

unless (2 < @ARGV){
	my $name = basename($0);
	print join("\n", (
		"To supplement the date obfuscation file with entries for all nodes with idObfuscationFunction=[function name] in investigation.xml:\n",
		"\t$name investigation.xml dateObfuscation.txt\n",
		"To write idmap.txt mapping obfuscated IDs to original IDs:\n",
		"\t$name investigation.xml dateObfuscation.txt 1\n\n"
	));
	exit;
}

my $failed;
do {
	my $inv = CBIL::ISA::InvestigationSimple->new($invFile, $ontologyMappingFile, undef , undef, 0, 0, $dateObfuscationFile);
	
	my $ont = $inv->getOntologyMapping();
	my $xml = $inv->getSimpleXml();
	unless($test){
		foreach my $studyXml (@{$xml->{study}}) {
			foreach my $node (values %{$studyXml->{node}}){
				if($node->{idObfuscationFunction}){
					printf("Disabling ID Obfuscation '%s' for %s in %s\n", $node->{idObfuscationFunction}, $node->{type}, $studyXml->{fileName});
				}
				$node->{DISABLEidObfuscationFunction} = $node->{idObfuscationFunction}; 
				delete($node->{idObfuscationFunction});
			}
		}
	}
	printf "Parsing...\n";
	eval {
		$inv->parse();
	};
	printf "Get Studies...\n";
	my $studies = $inv->getStudies();
	printf "Map Parents...\n";
	
	my %parentOf; # map nodes to parents
	foreach my $study (@$studies){
		my $edges = $study->getEdges();
		foreach my $edge (@$edges){
			my $inputs = $edge->getInputs();
			my $outputs = $edge->getOutputs();
			foreach my $node (@$outputs){
				$parentOf{$node->getValue()} = $inputs->[0]->getValue();
			}
  	}
	}
 	# print Dumper \%parentOf;
	
	my $func = $inv->getFunctions();
	my $obf = $func->getDateObfuscation();
	
	my %types;
	my %deltas;
	printf "Get Deltas...\n";
	foreach my $study (@$studies){ ## studies are in order: household, participant, observation, sample
		my $edges = $study->getEdges();
		my $nodes = $study->getNodes();
		foreach my $node (@$nodes){
			my $type = $node->getMaterialType()->getTermAccessionNumber();
			my $id = $node->getValue();
			$types{$id} = $type;
			my $delta = $obf->{$type}->{$id};
			$deltas{$id} = $delta if $delta;
		}
	}
	
	$failed = 0;
	printf "Find Missing Deltas...\n";
	foreach my $id (keys %parentOf){
		next if ($deltas{$id});
		my $pid = $parentOf{$id};
		do {
			printf "Climbing $id => $pid\n";
			if($pid && $deltas{$pid}){
				$deltas{$id} = $deltas{ $pid };
				$func->cacheDelta($types{$id}, $id, $deltas{$id});
				print "Restored: $types{$id}\t$id\t$deltas{$id}\n";
			}
			else {
				$pid = $parentOf{$pid};
			}
			
		} while ($pid && !defined($deltas{$id}));
		unless($deltas{$id}){
			print "FAILED: $types{$id}\t$id\n";
			$failed = 1;
		}
	}
	if($failed){
		print "Re-parsing...\n";
	}
	elsif($test) {
		print "Done. Writing idmap.txt\n";
		$inv->writeObfuscatedIdFile("idmap.txt");
	}
	else {
		print "Done. Run again with test=1 to write idmap.txt\n";
	}
	exit;
	
} while($failed);

1;
