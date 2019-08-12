#!/usr/bin/perl

use strict;
use warnings;
use lib "$ENV{GUS_HOME}/lib/perl";

use CBIL::ISA::InvestigationSimple;
# use Data::Dumper;
use File::Basename;
use Getopt::Long;


my ($invFile, $ontologyMappingFile, $dateObfuscationFile, $valueMapFile, $test, $calcMissing);

GetOptions(
  'i|investigationFile=s' => \$invFile,
  'o|ontologyMappingFile=s' => \$ontologyMappingFile,
  'd|dateObfuscationFile=s' => \$dateObfuscationFile,
  'v|valueMapFile=s' => \$valueMapFile,
  't|makeIDMap!' => \$test,
  'c|calculateMissingDeltas!' => \$calcMissing,
);

unless ( -e $invFile && -e $ontologyMappingFile){
  my $name = basename($0);
  print join("\n", (
    "To supplement the date obfuscation file with entries for all nodes with idObfuscationFunction=[function name] in investigation.xml:\n",
    "\t$name -i investigation.xml -o ontologyMapping.xml -d dateObfuscation.txt\n",
    "To write idmap.txt mapping obfuscated IDs to original IDs:\n",
    "\t$name -i investigation.xml -o ontologyMapping.xml -d dateObfuscation.txt -t\n\n"
  ));
  exit;
}
$dateObfuscationFile ||= 'dateObfuscation-NEW.txt';

unless(-e $dateObfuscationFile){
  print "Creating new file $dateObfuscationFile\n";
  open(FH, ">$dateObfuscationFile") or die "$!\n";
  close(FH);
}

my $inv = CBIL::ISA::InvestigationSimple->new($invFile, $ontologyMappingFile, undef, $valueMapFile, 0, 0, $dateObfuscationFile);

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
my $studies = $inv->getStudies();

my %parentOf; # map nodes to parents
my $func = $inv->getFunctions();
my $obf = $func->getDateObfuscation();

my %types;
my %deltas;
printf "Map parents, get deltas...\n";
foreach my $study (@$studies){ ## studies are in order: household, participant, observation, sample
  while($study->hasMoreData()) {
    my $nodes = [];
    my $edges = $study->getEdges();
    foreach my $edge (@$edges){
      my $inputs = $edge->getInputs();
      my $outputs = $edge->getOutputs();
      push(@$nodes, @$outputs);
      foreach my $node (@$outputs){
        my $id = $node->getValue();
        $parentOf{$id} = $inputs->[0]->getValue();
      }
    }
    unless(0 < scalar @$nodes){
      $nodes = $study->getNodes();
    }
    foreach my $node (@$nodes){
      my $type = $node->getMaterialType()->getTermAccessionNumber();
      my $id = $node->getValue();
      $types{$id} = $type;
      my $delta = $obf->{$type}->{$id};
      if(!$delta && $calcMissing){
        printf STDERR "Creating a new delta for $type:$id\n";
        $delta = $func->calculateDelta();
        $func->cacheDelta($types{$id}, $id, $delta);
      }
      $deltas{$id} = $delta if $delta;
    }
  }
}
  
printf "Find Missing Deltas...\n";
foreach my $id (keys %parentOf){
  next if (defined($deltas{$id}));
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
    die "FAILED: $types{$id}\t$id\n";
  }
}
if($test) {
  print "Done. Writing idmap.txt\n";
  $inv->writeObfuscatedIdFile("idmap.txt");
}
else {
  print "Done. Run again with test=1 to write idmap.txt\n";
}
  

1;
