#!/usr/bin/perl

use strict;
use warnings;
use lib "$ENV{GUS_HOME}/lib/perl";

use CBIL::ISA::InvestigationSimple;
use ClinEpiData::Load::Utilities::Investigation;
use ClinEpiData::Load::Utilities::File qw/csv2tab/;
use ClinEpiData::Load::Utilities::OntologyMapping;
# use Data::Dumper;
use File::Basename;
use File::Temp qw/tempfile tempdir/;
use Cwd qw/abs_path/;
use Getopt::Long;
use JSON;
use Data::Dumper;


my ($invFile, $ontologyMappingFile, $dateObfuscationFile, $valueMapFile, $noSqlFile, $testRun, $autoMode, @mdFiles, @protocols, @idCols, $cleanUp);
my %optStruct = (
    'i|investigationFile=s' => \$invFile,
    'o|ontologyMappingFile=s' => \$ontologyMappingFile,
    'd|dateObfuscationFile=s' => \$dateObfuscationFile,
    'v|valueMapFile=s' => \$valueMapFile,
    'j|noSqlFile=s' => \$noSqlFile,
    't|test!' => \$testRun,
    'a|auto!' => \$autoMode,
    'm|metadataFile=s' => \@mdFiles,
    'p|protocol=s' => \@protocols,
    'k|idColumn=s' => \@idCols,
    'c|cleanUp!' => \$cleanUp,
    );

GetOptions(
    %optStruct
    );

unless ( $autoMode || (-e $invFile && -e $ontologyMappingFile)){
  my $name = basename($0);
  print join("\n", (
        "To supplement the date obfuscation file with entries for all nodes with idObfuscationFunction=[function name] in investigation.xml:\n",
        "\t$name -i investigation.xml -o ontologyMapping.xml -d dateObfuscation.txt\n",
        "To write idmap.txt mapping obfuscated IDs to original IDs:\n",
        "\t$name -i investigation.xml -o ontologyMapping.xml -d dateObfuscation.txt -t\n\n"
        ));
  exit;
}
$dateObfuscationFile ||= 'dateObfuscation-tmp.txt';

unless($autoMode || -e $dateObfuscationFile){
  print "Creating new file $dateObfuscationFile\n";
  open(FH, ">$dateObfuscationFile") or die "$!\n";
  close(FH);
}

if($autoMode){
  my $dir = tempdir("auto_XXXX", CLEANUP => $cleanUp);
  $dateObfuscationFile = "$dir/dateObfuscation.txt";
  open(FH, ">$dateObfuscationFile") or die "$!\n";
  close(FH);
## TODO merge multiple files
  foreach my $file (@mdFiles){
    my $dest = join("/", $dir, basename($file));
    csv2tab($file, $dest);
  }
  my $inv = ClinEpiData::Load::Utilities::Investigation->new($dir);
  my @entities = map { lc(fileparse($_, qr/\.[^.]+$/)) } @mdFiles;
## first mdfile assumed to be top
  $inv->addStudy(basename($mdFiles[0]),$entities[0],$idCols[0]);
## each addditional file assumed to be child nodes
  if(1 < @mdFiles){
    foreach my $i (1 .. $#mdFiles){
      $inv->addStudy(basename($mdFiles[$i]),$entities[$i],$idCols[$i],
          $protocols[$i-1],
          $entities[$i-1],$idCols[$i-1]
          );
    }
  }
  my $ont = ClinEpiData::Load::Utilities::OntologyMapping->new();
  $ont->getOntologyXmlFromFiles(\@mdFiles,\@protocols);

  $invFile = join("/", $dir, "investigation.xml");
  $ontologyMappingFile = join("/", $dir, "ontologyMapping.xml");
  open(IF, ">$invFile") or die "Cannot write $invFile: $!";
  print IF $inv->getXml;
  close(IF);
  open(OF, ">$ontologyMappingFile") or die "Cannot write $ontologyMappingFile: $!";
  print OF $ont->getOntologyXml;
  close(OF);
}

my $inv = CBIL::ISA::InvestigationSimple->new($invFile, $ontologyMappingFile, undef, $valueMapFile, 0, 0, $dateObfuscationFile);

if($testRun){
  printf STDERR "TEST! IDs will be obfuscated, if indicated in investigation.xml\n";
}
my $ont = $inv->getOntologyMapping();
my $xml = $inv->getSimpleXml();
my $hasA = { _root => {} };
foreach my $studyXml (@{$xml->{study}}) {
  my %studyTypeOf;
  while(my ($name, $node) = each %{$studyXml->{node}}){
    $studyTypeOf{$name} = $node->{type};
    unless($testRun){
      $node->{DISABLEidObfuscationFunction} = $node->{idObfuscationFunction}; 
      delete($node->{idObfuscationFunction});
    }
    $hasA->{_root}->{$node->{type}} = 1;
  }
  next unless defined $studyXml->{edge};
  foreach my $edge (@{$studyXml->{edge}}){
    $hasA->{ $studyTypeOf{$edge->{input}} } ||= [];
    push(@{$hasA->{$studyTypeOf{$edge->{input}}}}, $studyTypeOf{$edge->{output}});
  }
}
while(my ($name, $node) = each %$hasA ){
  next if $name eq '_root';
  foreach my $child ( @$node ){
    delete($hasA->{ _root }->{$child}) unless $child eq $name;
  }
}
if(1 < scalar keys %{$hasA->{_root}}){
  die "Too many roots";
}
my ($root) = keys %{$hasA->{_root}};
delete($hasA->{_root});
my $func = $inv->getFunctions();
my $obf = $func->getDateObfuscation();

my $invId = $xml->{identifier};
my $tree = {
  _id => $invId,
  $invId => { # MALED0001
    _root => $root, # household
      $root => {}
  }
};
foreach my $child ( @{$hasA->{$root}}){
  $tree->{$invId}->{$root}->{$child} = {};
}

my %parentOf; # map nodes to parents
my %typeOf;
my %deltas;
$inv->parseInvestigation();
my $studies = $inv->getStudies();
# foreach my $study (@$studies){
#   $study->setHasMoreData(1);
#   my $file = $study->getFileName();
#   printf STDERR "set has more data $file\n";
# }
foreach my $study (@$studies){ ## studies are in order: household, participant, observation, sample
#my $edges = $study->getEdges();
  while($study->hasMoreData()) {
    $inv->parseStudy($study);
    my $file = $study->getFileName();
    my $edges = $study->getEdges();
    my $nodes = [];
    foreach my $edge (@$edges){
      my $inputs = $edge->getInputs();
      my $outputs = $edge->getOutputs();
      push(@$nodes, @$outputs);
      foreach my $node (@$outputs){
        $parentOf{$node->getValue()} = lc($inputs->[0]->getValue());
      }
    }
    unless(0 < scalar @$nodes){
      $nodes = $study->getNodes();
    }
    printf STDERR ("Parsing %d nodes in %s\n", scalar @$nodes, $file);
    foreach my $node (@$nodes){

      my $mat = $node->getMaterialType();
      my $type = $mat->getTerm();
      die("No type for node " . Dumper $node) unless $type;
# next if($edges && $hasA->{$type});
      my $sourceId = $mat->getTermAccessionNumber();
      my $id = lc($node->getValue());
      if(defined($typeOf{$id})){ die "Redundant ID: $typeOf{$id} $id\n" . Dumper $node; }
      $typeOf{$id} = $type;
## TODO apply obfuscation function to IDs
      my $delta = $obf->{$type}->{$id};
      $deltas{$id} = $delta if $delta;
      my $pan = { _id => $id }; #, type=> $type, name=> $id;
      my $charsList = $node->getCharacteristics();
      my $chars = {};
      foreach my $ch (@$charsList){
# my $var = $ch->getQualifier;
        my $var = $ch->getAlternativeQualifier;
        $chars->{$var} ||= [];
        push(@{$chars->{$var}}, $ch->getValue );
      }
      $pan->{characteristics} = $chars;
## backtrack 
      my @pids;
      my $pid = $id;
      if($parentOf{$pid}){
        while(my $nextPid = $parentOf{$pid}){ # nextPid: literal parent ID
          unless($typeOf{$nextPid}){ die "Type of $nextPid not found\n" . Dumper \%typeOf; }
          unshift(@pids, [$typeOf{$nextPid}, $nextPid]); # [ parent type, pid ]
            $pid = $nextPid;
        }
      }
      elsif( $typeOf{$id} ne $root ){
#die "Orphan: $id\n type $typeOf{$id} ne $root" . Dumper({ PIDS => \@pids});#, TREE => $tree});
    }
    else {
      push(@pids, [ $type, $id]);
    }
## if( $pids[0][0] ne $root){ die "Orphan: $id\n" . Dumper \@pids; }
## insert node
    my $treeNode = $tree->{$invId};
    foreach my $pid (@pids){
      my ($nodeType, $nextId) = @$pid;
      die "treeNode $nodeType:$nextId does not exist" . Dumper($tree) unless $treeNode->{$nodeType};
# die "treeNode $type:$nextId does not exist" unless $treeNode->{$type}->{$nextId};
      my $nextNode = $treeNode->{$nodeType}->{$nextId};
      $treeNode = $nextNode if($nextNode);
    }
# $treeNode->{$type} ||= {};
    $treeNode->{$type}->{$id} ||= $pan;
  }
}
}

if($noSqlFile){
  open(FH, ">$noSqlFile") or die "Cannot write $noSqlFile: $!\n";
  print FH to_json($tree,{convert_blessed=>1,utf8=>1, pretty=>1, canonical=>[0]});
  close(FH);
}

