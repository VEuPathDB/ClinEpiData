#!/usr/bin/perl

use strict;
use warnings;
use lib "$ENV{GUS_HOME}/lib/perl";

use CBIL::ISA::InvestigationSimple;
use ClinEpiData::Load::Utilities::Investigation;
use ClinEpiData::Load::MetadataHelper;
use ClinEpiData::Load::Utilities::File qw/csv2tab tabWriter/;
use ClinEpiData::Load::Utilities::OntologyMapping;
# use Data::Dumper;
use File::Basename;
use File::Temp qw/tempfile tempdir/;
use POSIX qw/strftime/;
use Cwd qw/abs_path/;
use Getopt::Long qw(:config no_ignore_case);
use JSON;
use Data::Dumper;

my ($invFile, $ontologyMappingFile, $ontologyOwlFile, $dateObfuscationFile, $valueMapFile, $noSqlFile, $testRun, $autoMode, @studyParams, @mdFiles, @protocols, @idCols, $cleanUp, @downloadFile, $isaSimpleDirectory);
my %optStruct = (
    'i|investigationFile=s' => \$invFile,
    'o|ontologyMappingFile=s' => \$ontologyMappingFile,
    'w|ontologyOwlFile=s' => \$ontologyOwlFile,
    'd|dateObfuscationFile=s' => \$dateObfuscationFile,
    'I|isaSimpleDirectory=s' => \$isaSimpleDirectory,
    'v|valueMapFile=s' => \$valueMapFile,
    'j|noSqlFile=s' => \$noSqlFile,
    't|test!' => \$testRun,
    'a|auto!' => \$autoMode,
    'S|studyParams=s' => \@studyParams,
    'm|metadataFile=s' => \@mdFiles,
    'p|protocol=s' => \@protocols,
    'k|idColumn=s' => \@idCols,
    'c|cleanUp!' => \$cleanUp,
    'f|downloadFile=s' => \@downloadFile, # make downloadFile by name of tree node 
    );

GetOptions(
    %optStruct
    );

if(-d $isaSimpleDirectory){
  my $isaDir = abs_path($isaSimpleDirectory);
  $invFile ||= join("/", $isaDir, "investigation.xml");
  $dateObfuscationFile ||= join("/", $isaDir, "dateObfuscation.txt");
  $valueMapFile ||= join("/", $isaDir, "valueMap.txt");
  $ontologyMappingFile ||= join("/", $isaDir, "ontologyMapping.xml");
}

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

my $labels = {};
if($ontologyOwlFile){
  $labels = getLabelsFromOwl($ontologyOwlFile);
}

if($autoMode){
  my $timestamp = strftime("%Y%m%d%H%M%S", localtime);
  my $dir = tempdir("auto_${timestamp}_XXXX", CLEANUP => $cleanUp);
  $dateObfuscationFile = "$dir/dateObfuscation.txt";
  open(FH, ">$dateObfuscationFile") or die "$!\n";
  close(FH);
  my $inv = ClinEpiData::Load::Utilities::Investigation->new($dir);
  my @mergedFiles;
  if(@studyParams){
   #{
   #  ## preprocessStudy returns values parsed from the pipe-delimited param string
   #  my ($mergedFile,$entity,$primaryKey,$protocol,$parent,$parentKey) = preprocessStudy($studyParams[0],$dir);
   #  # ($mergedFile,$entity,'PRIMARY_KEY',$parentEntity,'PARENT')
   #  $inv->addStudy($mergedFile,$entity,$primaryKey,$protocol,$parent,$parentKey);
   #  push(@mergedFiles,$mergedFile);
   #  push(@protocols,$protocol) if $protocol;
   #}
    foreach my $k ( 0 .. $#studyParams){
      my ($mergedFile,$entity,$primaryKey,$protocol,$parent,$parentKey) = preprocessStudy($studyParams[$k],$dir);
      $inv->addStudy($mergedFile,$entity,$primaryKey,$protocol,$parent,$parentKey);
      push(@mergedFiles,join("/", $dir, $mergedFile));
      push(@protocols,$protocol) if $protocol;
    }
  }
  else {
    ## use raw files
    foreach my $file (@mdFiles){
      my $dest = join("/", $dir, basename($file));
      csv2tab($file, $dest);
      push(@mergedFiles, $dest);
    }
    die "no merged files" unless @mergedFiles;
    my @entities = map { lc(fileparse($_, qr/\.[^.]+$/)) } @mergedFiles;
#  # first mdfile assumed to be top
    $inv->addStudy(basename($mergedFiles[0]),$entities[0],$idCols[0]);
#  # each addditional file assumed to be child nodes
    if(1 < @mergedFiles){
      foreach my $i (1 .. $#mergedFiles){
        $inv->addStudy(basename($mergedFiles[$i]),$entities[$i],$idCols[$i],
            $protocols[$i-1],
            $entities[$i-1],$idCols[$i-1]
            );
      }
    }
  }
printf STDERR "DONE MERGING FILES\n";
  $invFile = join("/", $dir, "investigation.xml");
  open(IF, ">$invFile") or die "Cannot write $invFile: $!";
  print IF $inv->getXml;
  close(IF);
  $ontologyMappingFile = join("/", $dir, "ontologyMapping.xml");
  writeOntologyMappingFile($ontologyMappingFile, \@mergedFiles, \@protocols);
}

my $inv = CBIL::ISA::InvestigationSimple->new($invFile, $ontologyMappingFile, undef, $valueMapFile, 0, 0, $dateObfuscationFile);

if($testRun){
  printf STDERR "TEST! IDs will be obfuscated, if indicated in investigation.xml\n";
}
my $ont = $inv->getOntologyMapping();
my $xml = $inv->getSimpleXml();
my $hasA = { _root => {} };
my $datasetName = $xml->{study}->[0]->{dataset}->[0];
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
my %charsByType;
$inv->parseInvestigation();
my $studies = $inv->getStudies();
foreach my $study (@$studies){ ## studies are in order: household, participant, observation, sample
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
         my $var = $ch->getQualifier;
         if($labels->{$var}){ $var = $labels->{$var} }
     #   my $var = $ch->getAlternativeQualifier; # provider variable
         $chars->{$var} ||= [];
         push(@{$chars->{$var}}, $ch->getValue );
       }
       if($chars == {}){ printf STDERR ("EMPTY NODE:%s:%s\n", $type, $id) }
       $pan->{characteristics} = $chars;
       $charsByType{$type}->{$id} = {is_sub => 0, characteristics => $chars};
 ## backtrack 
       my @pids;
       my $pid = $id;
       if($parentOf{$pid}){
         while(my $nextPid = $parentOf{$pid}){ # nextPid: literal parent ID
           unless($typeOf{$nextPid}){ die "Type of $nextPid not found\n" . Dumper \%typeOf; }
           unshift(@pids, [$typeOf{$nextPid}, $nextPid]); # [ parent type, pid ]
           push(@{$charsByType{$type}->{$id}->{ancestors}}, [$typeOf{$nextPid},$nextPid]);
           $pid = $nextPid;
         }
       }
       elsif( $typeOf{$id} ne $root ){
 #die "Orphan: $id\n type $typeOf{$id} ne $root" . Dumper({ PIDS => \@pids}); # or dump $tree
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
        my $nextNode = $treeNode->{$nodeType}->{$nextId};
        $treeNode = $nextNode if($nextNode);
      }
      $treeNode->{$type}->{$id} ||= $pan;
    }
  }
}

if($noSqlFile){
  open(FH, ">$noSqlFile") or die "Cannot write $noSqlFile: $!\n";
  print FH to_json($tree,{convert_blessed=>1,utf8=>1, pretty=>1, canonical=>[0]});
  close(FH);
}

if( @downloadFile && $downloadFile[0] eq 'ALL' ){ @downloadFile = keys %charsByType }
foreach my $type( @downloadFile ){
  my $outputFile = sprintf("%s_%ss.txt", $datasetName, $type);
  $outputFile =~ s/ys.txt$/ies.txt/;
  open(my $fh, ">$outputFile") or die "Cannot write $outputFile: $!\n";
  my %variableCols;
  my %ancestorCols;
  # first scan to get the total column headers
  # and the longest ancestor path
  my $maxAncestorPath = [];
  my $hasSubs = 0;
  while( my ($id,$node) = each %{$charsByType{$type}}){
    $variableCols{$_} = 1 for keys %{$node->{characteristics}};
    my $ancestors = $node->{ancestors};
    foreach my $anc (@$ancestors){
      my ($ancType, $ancId) = @$anc;
      if($ancType =~ /^$type$/i){
        # label the primary ID as x_Observation_Id
        $node->{is_sub}=1;
        $hasSubs = 1;
      }
    }
    if(scalar(@$ancestors) > scalar(@$maxAncestorPath)) { $maxAncestorPath = $ancestors }
  }
  # print headers
  my @vars = sort { uc($a) cmp uc($b) } keys %variableCols;
  my @cols;
  if($hasSubs){
    if($type =~ /^observation$/i){ push(@cols, "Subobservation_Id") }
    else { push(@cols, sprintf("%s_Observation_Id", ucfirst($type))) }
  }
  else { push(@cols,sprintf("%s_Id", ucfirst($type))) }
  # now we potentionally have Household_Id OR Household_Observation_Id
  push(@cols, map { sprintf"%s_Id", ucfirst($_->[0]) } @$maxAncestorPath);
  # now potentionally Household_Observation_Id, Household_Id, Community_Id
  #                   Subobservation_Id, Observation_Id, Participant_Id, Household_Id, Community_Id
  push(@cols, @vars); # alphabetized variable labels "Label [IRI]"
  my $tab = tabWriter();
  $tab->print($fh, \@cols); ## print header
  print $fh "\n";
  foreach my $id (sort keys %{$charsByType{$type}}){
    my $node = $charsByType{$type}->{$id};
    my @row = ($id);
    if($hasSubs &! $node->{is_sub}){ push(@row, $id) } # insert self again 
    push(@row, map { $_->[1] } @{$node->{ancestors}});
    foreach my $var ( @vars ){
      unless(defined($node->{characteristics}->{$var})){ push(@row, '') }
      else {
        my %uniqueValues = map { $_ => 1 } @{$node->{characteristics}->{$var}};
        push(@row, join(" | ", sort keys %uniqueValues))
      }
    }
    $tab->print($fh, \@row);
    print $fh "\n";
  }
  close($fh);
}

exit;

sub getLabelsFromOwl {
  my ($ont) = @_;
  my %labels;
  my $owlFile;
  if(-e $ont){
  	$owlFile = $ont;
  }
  else {
    my $ontdir = $ENV{GUS_HOME} . "/ontology/release/production";
  	$owlFile = sprintf("%s/%s.owl", $ontdir, $ont);
  }
  my $owl = {};
  eval 'require ApiCommonData::Load::OwlReader';
  eval '$owl = ApiCommonData::Load::OwlReader->new($owlFile)';
	my $it = $owl->execute('get_terms');
	while( my $row = $it->next ){
		my $label = $row->{label} ? $row->{label}->as_sparql : ""  ;
    $label =~ s/^"(.*)"$/$1/;
    my $uri = $row->{entity}->as_hash()->{iri}|| $row->{entity}->as_hash()->{URI};
    my $sid = $owl->getSourceIdFromIRI($uri);
    $labels{$sid} = sprintf("%s [%s]", $label, $sid);
	}
  return \%labels;
}

sub preprocessStudy {
  my ($studyParam,$tempdir) = @_;
  ## preprocess/merge files
  my @datasets; # configs for each study
  # my @mdfiles;
  my @mergedFiles;
  my @protocols;
  #my $ontologyMappingXmlFile = join("/", $tempdir, "ontologyMapping.xml");
  #foreach my $study (@$studyParams){
    my %config = ( ## set some defaults
      packageName => 'ClinEpiData::Load::GenericReader',
      type => 'Category',
      parentType => 'Output');
   #  ontologyMappingXmlFile => $ontologyMappingXmlFile,
   #);
    my @params = split(/[|]/, $studyParam);
    foreach my $param (@params){
      my ($k,$v) = split(/=/, $param);
      $config{$k} = $v;
    }
    if($config{metadataFile}){
      my @files = split (/,/, $config{metadataFile});
      $config{metadataFile} = \@files;
      # push(@mdfiles, @files);## for ontologyMappingXmlFile
    }

    ## readerConfig params
    if($config{idColumn}){
      my @fileIdSets = split(/,/, $config{idColumn});
      foreach my $k (0 .. $#fileIdSets){
        my $file = $config{metadataFile}->[$k];
        my $mdfilekey = lc(fileparse($file, qr/\.[^.]+$/));
        my $type = lc($config{type});
        my @cols = split(/[:]{3}|[+]/, $fileIdSets[$k]);
        $config{readerConfig}->{idMap}->{$mdfilekey}->{$type} = \@cols;
        $config{readerConfig}->{noFilePrefix} = 1;
      }
    }
    if($config{parentIdColumn}){
      my @fileIdSets = split(/,/, $config{parentIdColumn});
      foreach my $k (0 .. $#fileIdSets){
        my $file = $config{metadataFile}->[$k];
        my $mdfilekey = lc(fileparse($file, qr/\.[^.]+$/));
        my $type = lc($config{parentType});
        my @cols = split(/[:]{3}|[+]/, $fileIdSets[$k]);
        $config{readerConfig}->{idMap}->{$mdfilekey}->{$type} = \@cols;
        $config{readerConfig}->{noFilePrefix} = 1;
      }
    }
    unless($config{outputFile}){
      my $fh;
      ($fh,$config{outputFile}) = tempfile("merged_XXXX", SUFFIX=>'.txt', DIR => $tempdir);
      close($fh);
    }
    else{
      $config{outputFile}=join("/", $tempdir, $config{outputFile});
    }
  # $config{packageName} = 'ClinEpiData::Load::GenericReader';
  # $config{type} = 'Category';
  # push(@datasets, \%config);
  # push(@mergedFiles, $config{outputFile});
  # push(@protocols, $config{protocol}) if $config{protocol};
  #}
  # foreach my $study (@datasets){
    my @args = map { $config{$_} } qw/type metadataFile rowExcludeFile colExcludeFile parentMergedFile parentType ontologyMappingXmlFile ancillaryInputFile packageName readerConfig/;
    my $metadataHelper = ClinEpiData::Load::MetadataHelper->new(@args);
  	$metadataHelper->merge();
  	if($metadataHelper->isValid()) {
  	  $metadataHelper->writeMergedFile($config{outputFile});
  	}
  	else {
  	  $metadataHelper->writeMergedFile($config{outputFile});
  	  die "ERRORS Found.  Please fix and try again.";
  	}
    ## Clean up memory before trying to run the next step
    $metadataHelper->setMergedOutput({});
  #}
  #return @mergedFiles;
  $config{entity} ||= basename($config{outputFile},'.txt');
  if($config{parent}) { $config{parentKey} = 'PARENT' }
  return (basename($config{outputFile}),$config{entity},'PRIMARY_KEY',$config{protocol},$config{parent}, $config{parentKey});
}

sub writeOntologyMappingFile {
  my ($file, $mdfiles, $protocols) = @_;
  my $ont = ClinEpiData::Load::Utilities::OntologyMapping->new();
  $ont->getOntologyXmlFromFiles($mdfiles,$protocols);
  open(OF, ">$file") or die "Cannot write $ontologyMappingFile: $!";
  print OF $ont->getOntologyXml;
  close(OF);
}

