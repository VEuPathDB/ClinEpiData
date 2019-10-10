#!/usr/bin/perl

use strict;
use warnings;
use lib "$ENV{GUS_HOME}/lib/perl";

use CBIL::ISA::InvestigationSimple;
# use Data::Dumper;
use File::Basename;
use Getopt::Long;
use JSON;
use Data::Dumper;


my ($invFile, $ontologyMappingFile, $dateObfuscationFile, $valueMapFile, $noSqlFile);
my %optStruct = (
  'i|investigationFile=s' => \$invFile,
  'o|ontologyMappingFile=s' => \$ontologyMappingFile,
  'd|dateObfuscationFile=s' => \$dateObfuscationFile,
  'v|valueMapFile=s' => \$valueMapFile,
  'j|noSqlFile=s' => \$noSqlFile,
);

GetOptions(
	%optStruct
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
my $hasA = { _root => {} };
foreach my $studyXml (@{$xml->{study}}) {
	my %studyTypeOf;
	while(my ($name, $node) = each %{$studyXml->{node}}){
		$studyTypeOf{$name} = $node->{type};
		$node->{DISABLEidObfuscationFunction} = $node->{idObfuscationFunction}; 
		delete($node->{idObfuscationFunction});
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
# 	$study->setHasMoreData(1);
# 	my $file = $study->getFileName();
# 	printf STDERR "set has more data $file\n";
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
				die "Orphan: $id\n type $typeOf{$id} ne $root" . Dumper({ PIDS => \@pids, TREE => $tree});
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
exit;


sub getTuningTablePrefix {
  my ($datasetName) = @_;
  return "D" . substr(sha1_hex($datasetName), 0, 10);
}

sub writeConfigFile {
  my ($self, $configFile, $dataFile, $table, $fieldsArr) = @_;

  my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
  my @abbr = qw(JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC);
  my $modDate = sprintf('%2d-%s-%02d', $mday, $abbr[$mon], ($year+1900) % 100);
  my $fields = join(",\n", @$fieldsArr);
  my $database = $self->getDb();
  my $projectId = $database->getDefaultProjectId();
  my $userId = $database->getDefaultUserId();
  my $groupId = $database->getDefaultGroupId();
  my $algInvocationId = $database->getDefaultAlgoInvoId();
  my $userRead = $database->getDefaultUserRead();
  my $userWrite = $database->getDefaultUserWrite();
  my $groupRead = $database->getDefaultGroupRead();
  my $groupWrite = $database->getDefaultGroupWrite();
  my $otherRead = $database->getDefaultOtherRead();
  my $otherWrite = $database->getDefaultOtherWrite();

  open(CONFIG, "> $configFile") or die "Cannot open file $configFile For writing:$!";

  print CONFIG "LOAD DATA
INFILE '$dataFile'
APPEND
INTO TABLE $table 
REENABLE DISABLED_CONSTRAINTS
FIELDS TERMINATED BY '\\t'
TRAILING NULLCOLS
($fields,
modification_date constant \"$modDate\",
user_read constant $userRead,
user_write constant $userWrite,
group_read constant $groupRead,
group_write constant $groupWrite,
other_read constant $otherRead,
other_write constant $otherWrite,
row_user_id constant $userId,
row_group_id constant $groupId,
row_project_id constant $projectId,
row_alg_invocation_id constant $algInvocationId,
)\n";
  close CONFIG;
}




1;
