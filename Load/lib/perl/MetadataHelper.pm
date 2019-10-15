package ClinEpiData::Load::MetadataHelper;

use strict;
use warnings;

use JSON;
use ClinEpiData::Load::MetadataReader;
use Statistics::Descriptive;
use ClinEpiData::Load::OntologyDAGNode;
use XML::Simple;
use File::Basename;
use CBIL::ISA::InvestigationSimple;
use Scalar::Util qw(looks_like_number); 
use ApiCommonData::Load::OwlReader;

use Data::Dumper;


sub getReaders { $_[0]->{_readers} }
sub setReaders { $_[0]->{_readers} = $_[1] }

sub getDistinctQualifiers { $_[0]->{_distinct_qualifiers} }
sub setDistinctQualifiers { $_[0]->{_distinct_qualifiers} = $_[1] }

sub getMergedOutput { $_[0]->{_merged_output} }
sub setMergedOutput { $_[0]->{_merged_output} = $_[1] }

sub getParentParsedOutput { $_[0]->{_parent_parsed_output} }
sub setParentParsedOutput { $_[0]->{_parent_parsed_output} = $_[1] }

sub getOntologyMapping { $_[0]->{_ontology_mapping} }
sub setOntologyMapping { $_[0]->{_ontology_mapping} = $_[1] }

sub new {
  my ($class, $type, $metadataFiles, $rowExcludeFile, $colExcludeFile, $parentMergedFile, $parentType, $ontologyMappingXmlFile, $ancillaryInputFile, $packageName, $readerConfig) = @_;

  eval "require $packageName";
  die $@ if $@;  

  my $self = bless {}, $class;

  my $rowExcludes = &readRowExcludeFile($rowExcludeFile);
  my $colExcludes = &readColExcludeFile($colExcludeFile);

  my $ontologyMapping = &readOntologyMappingXmlFile($ontologyMappingXmlFile);

  $self->setOntologyMapping($ontologyMapping);

  my $parentParsedOutput;
  if($parentMergedFile) {
    my $parentReaderClass = $packageName . "::" . $parentType . "Reader";

    my $parentReader = eval {
      $parentReaderClass->new($parentMergedFile, {}, {}, undef);
    };
    die $@ if $@;

    $parentReader->read();

    $parentParsedOutput = $parentReader->getParsedOutput();

    $self->setParentParsedOutput($parentParsedOutput);
  }

  my @readers;
  foreach my $metadataFile (@$metadataFiles) {
    my $readerClass = $packageName. "::" . $type . "Reader";

   my $reader = eval {
     $readerClass->new($metadataFile, $rowExcludes, $colExcludes, $parentParsedOutput, $ancillaryInputFile, $readerConfig);
   };
    die $@ if $@;

    push @readers, $reader;
  }

  $self->setReaders(\@readers);

  return $self;
}

sub merge {
  my ($self) = @_;

  my $readers = $self->getReaders();

  my $mergedOutput = {};
  my %distinctQualifiers;

  foreach my $reader (@$readers) {
    $reader->read();

    my @parsedOutputs;
    my $nestedReaders = $reader->getNestedReaders();
    if($nestedReaders) {
      @parsedOutputs = map {$_->getParsedOutput()} @$nestedReaders;
    }

    push @parsedOutputs, $reader->getParsedOutput();

    foreach my $parsedOutput(@parsedOutputs) {

      foreach my $pk (keys %$parsedOutput) {
        my $qualifiersHash = $parsedOutput->{$pk};

        foreach my $qualifier (keys %$qualifiersHash) {


          my $value = $qualifiersHash->{$qualifier};

          push @{$mergedOutput->{$pk}->{$qualifier}}, $value if(defined $value);
          $distinctQualifiers{$qualifier}++ unless($qualifier eq '__PARENT__');
        }
      }
    }
  }

  $self->setMergedOutput($mergedOutput);
  $self->setDistinctQualifiers(\%distinctQualifiers);
}


sub isValid {
  my ($self) = @_;

  my $mergedOutput = $self->getMergedOutput();
  my $parentOutput = $self->getParentParsedOutput();
  my $ontologyMapping = $self->getOntologyMapping();

  my $errors = {};
  my %errorsDistinctQualifiers;

  my %distinctValues;

  foreach my $pk (keys %$mergedOutput) {
    if($parentOutput) {
      my $parentId = $mergedOutput->{$pk}->{"__PARENT__"};

      $parentId = &getDistinctLowerCaseValues($parentId);
      die "No Parent Defined for $pk" unless(defined $parentId);

      unless(defined($parentOutput->{lc($parentId)})) {
        print STDERR "PRIMARY_KEY=$pk, PARENT=$parentId\n";
				my @pks = sort keys %$parentOutput;
        die "Parent $parentId not defined as primary key in parent file\nParent keys look like this:\n" . join("\n", @pks[0 .. 9]) . "\n";
      }

    }
    my $qualifiersHash = $mergedOutput->{$pk};
    foreach my $qualifier (keys %$qualifiersHash) {
      if($ontologyMapping) {
        unless($ontologyMapping->{$qualifier}->{characteristicQualifier}->{source_id}) {
          $errors->{$qualifier}->{"MISSING_ONTOLOGY_MAPPING"} = 1 unless($qualifier eq '__PARENT__');
        }
      }

      my $values = $qualifiersHash->{$qualifier};
      foreach my $value (@$values) {
        if($value =~ /USER_ERROR/) {

          $errors->{$qualifier}->{"MERGE_ERRORS"} = $errors->{$qualifier}->{"MERGE_ERRORS"} + 1;
          $errorsDistinctQualifiers{$qualifier} = $errorsDistinctQualifiers{$qualifier} + 1;
        }

        $distinctValues{$qualifier}->{$value} = 1;
      }
    }
  }


  foreach my $qualifier (keys %distinctValues) {
    my @values = keys %{$distinctValues{$qualifier}};
    my $valuesCount = scalar @values;

    print STDERR "QUALIFIER=$qualifier has $valuesCount Distinct Values\n";    

    my $max;
    if($valuesCount > 10) {
      print STDERR "Showing 10\n";
      $max = 10;
    }
    else {
      $max = $valuesCount;
    }
    
    for(my $i = 0; $i < $max; $i++) {
      print STDERR "   $values[$i]\n";
    }
  }

  if(scalar keys %$errors == 0) {
    return 1;
  }

  print STDERR "\n-----------------------------------------\n";

  print STDERR "Errors found:\n";


  foreach my $qualifier (keys %$errors) {
    foreach my $type (keys %{$errors->{$qualifier}}) {
      my $v = $errors->{$qualifier}->{$type};

      print "$qualifier\t$type\t$v\n";
    }
  }




#  &write(\*STDERR, \%errorsDistinctQualifiers, $errors, undef);

  return 0;
}


# this is a one column file (no header) of primary keys to exclude
sub readRowExcludeFile {
  my $file = shift;

  my %hash;

  if($file) {
    open(FILE, $file) or die "cannot open file $file for reading:$!";

    while(my $line = <FILE>) {
      chomp $line;
			my ($id,$file) = split(/\t/, $line);
			if($file){
      	$hash{lc($id)} = basename($file);
			}
			else{
      	$hash{lc($id)} = '__ALL__';
			}
    }
    close FILE;
  }
  return \%hash;
}



sub readOntologyMappingXmlFile {
  my ($file) = shift;

  if($file) {
    my $ontologyMappingXML = XMLin($file, ForceArray => 1);

    my %ontologyMapping;

    foreach my $ot (@{$ontologyMappingXML->{ontologyTerm}}) {
      my $sourceId = $ot->{source_id};

      foreach my $name (@{$ot->{name}}) {
        $ontologyMapping{lc($name)}->{$ot->{type}} = $ot;
      }
    }

    return \%ontologyMapping;
  }

}

sub readColExcludeFile {
  my $file = shift;

  my %hash;

  if($file) {
    open(FILE, $file) or die "cannot open file $file for reading:$!";

    while(<FILE>) {
      chomp;

      my @a = split(/\t/, $_);
			next unless @a;

      my $file = $a[1];
      my $col = lc($a[0]);

      $file = "__ALL__" unless($file);

      $hash{$file}->{$col} = 1;
    }
    close FILE;
  }

  return \%hash;

}


sub writeMergedFile {
  my ($self, $outputFile) = @_;

  my $distinctQualifiers = $self->getDistinctQualifiers();
  my $mergedOutput = $self->getMergedOutput();

  open(my $fh, ">$outputFile") or die "Cannot open file $outputFile for writing:$!";

  &write($fh, $distinctQualifiers, $mergedOutput);

  close $fh;
}

sub makeTreeObjFromOntology {
  my ($self, $owlFile, $filterParentSourceIds) = @_;

	my $owl = ApiCommonData::Load::OwlReader->new($owlFile);
  my ($propertyNames, $propertySubclasses, $propertyOrder) = $owl->getLabelsAndParentsHashes($owlFile);

  my %nodeLookup;

  my $rootSourceId = "http://www.w3.org/2002/07/owl#Thing";
  my $altRootSourceId = "Thing";

  my $root = ClinEpiData::Load::OntologyDAGNode->new({name => $rootSourceId, attributes => {"displayName" => "Thing"} });

  $nodeLookup{$rootSourceId} = $root;
  $nodeLookup{$altRootSourceId} = $root;

  foreach my $parentSourceId (sort { ($propertyOrder->{$a} <=> $propertyOrder->{$b})||($a cmp $b) } keys %$propertySubclasses) {

    my $parentNode = $nodeLookup{$parentSourceId};

    unless($parentNode) {
      my $parentDisplayName = $propertyNames->{$parentSourceId};
      $parentNode = ClinEpiData::Load::OntologyDAGNode->new({name => $parentSourceId, attributes => {"displayName" => $parentDisplayName, "order" => $propertyOrder->{$parentSourceId}}});
      $nodeLookup{$parentSourceId} = $parentNode;
      if($filterParentSourceIds->{$parentSourceId}){
        $parentNode->{attributes}->{filter} = 1;
      }
    }

    my @childrenSourceIds = sort {($propertyOrder->{$a} <=> $propertyOrder->{$b})||($a cmp $b) } @{$propertySubclasses->{$parentSourceId}};

    foreach my $childSourceId (@childrenSourceIds) {
      my $childNode = $nodeLookup{$childSourceId};

      unless($childNode) {
        my $childDisplayName = $propertyNames->{$childSourceId};
        $childNode = ClinEpiData::Load::OntologyDAGNode->new({name => $childSourceId, attributes => {"displayName" => $childDisplayName, "order" => $propertyOrder->{$childSourceId}}}) ;
        $nodeLookup{$childSourceId} = $childNode;
        if($filterParentSourceIds->{$childSourceId}){
          $childNode->{attributes}->{filter} = 1;
        }
      }

      $parentNode->add_daughter($childNode);
    }
  }

  return ($root, \%nodeLookup);
}



sub writeInvestigationTree {
  my ($self, $ontologyMappingFile, $valueMappingFile, $dateObfuscationFile, $ontologyOwlFile, $mergedOutputFile,$filterParentSourceIds, $investigationFile) = @_;

	print STDERR "Making tree from $ontologyOwlFile\n";
  my ($treeObjRoot, $nodeLookup) = $self->makeTreeObjFromOntology($ontologyOwlFile, $filterParentSourceIds);

  my $dirname = dirname($mergedOutputFile);

  my $treeStringOutputFile = $mergedOutputFile . ".tree.txt";
  my $jsonStringOutputFile = $mergedOutputFile . ".tree.json";


  my $mergedOutputBaseName = basename($mergedOutputFile);

  unless($investigationFile) {
    $investigationFile = "$dirname/tempInvestigation.xml";

    open(FILE, ">$investigationFile") or die "Cannot open file $investigationFile for writing: $!";
  

    print FILE "<investigation identifier=\"DUMMY\" identifierIsDirectoryName=\"false\">
  <study fileName=\"$mergedOutputBaseName\" identifierSuffix=\"-1\">
    <node isaObject=\"Source\" name=\"ENTITY\" type=\"INTERNAL\" suffix=\"\" useExactSuffix=\"true\" idColumn=\"PRIMARY_KEY\"/>  
  </study>
</investigation>
";

    close FILE;
  }

  my $investigation = CBIL::ISA::InvestigationSimple->new($investigationFile, $ontologyMappingFile, undef, $valueMappingFile, undef, 0, $dateObfuscationFile);
  eval {
    $investigation->parseInvestigation();
  };
  if($@) {
    die $@;
    next;
  }

  my $studies = $investigation->getStudies();

  my %data;
  my %qualifierToHeaderNames;


  foreach my $study (@$studies) {
    
    while($study->hasMoreData()) {
      
      eval {
        $investigation->parseStudy($study);
        $investigation->dealWithAllOntologies();
      };
      if($@) {
        die $@;
      }

      my $nodes = $study->getNodes();


      foreach my $node (@$nodes) {
        if($node->hasAttribute("MaterialType")) {
          my $characteristics = $node->getCharacteristics();
          foreach my $characteristic (@$characteristics) {
            my $qualifier = $characteristic->getQualifier();

            my $altQualifier = $characteristic->getAlternativeQualifier();

            
            my $value = $characteristic->getValue();
            push @{$data{$qualifier}}, $value if(defined $value);
            $qualifierToHeaderNames{$qualifier}->{$altQualifier} = 1;
          }
        }
      }
    }
  }

  foreach my $sourceId (keys %data) {
    my @altQualifiers = sort keys %{$qualifierToHeaderNames{$sourceId}};

    my $parentNode = $nodeLookup->{$sourceId};

    die "Source_id [$sourceId] is missing from the OWL file but used in data" unless($parentNode);

    $parentNode->attributes->{'alternativeQualifiers'} = \@altQualifiers;

    my %count;

    my @values = @{$data{$sourceId}};

		printf STDERR ("Scanning %d values in %s\n", scalar @values, $sourceId);

    foreach my $value (@values) {
      if($value =~ /\d\d\d\d-\d\d-\d\d/) {
        $count{"date"}++;
      }
      elsif(looks_like_number($value)) {
        $count{"number"}++;
      }
      else {
        $count{"string"}++;
      }

      $count{"total"}++;
    }
		my $total = $count{total};
    my %valueCount;
    foreach my $value(@values) {
      $valueCount{$value}++;
    }
		my $size = scalar keys %valueCount;

    if(defined($count{"date"}) && defined($count{"total"}) && $count{"date"} == $count{"total"}) {
      #sort and take first and last
      my @sorted = sort @values;
      my $mindate = $sorted[0];
      my $maxdate = $sorted[$#sorted];
      my $display = "$total values $size distinct DATE_RANGE=$mindate...$maxdate";

      $parentNode->add_daughter(ClinEpiData::Load::OntologyDAGNode->new({name => "$sourceId.1", attributes => {"displayName" => $display, "isLeaf" => 1, "keep" => 1 }})) ;
    }
    elsif(defined($count{"number"}) && defined($count{"total"}) &&  ($count{"number"} == $count{"total"})) {
      # use stats package to get quantiles and mean
      my $stat = Statistics::Descriptive::Full->new();
      $stat->add_data(@values);
      my $min = $stat->quantile(0);
      my $firstQuantile = $stat->quantile(1);
      my $median = $stat->quantile(2);
      my $thirdQuantile = $stat->quantile(3);
      my $max = $stat->quantile(4);
      my $mean = $stat->mean();

      my $displayName = sprintf("%d values %d distinct MIN=%s MAX=%s MEDIAN=%0.1f MEAN=%0.1f LOWER_Q=%0.1f UPPER_Q=%0.1f",$total, $size, $min, $max, $median, $mean, $firstQuantile, $thirdQuantile);

      $parentNode->add_daughter(ClinEpiData::Load::OntologyDAGNode->new({name => "$sourceId.stats", attributes => {"displayName" => $displayName, "isLeaf" => 1, "keep" => 1} })) ;



    }
    else {
			printf STDERR ("%d values %d distinct in %s %s\n", $total, $size, $sourceId, join(",", @altQualifiers) || "");
			if(0){ # do not print huge list
        $parentNode->add_daughter(ClinEpiData::Load::OntologyDAGNode->new({name => "$sourceId.1", attributes => {"displayName" => "$size distinct values", "isLeaf" => 1, "keep" => 1} }));
			}
			else {
     	  my $ct = 1;
     	  foreach my $value (keys %valueCount) {
     	    $parentNode->add_daughter(ClinEpiData::Load::OntologyDAGNode->new({name => "$sourceId.$ct", attributes => {"displayName" => "$value ($valueCount{$value})", "isLeaf" => 1, "keep" => 1} })) ;
     	    $ct++;
     	  }
			}
    }

    &keepNode($parentNode);

  }

  if(0 < scalar values %{$filterParentSourceIds}){
		printf STDERR ("Scanning for column headers to filter under %s\n", join(", ", sort keys %$filterParentSourceIds)) ;
  	my $filterColumns = $treeObjRoot->getNonFilteredAlternativeQualifiers();
		if(0 < scalar @$filterColumns){
    	printf STDERR ("Here are the column headers to be excluded\n%s\n", join("\n", sort @$filterColumns));
		}
		printf STDERR "\t...done\n";
  }
	else {
		printf STDERR "\nNo filterParentSourceId, skipping scan for column headers to exclude\n\n";
  }

	printf STDERR "printing tree files\n";
  open(TREE, ">$treeStringOutputFile") or die "Cannot open file $treeStringOutputFile for writing:$!";
  open(JSON, ">$jsonStringOutputFile") or die "Cannot open file $jsonStringOutputFile for writing:$!";

  print TREE map { "$_\n" if(defined($_)) } @{$treeObjRoot->tree2string({no_attributes => 0})};

  my $treeHashRef = $treeObjRoot->transformToHashRef();
  
  #print Dumper $treeHashRef;
  my $json_text;
  eval {
      $json_text = to_json($treeHashRef,{utf8=>1, pretty=>1, canonical=>[0]});
  };
  if ($@){
       print STDERR  Dumper $treeHashRef;
       die "could not make tree.";     
}


 # my $json_text = to_json($treeHashRef,{utf8=>1, pretty=>1});

  print JSON "$json_text\n";

  close TREE;
  close JSON;
		printf STDERR "\t...done\n";
}



sub keepNode {
  my ($node) = @_;

  $node->{attributes}->{keep} = 1;

  return if($node->is_root());

  &keepNode($node->mother());
}


sub write {
  my ($fh, $distinctQualifiers, $mergedOutput, $summarize) = @_;

  my @qualifiers = keys %$distinctQualifiers;

  print $fh "PRIMARY_KEY\tPARENT\t" . join("\t", @qualifiers) . "\n";

  foreach my $pk (keys %$mergedOutput) {
    my $qualifiersHash = $mergedOutput->{$pk};

    my $parent = &getDistinctLowerCaseValues($qualifiersHash->{'__PARENT__'});

    my @qualifierValues = map { &getDistinctLowerCaseValues($qualifiersHash->{$_})  } @qualifiers;

    print $fh "$pk\t$parent\t" . join("\t", @qualifierValues) . "\n";
  }
}

sub getDistinctLowerCaseValues {
  my ($a) = @_;

  my %seen;
  foreach(@$a) {
    $seen{$_}++;
  }

  my $rv = join('|', keys(%seen));

  if(scalar(keys(%seen)) > 1) {
    $rv = "USER_ERROR_$rv";
  }

  return $rv;
}

1;
