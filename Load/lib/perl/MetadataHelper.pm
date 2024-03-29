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
# use ApiCommonData::Load::OwlReader;

use Data::Dumper;

my $MAX_TREE_NODES = 100;

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

sub getOwl { $_[0]->{_owl} }
sub setOwl { $_[0]->{_owl} = $_[1] }

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

  $readerConfig ||= {};
  $readerConfig->{type} ||= $type;
  $readerConfig->{parentType} ||= $parentType;
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
      my $parentId = $mergedOutput->{$pk}->{"__PARENT__"} || $mergedOutput->{$pk}->{__parent__};

      $parentId = &getDistinctLowerCaseValues($parentId);
      die "No Parent Defined for $pk" unless(defined $parentId);

      unless(defined($parentOutput->{lc($parentId)})) {
        print STDERR "PRIMARY_KEY=$pk, PARENT=$parentId\n" , Dumper $mergedOutput->{$pk};
				my @pks = sort keys %$parentOutput;
        die "Parent $parentId not defined as primary key in parent file\nParent keys look like this:\n" . join("\n", @pks[0 .. 9]) . "\n";
      }

    }
    my $qualifiersHash = $mergedOutput->{$pk};
    foreach my $qualifier (keys %$qualifiersHash) {
      # We will automatically accept multiple values per entity by trimming off the suffix !!1 (!!2, !!3, ...)
      my ($autoQual) = ($qualifier =~ /^(.*)\!\!.+$/);
      $autoQual //= $qualifier;
      if($ontologyMapping) {
        unless($ontologyMapping->{$autoQual}->{characteristicQualifier}->{source_id}) {
          unless(lc($qualifier) eq '__parent__'){
            $errors->{$qualifier}->{"MISSING_ONTOLOGY_MAPPING"} ||= 0 ;
            $errors->{$qualifier}->{"MISSING_ONTOLOGY_MAPPING"} += 1 ;
            # DEBUG with this: # $errors->{$qualifier}->{"MISSING_ONTOLOGY_MAPPING"} = Dumper($ontologyMapping->{$autoQual}) ;
          }
        }
      }

      my $values = $qualifiersHash->{$qualifier};
      foreach my $value (@$values) {
        if($value =~ /USER_ERROR/) {

          $errors->{$qualifier}->{"MERGE_ERRORS"} = $errors->{$qualifier}->{"MERGE_ERRORS"} + 1;
          $errorsDistinctQualifiers{$qualifier} = $errorsDistinctQualifiers{$qualifier} + 1;
        }
        # Merge values by $autoQual, even if $autoQual != $qualifier
        $distinctValues{$autoQual}->{$value} = 1;
        # Note that multi-value auto-generated vars (thing!!1) are omitted beyond this point
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

      my %names;
      foreach my $name (@{$ot->{name}}) {
        $ontologyMapping{lc($name)}->{$ot->{type}} = $ot;
        $names{$name} = 1;
      }
      # also hash by source_id
      unless($names{lc($sourceId)}){
        push(@{$ot->{name}}, lc($sourceId))
      }
      $ontologyMapping{lc($sourceId)}->{characteristicQualifier} = $ot;
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
  binmode($fh, "encoding(UTF-8)");

  $self->write($fh, $distinctQualifiers, $mergedOutput);

  close $fh;
}

sub makeTreeObjFromOntology {
  my ($self, $owlFile, $filterParentSourceIds, $filterOwlAttributes) = @_;
  
  my $owl;
  eval 'require ApiCommonData::Load::OwlReader';
  eval '$owl = ApiCommonData::Load::OwlReader->new($owlFile)';
  $self->setOwl($owl); # to use later
  my ($propertyNames, $propertySubclasses, $propertyOrder, $otherAttrs) = $owl->getLabelsAndParentsHashes($owlFile);

  my %nodeLookup;

  my $rootSourceId = "http://www.w3.org/2002/07/owl#Thing";
  my $altRootSourceId = "Thing";

  my $root = ClinEpiData::Load::OntologyDAGNode->new({name => $rootSourceId, attributes => {"displayName" => "Thing"} });

  $nodeLookup{$rootSourceId} = $root;
  $nodeLookup{$altRootSourceId} = $root;

  foreach my $parentSourceId (sort { ($propertyOrder->{$a} <=> $propertyOrder->{$b})||($propertyNames->{$a} cmp $propertyNames->{$b})||($a cmp $b) } keys %$propertySubclasses) {

    my $parentNode = $nodeLookup{$parentSourceId};

    unless($parentNode) {
      my $parentDisplayName = $propertyNames->{$parentSourceId};
      my $otherAttrStr = undef;
      if(keys %{$otherAttrs->{$parentSourceId}}){
        $otherAttrStr = join(",", map { "$_=$otherAttrs->{$parentSourceId}->{$_}" } keys %{$otherAttrs->{$parentSourceId}});
      }
      $parentNode = ClinEpiData::Load::OntologyDAGNode->new({name => $parentSourceId, attributes => {"displayName" => $parentDisplayName, "order" => $propertyOrder->{$parentSourceId}, "other" => $otherAttrStr}});
      $nodeLookup{$parentSourceId} = $parentNode;
      
      ## Filter by ancestor IRI; filter = 1 means KEEP
      if($filterParentSourceIds->{$parentSourceId}){
        $parentNode->{attributes}->{filter} = 1;
      }
    }

    my @childrenSourceIds = sort {($propertyOrder->{$a} <=> $propertyOrder->{$b})||($propertyNames->{$a} cmp $propertyNames->{$b})||($a cmp $b) } @{$propertySubclasses->{$parentSourceId}};

    foreach my $childSourceId (@childrenSourceIds) {
      my $childNode = $nodeLookup{$childSourceId};

      unless($childNode) {
        my $childDisplayName = $propertyNames->{$childSourceId};
        my $otherAttrStr = undef;
        if(keys %{$otherAttrs->{$childSourceId}}){
          $otherAttrStr = join(",", map { "$_=$otherAttrs->{$childSourceId}->{$_}" } keys %{$otherAttrs->{$childSourceId}});
        }
        $childNode = ClinEpiData::Load::OntologyDAGNode->new({name => $childSourceId, attributes => {"displayName" => $childDisplayName, "order" => $propertyOrder->{$childSourceId}, "other" => $otherAttrStr}}) ;
        $nodeLookup{$childSourceId} = $childNode;
        if($filterParentSourceIds->{$childSourceId}){
          $childNode->{attributes}->{filter} = 1;
        }
        ## Filter by other attributes, .e.g timeVarying
        if($filterOwlAttributes && $otherAttrs->{$childSourceId}){
          while(my ($attrName,$attrKeepValue) = each %$filterOwlAttributes){
            if($otherAttrs->{$childSourceId}->{$attrName}){
              if($otherAttrs->{$childSourceId}->{$attrName} =~ /$attrKeepValue/){
                $childNode->{attributes}->{$attrName} = 1;
                # printf STDERR ("FILTER: KEEP CHILD %s timeVarying = %s\n", $childSourceId, $otherAttrs->{$childSourceId}->{$attrName});
              }
             #else {
             #  $childNode->{attributes}->{filter} = undef;
             #  printf STDERR ("FILTER: REJECT CHILD %s timeVarying = %s\n", $childSourceId, $otherAttrs->{$childSourceId}->{$attrName});
             #}
            }
          }
        }
      }

      $parentNode->add_daughter($childNode);
    }
  }

  return ($root, \%nodeLookup);
}



sub writeInvestigationTree {
  my ($self, $ontologyMappingFile, $valueMappingFile, $dateObfuscationFile, $ontologyOwlFile, $mergedOutputFile,$filterParentSourceIds, $filterOwlAttributes, $investigationFile) = @_;

	print STDERR "Making tree from $ontologyOwlFile\n";
  my ($treeObjRoot, $nodeLookup) = $self->makeTreeObjFromOntology($ontologyOwlFile, $filterParentSourceIds, $filterOwlAttributes);

  my $dirname = dirname($mergedOutputFile);

  my $summaryOutputFile = $mergedOutputFile . ".summary.txt";
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

  my $investigation = CBIL::ISA::InvestigationSimple->new($investigationFile, $ontologyOwlFile, $ontologyMappingFile, $valueMappingFile, undef, 0, $dateObfuscationFile);
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
  my $totalRows = 0;
  my %qualifierToLabel;


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
      $totalRows += scalar @$nodes;

      foreach my $node (@$nodes) {
        if($node->hasAttribute("MaterialType")) {
          my $characteristics = $node->getCharacteristics();
          my %seen;
          foreach my $characteristic (@$characteristics) {
            my $qualifier = $characteristic->getQualifier();

            my $altQualifier = $characteristic->getAlternativeQualifier();

            
            my $value = $characteristic->getValue();
            push @{$data{$qualifier}}, $value if(defined $value &!
              ($seen{$qualifier}{$value} || $seen{$qualifier}{$value}));
             $seen{$qualifier}{$value} = $seen{$qualifier}{$value} = 1;
            $qualifierToHeaderNames{$qualifier}->{$altQualifier} = 1;
          }
        }
      }
    }
  }
  
  my %flatSummaries; # string, number, date

  foreach my $sourceId (keys %data) {
    my @altQualifiers = sort keys %{$qualifierToHeaderNames{$sourceId}};

    my $parentNode = $nodeLookup->{$sourceId};
    unless($parentNode){ printf STDERR ("%s has no parent\n", $sourceId) }
    my $label = $parentNode->attributes->{displayName};

    die "A variable was mapped to Source_id [$sourceId], but it was not found in the .OWL. You may need to update ontologyMapping.xml\n" unless($parentNode);

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

    my @summary;
    my $cols = join(",",@altQualifiers);

    if(defined($count{"date"}) && defined($count{"total"}) && $count{"date"} == $count{"total"}) {
      #sort and take first and last
      my @sorted = sort @values;
      my $mindate = $sorted[0];
      my $maxdate = $sorted[$#sorted];
      my $display = "$total values $size distinct DATE_RANGE=$mindate...$maxdate";

      $parentNode->add_daughter(ClinEpiData::Load::OntologyDAGNode->new({name => "$sourceId.1", attributes => {"displayName" => $display, "isLeaf" => 1, "keep" => 1 }})) ;
      @summary = ('date', $label, $cols, $total, $size, $mindate, $maxdate);
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
      @summary = ('number', $label, $cols, $total, $size, $min, $max, $median);



    }
    else {
			printf STDERR ("%d values %d distinct in %s %s\n", $total, $size, $sourceId, join(",", @altQualifiers) || "");
      @summary = ('string', $label, $cols, $total, $size);
		  if($size > 100){ 
        $parentNode->add_daughter(ClinEpiData::Load::OntologyDAGNode->new({name => "$sourceId.1", attributes => {"displayName" => "$size distinct values", "isLeaf" => 1, "keep" => 1} }));
        my $width = 10;
     	  my $things = 0;
        my $ct = 1;
        my @row;
     	  while(my ($value,$vc) = each %valueCount) {
          push(@row, sprintf("%s (%d)", $value, $vc));
     	    $things++;
          if($things % $width == 0){
     	      $ct++;
            my $text = join(",", @row);
     	      $parentNode->add_daughter(ClinEpiData::Load::OntologyDAGNode->new({name => "$sourceId.$ct", attributes => {"displayName" => "$text", "isLeaf" => 1, "keep" => 1} })) ;
            @row = ();
          }
     	  }
		  }
		  else {
     	  my $ct = 1;
        #my @sumValues;
     	  foreach my $value (keys %valueCount) {
     	    $parentNode->add_daughter(ClinEpiData::Load::OntologyDAGNode->new({name => "$sourceId.$ct", attributes => {"displayName" => "$value ($valueCount{$value})", "isLeaf" => 1, "keep" => 1} })) ;
          #push(@sumValues, "$value ($valueCount{$value})");
     	    $ct++;
     	  }
			}
    }
    
    $flatSummaries{ $summary[0] }->{$sourceId} = \@summary;

    &keepNode($parentNode);

  }

  if(0 < scalar values %{$filterParentSourceIds}){
		printf STDERR ("Scanning for column headers to filter under %s\n", join(", ", sort keys %$filterParentSourceIds)) ;
  	my $filterColumns = $treeObjRoot->getNonFilteredAlternativeQualifiers();
		if(0 < scalar @$filterColumns){
    	printf STDERR ("Here are the column headers to be excluded by ancestor source ID\n%s\n", join("\n", sort @$filterColumns));
		}
		printf STDERR "\t...done\n";
    if($filterOwlAttributes){
		  printf STDERR ("Scanning for column headers to filter by attributes %s\n", join(", ", sort values %$filterOwlAttributes)) ;
      my $filterByAttrs = $treeObjRoot->getNoMatchAttribAlternativeQualifiers($filterOwlAttributes);
		  if($filterByAttrs && (0 < scalar @$filterByAttrs)){
      	printf STDERR ("Here are the column headers to be excluded by owl attribute\n%s\n", join("\n", sort @$filterByAttrs));
		  }
		  printf STDERR "\t...done\n";
    }
  }
	else {
		printf STDERR "\nNo filterParentSourceId, skipping scan for column headers to exclude\n\n";
  }

	printf STDERR "printing summary file $summaryOutputFile\n";
  open(SUMM, ">$summaryOutputFile") or die "Cannot open file $summaryOutputFile for writing:$!";
  printf SUMM ("%s\n", join("\t", 'SOURCE_ID', qw/dataType label columns totalValues distinctValues min max median/));
  while(my ($dataType,$varData) = each %flatSummaries){
    while( my ($sourceId, $rowData) = each %$varData){
      printf SUMM ("%s\n", join("\t", $sourceId, @$rowData));
    }
  }
  close SUMM;
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
  my ($self, $fh, $distinctQualifiers, $mergedOutput, $summarize) = @_;

  my $ontologyMapping = $self->getOntologyMapping();
  my @qualifiers = sort { $ontologyMapping->{$a}->{characteristicQualifier}->{source_id} cmp $ontologyMapping->{$b}->{characteristicQualifier}->{source_id} } keys %$distinctQualifiers;

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
