package ClinEpiData::Load::Plugin::InsertInvestigations;

@ISA = qw(ApiCommonData::Load::Plugin::InsertInvestigations);
use ApiCommonData::Load::Plugin::InsertInvestigations;
use strict;
use GUS::PluginMgr::Plugin;

use CBIL::ISA::Investigation;
use CBIL::ISA::InvestigationSimple;
use File::Basename;
use GUS::Model::Study::Study;
use POSIX qw/strftime/;
use File::Temp qw/ tempfile /;
use Data::Dumper;

sub getIsReportMode { }

sub run {
  my ($self) = @_;
  my $metaDataRoot = $self->getArg('metaDataRoot');
  my $investigationBaseName = $self->getArg('investigationBaseName');

  my $isReportMode = $self->getIsReportMode();

  my ($charFh, $charFile) = tempfile(SUFFIX => '.dat');

  my @investigationFiles;

  my $investigationSubset = $self->getArg('investigationSubset');
  if($investigationSubset) {
    @investigationFiles = map { "$metaDataRoot/$_/$investigationBaseName" } @$investigationSubset;
  }
  else {
    @investigationFiles = glob "$metaDataRoot/*/$investigationBaseName";
  }

  my $investigationCount;
  foreach my $investigationFile (@investigationFiles) {
    my $dirname = dirname $investigationFile;
    $self->log("Processing ISA Directory:  $dirname");

    # clear out the protocol app node hash
    $self->{_PROTOCOL_APP_NODE_MAP} = {};

    my $investigation;
    if($self->getArg('isSimpleConfiguration')) {
      my $valueMappingFile = $self->getArg('valueMappingFile');
      my $dateObfuscationFile = $self->getArg('dateObfuscationFile');

      my $ontologyMappingFile = $self->getArg('ontologyMappingFile');
      my $ontologyMappingOverrideFileBaseName = $self->getArg('ontologyMappingOverrideFileBaseName');
      my $overrideFile = undef;
      if( -e $ontologyMappingOverrideFileBaseName){ $overrideFile = $ontologyMappingOverrideFileBaseName }
      elsif( -e "$dirname/$ontologyMappingOverrideFileBaseName") { $overrideFile = "$dirname/$ontologyMappingOverrideFileBaseName" }

      $investigation = CBIL::ISA::InvestigationSimple->new($investigationFile, $ontologyMappingFile, $overrideFile, $valueMappingFile, undef, $isReportMode, $dateObfuscationFile);
    }
    else {
      $investigation = CBIL::ISA::Investigation->new($investigationBaseName, $dirname, "\t");
    }

    eval {
      $investigation->parseInvestigation();
    };
    if($@) {
      $self->logOrError($@);
      next;
    }

    my $investigationId = $investigation->getIdentifier();
    my $studies = $investigation->getStudies();

    my $hasDatasets;

    foreach my $study (@$studies) {
      my %isatabDatasets;

      my $studyAssays = $study->getStudyAssays();

      foreach my $studyAssay (@$studyAssays) {
        my $comments = $studyAssay->getComments();
        foreach my $comment (@$comments) {
          next unless($comment->getQualifier() eq 'dataset_names');
          my @datasetNames = split(/;/, $comment->getValue());
          foreach my $datasetName (@datasetNames) {
            $isatabDatasets{$datasetName}++;
          }
        }
      }

      my $datasetsMatchedInDbCount = $self->checkLoadedDatasets(\%isatabDatasets);

      if($datasetsMatchedInDbCount > 0) {
        $hasDatasets++;
      }
      $study->{_insert_investigations_datasets} = \%isatabDatasets;
    }

    if(!$hasDatasets && !$self->getArg('skipDatasetLookup')) {
      $self->log("Skipping Investigation $investigationId.  No matching datasets in database");
      next;
    }

    $self->sideloadStudy($investigation);
    $self->{_PROTOCOL_APP_NODE_MAP} = {};
    $investigationCount++;
  }

  my $errorCount = $self->{_has_errors};
  if($errorCount) {
    $self->error("FOUND $errorCount ERRORS!");
  }


# $self->loadCharacteristics($charFile);

  $self->logRowsInserted() if($self->getArg('commit'));

  return("Processed $investigationCount Investigations.");
}

sub countLines {
  my ($self, $charFile) = @_;
  my $count = 0;
  open(FILE, "<", $charFile) or die "Cannot open file to count lines: $charFile";
  $count++ while <FILE>;
  close(FILE);
  return $count;
}











# ClinEpi Doesn't have existing database results so all edges are new
sub checkDatabaseProtocolApplicationsAreHandledAndMark {}


sub getConfig {
  my ($self) = @_;

  if (!$self->{config}) {
    my $gusConfigFile = $self->getArg('gusconfigfile');
     $self->{config} = GUS::Supported::GusConfig->new($gusConfigFile);
   }

  $self->{config};
}



sub sideloadStudy {
  my ($self, $inv) = @_;
## PAN data
  my ($dataFh, $dataFile) = tempfile(SUFFIX => '.dat');
  my $configFile = $dataFile . ".ctrl";
  my $table = 'Study.ProtocolAppNode';
  my $allnodes = [];
  my $alledges = [];
  my $allprots = [];
## each study
  my $studies = $inv->getStudies();
  my %nodeStudyInvestigation;
  my $protocolNamesToIdMap = $self->updateProtocolNamesToIdMap();
  foreach my $study (@$studies){
    my $investigationId = $self->loadInvestigation($inv);
    my $identifier = $study->getIdentifier();
    my $description = $study->getDescription();
    my $extDbRlsId = $self->{_external_database_release_id};
    my $gusStudy = GUS::Model::Study::Study->new({name => $identifier, source_id => $identifier, investigation_id =>$investigationId, external_database_release_id=>$extDbRlsId});
    $gusStudy->submit() unless ($gusStudy->retrieveFromDB());
    $gusStudy->setDescription($description);
    my $studyId = $gusStudy->getId();
    my $invId = $gusStudy->getInvestigationId();
    while($study->hasMoreData()) {
	  	$inv->parseStudy($study);
      ## We must update the cached ontology terms for each batch of rows from a study
      ## (Not every batch will use all ontology terms needed later for loading Characteristics)
      $inv->dealWithAllOntologies();
      $self->checkProtocolsAndSetIds($study->getProtocols());
      my $iOntologyTermAccessions = $inv->getOntologyAccessionsHash();
      $self->checkOntologyTermsAndSetIds($iOntologyTermAccessions);
      $self->checkMaterialEntitiesHaveMaterialType($study->getNodes());
      ##
	  	my $edges = $study->getEdges();
      push(@$alledges, @$edges);
	  	my $prots = $study->getProtocols();
      foreach my $prot ( @$prots ){
        my $protocolName = $prot->getProtocolName();
        unless(defined($protocolNamesToIdMap->{$protocolName})){
          push(@$allprots, $prot);
          ## placeholder - map must be updated later with real IDs
          $protocolNamesToIdMap->{$protocolName} = 1;
        }
      }
	  	my $nodes = [];
	  	foreach my $edge (@$edges){
	  		# my $inputs = $edge->getInputs();
	  		my $outputs = $edge->getOutputs();
	  		push(@$nodes, @$outputs);
	  	}
	  	unless(0 < scalar @$nodes){
	  		$nodes = $study->getNodes();
	  	}
      foreach my $node (@$nodes) {
        my $name = $node->getValue();
        $nodeStudyInvestigation{$name} = [$studyId, $invId]; ## for loading STUDYLINK
        my $desc = $node->getDescription();
        my ($isaType) = reverse(split(/::/, ref($node)));
        my $typeId;
        if($node->hasAttribute("MaterialType")) {
          my $materialTypeOntologyTerm = $node->getMaterialType();
          my $gusOntologyTerm = $self->getOntologyTermGusObj($materialTypeOntologyTerm, 0);
          $typeId = $gusOntologyTerm->getId();
        }
        printf $dataFh ("%s\n", join("\t", $typeId, $name, $desc, $isaType));
        push(@$allnodes, $node);
      }
    }
  }
  close($dataFh);
## /each study
  ## PROTOCOLAPPNODE
  my @fields = (
    'PROTOCOL_APP_NODE_ID SEQUENCE(MAX,1)',
    'TYPE_ID',
    'NAME',
    'DESCRIPTION',
    'ISA_TYPE'
  );
  $self->writeConfigFile($configFile, $dataFile, $table, \@fields);
  $self->runSqlldr($dataFile, [[ $table, 'PROTOCOL_APP_NODE_ID', 'Study.PROTOCOLAPPNODE_SQ' ]]);
    # keep the cache up to date as we add new nodes
  my $panNameToIdMap = $self->updateProtocolAppNodeMap($allnodes);

  ## STUDYLINK
  my ($linksFh, $linksFile) = tempfile(SUFFIX => '.dat');
  my $linksCtrlFile = $linksFile . ".ctrl";
  while( my ($name,$panId) = each %{$panNameToIdMap} ){
    die ("UNMAPPED node $name (PANID $panId)") unless(defined($nodeStudyInvestigation{$name}));
    my ($studyId, $invId) = @{ $nodeStudyInvestigation{$name} };
    printf $linksFh ("%s\t%s\n%s\t%s\n", $studyId, $panId, $invId, $panId);
  }
  close($linksFh);
  @fields = (
    'STUDY_LINK_ID SEQUENCE(MAX,1)',
    'STUDY_ID',
    'PROTOCOL_APP_NODE_ID',
  );
  $table = 'Study.StudyLink';
  $self->writeConfigFile($linksCtrlFile, $linksFile, $table, \@fields);
  $self->runSqlldr($linksFile, [[ $table, 'STUDY_LINK_ID', 'Study.STUDYLINK_SQ' ]]);

  ## CHARACTERISTIC
  my ($charFh, $charFile) = tempfile(SUFFIX => '.dat');
  my $charCtrlFile = $charFile . ".ctrl";
  foreach my $node(@$allnodes){
    my $name = $node->getValue(); 
    my $panId = $panNameToIdMap->{$name};
    die "ERROR: PAN with name $name was not inserted into Study.ProtocolAppNode\n" unless $panId;
    my $chars = $self->mungeCharacteristics([$node]);
    foreach my $row (@$chars){
      printf $charFh ("%s\n", join("\t", $panId, @$row));
    }
  }
  close($charFh);
  my @fields = (
    'CHARACTERISTIC_ID SEQUENCE(MAX,1)',
    'protocol_app_node_id',
    'qualifier_id',
    'unit_id',
    'value char(2000)',
    'ontology_term_id'
  );
  my $table = "Study.Characteristic";
  $self->writeConfigFile($charCtrlFile, $charFile, $table, \@fields);
  $self->runSqlldr($charFile, [[ $table, 'CHARACTERISTIC_ID', 'Study.CHARACTERISTIC_sq' ]]);

  ## PROTOCOLS
  # load new protocols and update protcolNamesToIdMap again
  $protocolNamesToIdMap = $self->loadProtocols($allprots);

  ## EDGES
  printf STDERR ("Loading %d Edges\n", scalar @$alledges);
  $self->loadEdges($alledges, $panNameToIdMap, $protocolNamesToIdMap);
  
}

sub loadProtocols {
  my ($self, $prots) = @_;
  # read from database, only need to add new ones
  my $protocolNamesToIdMap = $self->updateProtocolNamesToIdMap();
  my @protsToAdd;
  printf STDERR ("Scanning %d Protocol objects\n", scalar @$prots);
  foreach my $prot ( @$prots ){
    my $protocolName = $prot->getProtocolName();
    unless(defined($protocolNamesToIdMap->{$protocolName})){
      push(@protsToAdd, $prot);
      ## placeholder, do not add again
      $protocolNamesToIdMap->{$protocolName} = 1;
    }
  }
  if(@protsToAdd){
    printf STDERR ("Loading %d new Protocols\n", scalar @protsToAdd);
    $self->SUPER::loadProtocols(\@protsToAdd);
    $protocolNamesToIdMap = $self->updateProtocolNamesToIdMap();
  }
  else {
    printf STDERR ("No new Protocols to load\n");
  }
  return $protocolNamesToIdMap;
}

sub loadEdges {
  my ($self, $edges, $panNameToIdMap, $protocolNamesToIdMap) = @_;
  my ($protappsFh, $protappsFile) = tempfile(SUFFIX => '.dat');
  my $protappsCtrlFile = $protappsFile . ".ctrl";
  my $protappNext = $self->getNextVal('Study.PROTOCOLAPP_SQ');
  my ($inputsFh, $inputsFile) = tempfile(SUFFIX => '.dat');
  my $inputsCtrlFile = $inputsFile . ".ctrl";
  my $inputNext = $self->getNextVal('Study.INPUT_SQ');
  my ($outputsFh, $outputsFile) = tempfile(SUFFIX => '.dat');
  my $outputsCtrlFile = $outputsFile . ".ctrl";
  my $outputNext = $self->getNextVal('Study.OUTPUT_SQ');
  foreach my $edge (@$edges) {
    my $protocolName = $edge->getProtocolApplications()->[0]->getProtocol()->getProtocolName();
    my $gusProtocolId = $protocolNamesToIdMap->{$protocolName};
    my ($input) = @{$edge->getInputs()}; ## ClinEpiDB currently supports only one input per output
    my $inputName = $input->getValue();
    my $inputId = $panNameToIdMap->{ $inputName };
    die("Type $protocolName id $inputName was not loaded, cannot continue loading edges\n") unless $inputId;
    my @outputs = @{$edge->getOutputs()};
    printf $protappsFh ("%s\t%s\n", $protappNext, $gusProtocolId);
    printf $inputsFh ("%s\t%s\t%s\n", $inputNext, $protappNext, $inputId);
    foreach my $output (@outputs){
      my $outputName = $output->getValue();
      my $outputId = $panNameToIdMap->{ $outputName };
      die("Type $protocolName id $outputName was not loaded, cannot continue loading edges\n") unless $outputId;
      printf $outputsFh ("%s\t%s\t%s\n", $outputNext, $protappNext, $outputId);
      $outputNext++;
    }
    $protappNext++;
    $inputNext++;
  }
  close($protappsFh);
  close($inputsFh);
  close($outputsFh);
  my @fields = qw/PROTOCOL_APP_ID PROTOCOL_ID/;
  my $table = 'Study.ProtocolApp';
  $self->writeConfigFile($protappsCtrlFile, $protappsFile, $table, \@fields);
  $self->runSqlldr($protappsFile, [[ $table, 'PROTOCOL_APP_ID', 'Study.PROTOCOLAPP_SQ' ]]);
  @fields = qw/INPUT_ID PROTOCOL_APP_ID PROTOCOL_APP_NODE_ID/;
  $table = 'Study.Input';
  $self->writeConfigFile($inputsCtrlFile, $inputsFile, $table, \@fields);
  $self->runSqlldr($inputsFile, [[ $table, 'INPUT_ID', 'Study.INPUT_SQ' ]]);
  @fields = qw/OUTPUT_ID PROTOCOL_APP_ID PROTOCOL_APP_NODE_ID/;
  $table = 'Study.Output';
  $self->writeConfigFile($outputsCtrlFile, $outputsFile, $table, \@fields);
  $self->runSqlldr($outputsFile, [[ $table, 'OUTPUT_ID', 'Study.OUTPUT_SQ' ]]);
}


sub updateProtocolAppNodeMap {
  my ($self, $nodes) = @_;
  my $database = $self->getDb();
  my $algInvocationId = $database->getDefaultAlgoInvoId();
  my $sql = "select pan.name, pan.protocol_app_node_id from STUDY.PROTOCOLAPPNODE pan where pan.row_alg_invocation_id=?";
  my $dbh = $self->getQueryHandle();
  my $sh = $dbh->prepare($sql);
  $sh->execute($algInvocationId);
  while(my ($name, $panId) = $sh->fetchrow_array()) {
    if(defined($self->{_PROTOCOL_APP_NODE_MAP}->{$name}) && $self->{_PROTOCOL_APP_NODE_MAP}->{$name} ne $panId){
      $self->log("ERROR: Duplicate PROTOCOL_APP_NODE_ID $name!= $panId\n");
    }
    $self->{_PROTOCOL_APP_NODE_MAP}->{$name} = $panId;
  }
  $sh->finish();

  foreach my $node (@$nodes) {
    my $name= $node->getValue();
    $node->{_PROTOCOL_APP_NODE_ID} = $self->{_PROTOCOL_APP_NODE_MAP}->{$name};
    unless(defined($self->{_PROTOCOL_APP_NODE_MAP}->{$name})){
      die "ERROR: impossible $name has no PROTOCOL_APP_NODE_ID\n";
    }
  }
  return $self->{_PROTOCOL_APP_NODE_MAP};
}
sub updateProtocolNamesToIdMap {
  my ($self, $nodes) = @_;
  my $database = $self->getDb();
  my $algInvocationId = $database->getDefaultAlgoInvoId();
  my $sql = "select NAME,PROTOCOL_ID from STUDY.PROTOCOL";
  my $dbh = $self->getQueryHandle();
  my $sh = $dbh->prepare($sql);
  $sh->execute();
  delete($self->{_PROTOCOL_ID_MAP});
  while(my ($name, $Id) = $sh->fetchrow_array()) {
    $self->{_PROTOCOL_ID_MAP}->{$name} = $Id;
  }
  return $self->{_PROTOCOL_ID_MAP};
}

sub getNextVal {
  my ($self,$seqname) = @_;
  my $database = $self->getDb();
  my $sql = "SELECT $seqname.nextval FROM dual";
  my $dbh = $self->getQueryHandle();
  my $sh = $dbh->prepare($sql);
  $sh->execute();
  my ($nextVal) = $sh->fetchrow_array();
  return $nextVal;
}
  
sub writeConfigFile {
# configFile: name of file to write
# dataFile: data file
# table: table to load
# fieldsArray: fields/columns in data file, ordered
  my ($self, $configFile, $dataFile, $table, $fieldsArray) = @_;
  my $modDate = uc(strftime("%d-%b-%Y", localtime));
  my $fields = join(",\n", @$fieldsArray);
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

  open(CONFIG, ">$configFile") or die "Cannot open file $configFile For writing:$!";

  print CONFIG "LOAD DATA
INFILE '$dataFile'
APPEND
INTO TABLE $table
REENABLE DISABLED_CONSTRAINTS
FIELDS TERMINATED BY '\\t'
TRAILING NULLCOLS
(
$fields,
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
row_alg_invocation_id constant $algInvocationId
)\n";
  close CONFIG;
}

sub runSqlldr {
  my ($self,$dataFile, $sequences) = @_;
  my $configFile = $dataFile . ".ctrl";
  my $logFile = $dataFile . ".log";
  my $login       = $self->getConfig->getDatabaseLogin();
  my $password    = $self->getConfig->getDatabasePassword();
  my $dbiDsn      = $self->getConfig->getDbiDsn();
  my ($dbi, $type, $db) = split(':', $dbiDsn);
  my $directMode = 'false';
  if ($self->countLines($dataFile) > 1000000){
    $directMode = 'true';
    $self->log(sprintf("SQLLDR will use DIRECT path [%s]\n", $sequences->[0]->[0] || ""));
  }
  if($self->getArg('commit')) {
    my $exitstatus = system("sqlldr $login/$password\@$db control=$configFile log=$logFile rows=1000 direct=$directMode");
    if($exitstatus != 0){
      die "ERROR: sqlldr returned exit status $exitstatus\nCheck $logFile";
    }
    open(LOG, $logFile) or die "Cannot open log file $logFile: $!";
    while(<LOG>) {
      chomp;
      $self->log($_);
    }
    close LOG;
    unlink $logFile;
  }
  unlink $configFile;
  unlink $dataFile;
  foreach my $seq (@$sequences){
    my ($table,$field, $sequenceName) = @$seq;
    my $dbh = $self->getQueryHandle();
    my ($sequenceValue) = $dbh->selectrow_array("select ${sequenceName}.nextval from dual");
    my ($maxPrimaryKey) = $dbh->selectrow_array("select MAX($field) FROM $table");
    my $sequenceDifference = $maxPrimaryKey - $sequenceValue;
    $self->log("Increasing $field by $sequenceDifference\n");
    if($sequenceDifference > 0) {
      $dbh->do("alter sequence $sequenceName increment by $sequenceDifference");
      $dbh->do("select ${sequenceName}.nextval from dual");
      $dbh->do("alter sequence $sequenceName increment by 1");
    }
  }
}

sub mungeCharacteristics {
  my($self, $nodes) = @_;
  my @charsForLoader;
  foreach my $node(@$nodes){
    if($node->hasAttribute("MaterialType")) {
      # my $materialTypeOntologyTerm = $node->getMaterialType();
      # my $gusOntologyTerm = $self->getOntologyTermGusObj($materialTypeOntologyTerm, 0);
      # my $ontologyTermId = $gusOntologyTerm->getId();
      # $pan->setTypeId($ontologyTermId); # CANNOT Set Parent because OntologyTerm Table has type and subtype.  Both have fk to Sres.ontologyterm

      my $characteristics = $node->getCharacteristics();

      foreach my $characteristic (@$characteristics) {
        if(lc $characteristic->getTermSourceRef() eq 'ncbitaxon') {
          my $taxonId = $self->{_ontology_term_to_identifiers}->{$characteristic->getTermSourceRef()}->{$characteristic->getTermAccessionNumber()};
          # $pan->setTaxonId($taxonId);
        }

        my $charQualifierOntologyTerm = $self->getOntologyTermGusObj($characteristic, 1);
        my $charQualifierId = $charQualifierOntologyTerm->getId();

      # ALWAYS Set the qualifier_id

    #   $gusChar->setQualifierId($charQualifierOntologyTerm->getId()); # CANNOT SET Parent because ontology term id and Unit id.  both fk to sres.ontologyterm

        my $charUnitId;

        if($characteristic->getUnit()) {
          my $unitOntologyTerm = $self->getOntologyTermGusObj($characteristic->getUnit(), 0);
    #     $gusChar->setUnitId($unitOntologyTerm->getId());
          $charUnitId = $unitOntologyTerm->getId();
        }

        my ($charValue, $charOntologyTermId);

        if(lc $characteristic->getTermSourceRef() eq 'ncbitaxon') {
          my $value = $self->{_ontology_term_to_names}->{$characteristic->getTermSourceRef()}->{$characteristic->getTermAccessionNumber()};
    #     $gusChar->setValue($value);
          $charValue = $value;
        }
        elsif($characteristic->getTermAccessionNumber() && $characteristic->getTermSourceRef()) {
          my $valueOntologyTerm = $self->getOntologyTermGusObj($characteristic, 0);
    #     $gusChar->setOntologyTermId($valueOntologyTerm->getId());
              $charOntologyTermId = $valueOntologyTerm->getId();
              $charValue = $characteristic->getTerm();
        }
        else {
    #     $gusChar->setValue($characteristic->getTerm());
              $charValue = $characteristic->getTerm();
        }

        # strip off carriage return
        $charValue =~ s/\r//;


    #   print $charFh join("\t", ($pan->getId(),
        push @charsForLoader, [$charQualifierId, $charUnitId, $charValue, $charOntologyTermId];

      }
    }
  }
  return \@charsForLoader;
}



1;

