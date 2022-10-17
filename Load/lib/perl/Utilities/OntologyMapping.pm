package ClinEpiData::Load::Utilities::OntologyMapping;

use strict;
use warnings;
require Exporter;
our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/getEntityOrdering getIRImap/;

use open ':std', ':encoding(UTF-8)';
# use ApiCommonData::Load::OwlReader;
use ClinEpiData::Load::MetadataReader;
use File::Basename qw/basename dirname/;
use Env qw/PROJECT_HOME GUS_HOME/;
use XML::Simple;
use Data::Dumper;

my @GEOHASHTERMS = (
    { source_id => 'EUPATH_0043203', name => ['geohash1'], type => 'characteristicQualifier', parent => 'ENTITY' },
    { source_id => 'EUPATH_0043204', name => ['geohash2'], type => 'characteristicQualifier', parent => 'ENTITY' },
    { source_id => 'EUPATH_0043205', name => ['geohash3'], type => 'characteristicQualifier', parent => 'ENTITY' },
    { source_id => 'EUPATH_0043206', name => ['geohash4'], type => 'characteristicQualifier', parent => 'ENTITY' },
    { source_id => 'EUPATH_0043207', name => ['geohash5'], type => 'characteristicQualifier', parent => 'ENTITY' },
    { source_id => 'EUPATH_0043208', name => ['geohash6'], type => 'characteristicQualifier', parent => 'ENTITY' }
  );
sub setTerms { $_[0]->{_terms} = $_[1] }
sub getTerms { return $_[0]->{_terms} }

sub new {
  my ($class, $file) = @_;
  my $self = {};
  bless ($self, $class);
  if($file){
    $self->{_from_xml} = XMLin($file, ForceArray => 1);
  }
  return $self;
}

sub run {
  my ($self,$owlFile,$functionsFile,$sortByIRI,$varPrefix,$noEntities) = @_;
  unless( -f $owlFile ){
    my $owlDir = "$GUS_HOME/ontology/release/production";
    my $tmp = "$owlDir/$owlFile.owl";
    if(-f $tmp){
      $owlFile = $tmp;
    }
    else{
      opendir(DH, dirname($owlDir));
      my @owls = grep { /\.owl$/i } readdir(DH);
      close(DH);
      print STDERR "Error: $owlFile does not exist\n";
      printf STDERR ("Error: %s does not exist\nAvailable owl files in %s:\n%s\n",
          $owlFile, dirname($tmp), join("\n", @owls));
      exit;
    }
  }
  my $funcToAdd = {};
  if($functionsFile && -e $functionsFile){
    $funcToAdd = $self->readFunctionsFile($functionsFile);
  }
  my $owl = $self->getOwl($owlFile);
  my $vars = $self->getTermsFromOwl($owl, $funcToAdd, $sortByIRI, $varPrefix);
  my $materials = $self->getMaterialTypesFromOwl($owl);
  my $protocols = $self->getProtocols();
  my @terms;
  unless($noEntities){
    push(@terms, $_) for @$materials;
    push(@terms, $_) for @$protocols;
  }
  push(@terms, $_) for @$vars;
  $self->setTerms(\@terms);
  $self->printXml();
}

sub getIRImap {
  my ($file) = @_;
  my $xml = XMLin($file, ForceArray => 1);
  my $map = {};
  foreach my $term ( @{ $xml->{ontologyTerm} } ){
    next unless( $term->{type} eq 'characteristicQualifier' );
    foreach my $name (@{$term->{name}}){
      $map->{ $name } = $term->{source_id};
      $map->{ $term->{source_id} } = $term->{source_id};
      $map->{ lc($term->{source_id}) } = $term->{source_id};
    }
  }
  return $map;
}

sub getEntityOrdering {
  my ($file) = @_;
  my $xml = XMLin($file, ForceArray => 1);
  my $map = {};
  my %forcedOrder = (
    EUPATH_0035127 => 1, # com
    EUPATH_0043226 => 2, # crm
    PCO_0000024    => 3, # house
    EUPATH_0000776 => 4, # hrm
    EUPATH_0000096 => 5, # part
    EUPATH_0000738 => 6, # prm
    EUPATH_0000609 => 7, # sam
  );
  foreach my $term ( @{ $xml->{ontologyTerm} } ){
    if( $term->{type} eq 'materialType' ){
      next unless $forcedOrder{$term->{source_id}};
      $map->{ $term->{name} } = $forcedOrder{$term->{source_id}};
    }
  }
  return $map;
}

sub getOntologyHash {
  my ($self) = @_;
  my $terms = $self->getTerms;
  my $data = {
    ontologymappings => [
    {
      ontologyTerm => $terms
    }
    ]
  };
  return $data;
}

sub getOntologyXml {
  my ($self) = @_;
  my $data = $self->getOntologyHash();
  return XMLout($data, KeepRoot => 1, AttrIndent => 0);
}

sub getOntologyXmlFromFiles {
  my ($self, $files, $protocols) = @_;
  my @allterms;
  foreach my $file (@$files){
    my $terms = $self->getTermsFromSourceFile($file);
    push(@allterms, @$terms);
  }
  foreach my $protocol (@$protocols){
    push(@allterms, { source_id => "TEMP_" . $protocol, type => 'protocol', name => [ $protocol ] }); 
  }
  $self->setTerms(\@allterms);
  return $self->getOntologyXml();
}

sub printXml {
  my ($self,$outFile) = @_;
  my $xml = $self->getOntologyXml();
  if(defined($outFile)){
    open(FH, ">$outFile") or die "Cannot write $outFile:$!";
    print FH $xml;
    close(FH);
  }
  else { print $xml }
}

sub getOwl {
  my $owl = {};
  eval 'require ApiCommonData::Load::OwlReader';
  eval '$owl = ApiCommonData::Load::OwlReader->new($_[1])';
  return $owl;
}

sub getTermsFromSourceFile {
  my ($self,$file,$noFilePrefix) = @_;
  my $reader = ClinEpiData::Load::MetadataReader->new($file);
  my $entity = $reader->getMetadataFileLCB();
  my $fh = $reader->getFH();
  my $headers = $reader->readHeaders();
  my %terms;
  foreach my $col (@$headers){
    $terms{$col} = { 'source_id' => "TEMP_$col", 'name' =>  [$col], 'type' => 'characteristicQualifier', 'parent'=> 'ENTITY'};
  }
  my @sorted;
  @sorted = sort { $a->{name}->[0] cmp $b->{name}->[0] } values %terms;
  unshift(@sorted,{ source_id => "TEMP_$entity", type => 'materialType', name => [ $entity ] });
  unshift(@sorted,{ source_id => 'INTERNAL_X', type => 'materialType', name => [ 'INTERNAL' ] });
  return \@sorted;
}

sub getTermsFromOwl{
  my ($self,$owl,$funcToAdd,$sortByIRI,$varPrefix) = @_;
  my $it = $owl->execute('column2iri');
  my %terms;
  while (my $row = $it->next) {
    my $names = $row->{col}->as_hash()->{literal};
    my $sid = $row->{sid}->as_hash()->{literal};
    my $category = $row->{category}->as_hash()->{literal};
#my $name = "";
    if(ref($names) eq 'ARRAY'){
#$name = lc($names->[0]);
    }
    else {
      my $name = lc($names);
      if($name =~ /,/){
        my @splitnames = split(/\s*,\s*/, $name);
        $names = \@splitnames;
      }
      else {
        $names = [ $name ];
      }
    }
    my %allnames;
    foreach my $n (@$names){
      if($varPrefix){
        next unless ($n =~ /^${varPrefix}::/i)
      }
#if( $n =~ /::/ ) {
#  my ($mdfile,$colName) = split(/::/, $n);
#  print STDERR ("$colName\t$mdfile\n");
#  delete $allnames{$n};
#  $n = $colName;
#}
      $allnames{$n} = 1;
    }
    if(defined($terms{$sid})){
      foreach my $n (@{ $terms{$sid}->{name} } ){ # all rows for this $sid previously read
        $allnames{$n} = 1;
      }
    }
    @$names = sort keys %allnames;
    next unless (@$names);
    my @funcs;
    my $rank = 1;
    $funcToAdd //= {};
    if(0 < keys %$funcToAdd){
      my %funcHash;
      foreach my $id (map { lc } ($sid, @$names)){
        if($funcToAdd->{$id}){
          foreach my $func ( keys %{$funcToAdd->{$id}} ){
            $funcHash{$func} = $funcToAdd->{$id}->{$func};
          }
        }
      }
      @funcs = sort { $funcHash{$a} <=> $funcHash{$b} } keys %funcHash;
    }
    $terms{$sid} = { 'source_id' => $sid, 'name' =>  $names, 'type' => 'characteristicQualifier', 'parent'=> 'ENTITY', 'category' => lc($category), 'function' => \@funcs };
  }
  if($terms{OBI_0001620}){
    foreach my $term (@GEOHASHTERMS){
      $term->{category} = $terms{OBI_0001620}->{category};
      my ($sid) = $term->{source_id};
      $terms{$sid} ||= $term;
    }
  }
  my @sorted;
  if($sortByIRI){
    @sorted = sort { $a->{source_id} cmp $b->{source_id} } values %terms;
  }
  else {
    @sorted = sort { $a->{name}->[0] cmp $b->{name}->[0] } values %terms;
  }
  return \@sorted;
}

sub getMaterialTypesFromOwl {
  my ($self,$owl) = @_;
  my @sorted = ( 
      { source_id => 'INTERNAL_X',     type => 'materialType', name => [ 'INTERNAL' ] },
      { source_id => 'PCO_0000024',    type => 'materialType', name => ['household'] },
      { source_id => 'EUPATH_0000776', type => 'materialType', name => [ 'household_repeated_measures' ] },
    # { source_id => 'EUPATH_0000327', type => 'materialType', name => ['entomology'] },
      { source_id => 'EUPATH_0000096', type => 'materialType', name => ['participant'] },
      { source_id => 'EUPATH_0000738', type => 'materialType', name => [ 'participant_repeated_measures' ] },
      { source_id => 'EUPATH_0000609', type => 'materialType', name => ['sample'] },
      { source_id => 'EUPATH_0035127', type => 'materialType', name => ['community'] },
      { source_id => 'EUPATH_0043226', type => 'materialType', name => ['community_repeated_measures'] },
  ); 
  return \@sorted;
}

sub getProtocols {
  my %protocols = (
      'parent of community_repeated_measures' => 'EUPATH_0035127', # community-community observation
      'parent of household' => 'PCO_0000027', # community-household
      'parent of household_repeated_measures' => 'EUPATH_0015467', # household-household observation
    # entomology => 'EUPATH_0000055', # household-entomology
      'parent of participant' => 'OBI_0600004', # household-participant edge
      'parent of participant_repeated_measures' => 'BFO_0000015', # participant-observation edge
      'parent of sample' => 'OBI_0000659', # observation-sample edge
      );
  my @sorted;
  foreach my $prot ( sort keys %protocols ){
    push(@sorted, { source_id => $protocols{$prot}, type => 'protocol', name => [ $prot ] }); 
  }
  return \@sorted;
}

sub readFunctionsFile {
  my ($self, $functionsFile) = @_;
  my %funcToAdd;
  open(FH, "<$functionsFile") or die "Cannot read $functionsFile:$!\n";
  my $rank = 1;
  while(my $line = <FH>){
    chomp $line;
    my($sid, @funcs) = split(/\t/, $line);
    $sid = lc $sid; # source ID or variable name
      if(0 < @funcs){
        $funcToAdd{$sid} ||= {};
        foreach my $func (@funcs){
          $funcToAdd{$sid}->{$func} = $rank;
          $rank += 1;
        }
      }
  }
  close(FH);
  return \%funcToAdd;
}
1;
