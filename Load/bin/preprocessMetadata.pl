#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ClinEpiData::Load::MetadataHelper;
use ApiCommonData::Load::OwlReader;
use CBIL::Util::PropertySet;

use Data::Dumper;

# TODO:  ontologyMappingFile is a validation step in the end
my ($help, $ontologyMappingXmlFile, $investigationFile, $type, @metadataFiles, $rowExcludeFile, $colExcludeFile, $parentMergedFile, $parentType, $outputFile, $ancillaryInputFile, $packageName, $propFile, $valueMappingFile, $ontologyOwlFile, $valuesOwlFile, $dateObfuscationFile, @filterParentSourceIds, $isMerged, $readerConfig, $diyMode);

my $ONTOLOGY_MAPPING_XML_FILE = "ontologyMappingXmlFile";
my $INVESTIGATION_FILE = "investigationFile";
my $TYPE = "type";
my $PARENT_TYPE = "parentType";
my $PARENT_MERGED_FILE = "parentMergedFile";
my $METADATA_FILE = "metadataFile";
my $ROW_EXCLUDE_FILE = "rowExcludeFile";
my $COL_EXCLUDE_FILE = "colExcludeFile";
my $OUTPUT_FILE = "outputFile";
my $ANCILLARY_INPUT_FILE = "ancillaryInputFile";
my $PACKAGE_NAME = "packageName";

my $VALUE_MAPPING_FILE = "valueMappingFile";
my $ONTOLOGY_OWL_FILE = "ontologyOwlFile";
my $VALUES_OWL_FILE = "valuesOwlFile";
my $DATE_OBFUSCATION_FILE = "dateObfuscationFile";
my $FILTER_PARENT_SOURCE_ID =  "filterParentSourceId";
my $IS_MERGED =  "isMerged";
my $READER_CONFIG =  "readerConfig";
my $DIY_MODE = "diyMode";

&GetOptions(
	'help|h' => \$help,
  'p|propFile=s' => \$propFile,
  "$TYPE=s" => \$type,
  "$PARENT_TYPE=s" => \$parentType,
  "$PARENT_MERGED_FILE=s" => \$parentMergedFile,
  "$ONTOLOGY_MAPPING_XML_FILE=s" => \$ontologyMappingXmlFile, 
  "i|$INVESTIGATION_FILE=s" => \$investigationFile, 
  "$METADATA_FILE=s" => \@metadataFiles,
  "$ROW_EXCLUDE_FILE=s" => \$rowExcludeFile,
  "$COL_EXCLUDE_FILE=s" => \$colExcludeFile,
  "$OUTPUT_FILE=s" => \$outputFile,
  "$ANCILLARY_INPUT_FILE=s" => \$ancillaryInputFile,
  "$PACKAGE_NAME=s" => \$packageName,
  "$VALUE_MAPPING_FILE=s" => \$valueMappingFile,
  "$ONTOLOGY_OWL_FILE=s" => \$ontologyOwlFile,
  "$VALUES_OWL_FILE=s" => \$valuesOwlFile,
  "$DATE_OBFUSCATION_FILE=s" => \$dateObfuscationFile,
  "$FILTER_PARENT_SOURCE_ID=s" => \@filterParentSourceIds,
	"m|$IS_MERGED" => \$isMerged,
  "$READER_CONFIG=s" => \$readerConfig,
  "$DIY_MODE!" => \$diyMode,
);

my @filesInDirs;
if(defined($readerConfig)){
  if(-e $readerConfig){
    open(FH, "<$readerConfig") or die "$!Cannot read $readerConfig:$!\n";
    my @lines = <FH>;
    $readerConfig = eval (join("", @lines));
  }
  else {
    $readerConfig = eval($readerConfig) if defined($readerConfig);
  }
  die "invalid $READER_CONFIG - check syntax" unless $readerConfig;
}
if(defined($propFile) && -e $propFile) {
  my @properties;
  my $properties = CBIL::Util::PropertySet->new($propFile, \@properties, 1);

  $type ||= $properties->{props}->{$TYPE};
  $parentType ||= $properties->{props}->{$PARENT_TYPE};
  $parentMergedFile ||= $properties->{props}->{$PARENT_MERGED_FILE};

  $rowExcludeFile ||= $properties->{props}->{$ROW_EXCLUDE_FILE};
  $colExcludeFile ||= $properties->{props}->{$COL_EXCLUDE_FILE};
  $outputFile ||= $properties->{props}->{$OUTPUT_FILE};
  $ancillaryInputFile ||= $properties->{props}->{$ANCILLARY_INPUT_FILE};
  $packageName ||= $properties->{props}->{$PACKAGE_NAME};

  $ontologyMappingXmlFile ||= $properties->{props}->{$ONTOLOGY_MAPPING_XML_FILE};

  $ontologyOwlFile ||= $properties->{props}->{$ONTOLOGY_OWL_FILE};
  unless(-e $ontologyOwlFile){
    $ontologyOwlFile = sprintf("%s/ApiCommonData/Load/ontology/release/production/%s.owl", $ENV{PROJECT_HOME}, $ontologyOwlFile);
  }
  $valuesOwlFile ||= $properties->{props}->{$VALUES_OWL_FILE};
  $valueMappingFile ||= $properties->{props}->{$VALUE_MAPPING_FILE};
  $dateObfuscationFile ||= $properties->{props}->{$DATE_OBFUSCATION_FILE};
  $isMerged ||= $properties->{props}->{$IS_MERGED};
  $readerConfig ||= $properties->{props}->{$READER_CONFIG};
  if(defined($readerConfig)){
    if(-e $readerConfig){
      open(FH, "<$readerConfig") or die "$!Cannot read $readerConfig:$!\n";
      my @lines = <FH>;
      $readerConfig = eval (join("", @lines));
    }
    else {
      $readerConfig = eval($readerConfig) if defined($readerConfig);
    }
    die "invalid $READER_CONFIG - check syntax" unless $readerConfig;
  }

  unless(scalar @metadataFiles > 0) {
    my $metadataFileString = $properties->{props}->{$METADATA_FILE};
    @metadataFiles = split(/\s*,\s*/, $metadataFileString);
  }
  #foreach my $mdfile (@metadataFiles){
  while(my $mdfile = shift @metadataFiles){
    if( -d $mdfile ){
      opendir(DH, $mdfile) or die "Cannot read directory $mdfile: $!";
      my @files = map { "$mdfile/$_" } grep { -f "$mdfile/$_" } readdir(DH);
      print STDERR ("metadataFiles = " . join(",",@files) . "\n" );
      closedir(DH);
      push(@filesInDirs, @files);
    }
    else{ push(@filesInDirs,$mdfile) };
  }
  unless(scalar @filterParentSourceIds > 0) {
    my $filterParentSourceIdsString = $properties->{props}->{$FILTER_PARENT_SOURCE_ID};
    @filterParentSourceIds = split(/\s*,\s*/, $filterParentSourceIdsString) if($filterParentSourceIdsString);
  }
}

&usage() if($help);

unless(0 < scalar @metadataFiles){
  @metadataFiles = @filesInDirs;
}

unless(scalar @metadataFiles > 0) {
  &usage("Must Provide at least one meta data file");
}

foreach(@metadataFiles) {
  &usage("Metadata file $_ does not exist") unless(-e $_);
}

unless($outputFile) {
  &usage("outputFile not specified");
}


&usage("Type cannot be null") unless(defined $type);

if($rowExcludeFile) {
  &usage("File $rowExcludeFile does not exist") unless(-e $rowExcludeFile);
}

if($colExcludeFile) {
  &usage("File $colExcludeFile does not exist") unless(-e $colExcludeFile);
}

if($parentMergedFile) {
  &usage("File $parentMergedFile does not exist") unless(-e $parentMergedFile);
}


if($valuesOwlFile){ updateValueMappingFile($valueMappingFile,$valuesOwlFile) }


unless($packageName) {
  $packageName = "ClinEpiData::Load::MetadataReader";
}

my $metadataHelper = ClinEpiData::Load::MetadataHelper->new($type, \@metadataFiles, $rowExcludeFile, $colExcludeFile, $parentMergedFile, $parentType, $ontologyMappingXmlFile, $ancillaryInputFile, $packageName, $readerConfig);

#my $validator = ClinEpiData::Load::MetadataValidator->new($parentMergedFile, $ontologyMappingXmlFile);

unless($isMerged){

	$metadataHelper->merge();
	if($metadataHelper->isValid()) {
	  $metadataHelper->writeMergedFile($outputFile);
	}
	else {
	  $metadataHelper->writeMergedFile($outputFile);
	  die "ERRORS Found.  Please fix and try again.";
	}
}
## Clean up memory before trying to run the next step
$metadataHelper->setMergedOutput({});

if(-e $ontologyMappingXmlFile && -e $valueMappingFile && -e $ontologyOwlFile) {
  my %filterParents = map { $_ => 1 } @filterParentSourceIds;
  $metadataHelper->writeInvestigationTree($ontologyMappingXmlFile, $valueMappingFile, $dateObfuscationFile, $ontologyOwlFile, $outputFile, \%filterParents, $investigationFile);
}

else {
	print "ontologyMappingXmlFile $ontologyMappingXmlFile missing\n" unless(-e $ontologyMappingXmlFile);
	print "valueMappingFile $valueMappingFile missing\n" unless(-e $valueMappingFile);
	print "ontologyOwlFile $ontologyOwlFile missing\n" unless(-e $ontologyOwlFile);
}
	

# check each row that has a parent matches in parent merged file
# check for "USER ERRORS" in any value; keep record of columns and primary keys
# check that each header/qualifier is handled in the ontologymapping xml.  report new and missing
#unless($validator->isValidFile($outputFile)) {
#  open(FILE, ">$outputFile") or die "Cannot open file $outputFile for writing: $!";
#  close FILE;
#}


sub usage {
  my $msg = shift;

  print STDERR "$msg\n" if($msg);
  print STDERR "This script can be run with the prop file or with all with command line arguements.  Command lines values will override the values come from the prop  file.



mode 1:
preprocessMetadata.pl --p|propFile=<FILE>


mode 2:
preprocessMetadata.pl --metadataFile fileA.csv --metadataFile fileB.csv --type Dwelling --ontologyMappingXmlFile XML --rowExcludeFile FILE --colExcludeFile FILE
";
#TODO:  fix error message here
  die "error running preprocessMetadata.pl ";
}


sub updateValueMappingFile {
  my ($valueMappingFile, $valuesOwlFile) = @_;
 #$valuesOwlFile ||= 'clinEpi_values';
 #unless(-e $valuesOwlFile){
 #  $valuesOwlFile = sprintf("%s/ApiCommonData/Load/ontology/harmonization/%s.owl", $ENV{PROJECT_HOME}, $valuesOwlFile);
 #}
  printf STDERR ("Updating %s using %s\n", $valueMappingFile, $valuesOwlFile);
  my $owl = ApiCommonData::Load::OwlReader->new($valuesOwlFile);
  my $terms = $owl->getTerms();
  my %values;
  foreach my $term (@$terms){ $values{$term->{sid}} = $term->{name} }
  open(FH, "<$valueMappingFile") or die "Cannot read $valueMappingFile:$!\n";
  my @finalValues;
  my $wasUpdated = 0;
  while(my $row = <FH>){
    chomp $row;
    next unless $row;
    my(@data) = split(/\t/, $row);
    unless($data[3]){ push(@finalValues,$row); next }
    my ($sid) = ($data[3] =~ m/^\{\{(.*)\}\}$/);
    if(defined($sid)){
      if(defined($values{$sid})){
        $data[4] = $data[3];
        $data[3] = $values{$sid};
        $row = join("\t", @data);
        $wasUpdated = 1;
      }
      else{
        print STDERR "ERROR: $sid not found in $valuesOwlFile\n"
      }
    }
    push(@finalValues, $row);
  }
  close(FH);
  if($wasUpdated){
    rename($valueMappingFile,"$valueMappingFile.orig") or die "Cannot create backup $valueMappingFile.orig: $!";
    open(OF, ">$valueMappingFile") or die "Cannot write $valueMappingFile:$!";
    print OF "$_\n" for @finalValues;
    close(OF);
    printf STDERR ("%s updated, original saved as %s.orig\n", $valueMappingFile, $valueMappingFile);
  }
}
