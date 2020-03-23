package ClinEpiData::Load::MetadataReader;

use strict;

use File::Basename;
use Date::Manip qw(Date_Init ParseDate UnixDate DateCalc);
use Text::CSV_XS;
use Data::Dumper;

sub getParentParsedOutput { $_[0]->{_parent_parsed_output} }
sub setParentParsedOutput { $_[0]->{_parent_parsed_output} = $_[1] }

sub getMetadataFile { $_[0]->{_metadata_file} }
sub setMetadataFile { $_[0]->{_metadata_file} = $_[1] }

sub getLineParser { $_[0]->{_line_parser} }
sub setLineParser { $_[0]->{_line_parser} = $_[1] }

sub getRowExcludes { $_[0]->{_row_excludes} }
sub setRowExcludes { $_[0]->{_row_excludes} = $_[1] }

sub getColExcludes { $_[0]->{_col_excludes} }
sub setColExcludes { $_[0]->{_col_excludes} = $_[1] }

sub getParsedOutput { $_[0]->{_parsed_output} }
sub setParsedOutput { $_[0]->{_parsed_output} = $_[1] }

sub getNestedReaders { $_[0]->{_nested_readers} }
sub setNestedReaders { $_[0]->{_nested_readers} = $_[1] }

sub getAncillaryData { $_[0]->{_ancillary_data} }
sub setAncillaryData { $_[0]->{_ancillary_data} = $_[1] }

sub getConfig { return $_[0]->{_CONFIG}->{$_[1]} }
sub setConfig { $_[0]->{_CONFIG}->{$_[1]} = $_[2] }

sub cleanAndAddDerivedData {}
sub updateConfig {}

sub readAncillaryInputFile {
  die "Ancillary File provided bun no method implemented to read it.";
}

sub applyAncillaryData {
  die "Ancillary File provided bun no method implemented to use it.";
}

sub rowMultiplier { return [ $_[1] ]; }

sub seen {
  my ($self, $ar, $v) = @_;

  foreach(@$ar) {
    return 1 if($_ eq $v);
  }
  return 0;
}


sub clean {
  my ($self, $ar) = @_;

  for(my $i = 0; $i < scalar @$ar; $i++) {
    my $v = $ar->[$i];

    if($v =~ /^(\")(.*)(\")$/ || $v =~ /^(\')(.*)(\')$/) {
      $ar->[$i] = $2;
    }
  }
  return $ar;
}

sub adjustHeaderArray { 
  my ($self, $ha) = @_;

  return $ha;
}

sub skipIfNoParent { return 0; }

sub getDelimiter { 
  my ($self, $header, $guessDelimter) = @_;
  my $csv = $self->getLineParser();
  if($header) {
    if($header =~ /\t/) {
      return "\t";
    }
    else {
      $csv->sep_char(",");
      return ",";
    }
  }

  die "Must provide header row to determine the delimiter OR override this function";
}


sub new {
  my ($class, $metadataFile, $rowExcludes, $colExcludes, $parentParsedOutput, $ancillaryInputFile, $config) = @_;

  my $self = bless {}, $class;

  $self->setMetadataFile($metadataFile);
  $self->setRowExcludes($rowExcludes);
  $self->setColExcludes($colExcludes);
  $self->setParentParsedOutput($parentParsedOutput);

  if(-e $ancillaryInputFile) {

    my $ancillaryData = $self->readAncillaryInputFile($ancillaryInputFile);

    $self->setAncillaryData($ancillaryData);
  }
  $self->{_CONFIG} = $config;

  my $csv = Text::CSV_XS->new({ binary => 1, sep_char => "\t", quote_char => '"', allow_loose_quotes => 1 }) 
      or die "Cannot use CSV: ".Text::CSV_XS->error_diag ();  

  $self->setLineParser($csv);
  return $self;
}


sub splitLine {
  my ($self, $delimiter, $line) = @_;

  my $csv = $self->getLineParser();

  my @columns;
  if($csv->parse($line)) {
    @columns = $csv->fields();
  }
  else {
      my $error= "".$csv->error_diag;
    die "Could not parse line: $error";
  }

  return wantarray ? @columns : \@columns;
}

sub getMetadataFileLCB {
  return lc(fileparse($_[0]->getMetadataFile(), qr/\.[^.]+$/));
}

sub read {
  my ($self) = @_;
  $self->updateConfig();
  my $metadataFile = $self->getMetadataFile();
  my ($fileBasename) = $self->getMetadataFileLCB();
  my $colExcludes = $self->getColExcludes();
  my $rowExcludes = $self->getRowExcludes();
  my $forceFilePrefix = $self->getConfig('forceFilePrefix');
  my $cleanFirst = $self->getConfig('cleanFirst');
  open(FILE, $metadataFile) or die "Cannot open file $metadataFile for reading: $!";
  my $header = <FILE>;
  $header =~s/\n|\r//g;
  my $delimiter = $self->getDelimiter($header);
  my @headers = $self->splitLine($delimiter, $header);
  my $headersAr = $self->adjustHeaderArray(\@headers);
  $headersAr = $self->clean($headersAr);
  my $parsedOutput = {};
  while(my $row = <FILE>) {
    $row =~ s/\n|\r//g;
    my @values = $self->splitLine($delimiter, $row);
    my $valuesAr = $self->clean(\@values);
    my %rowData;
    for(my $i = 0; $i < scalar @$headersAr; $i++) {
      my $key = lc($headersAr->[$i]);
      if($forceFilePrefix){ $key = join("::", $fileBasename, $key) }
      my $value = lc($valuesAr->[$i]);
      next if($value eq '[skipped]');
      $rowData{$key} = $value if(defined $value);
    }
    my $rowMulti = $self->rowMultiplier(\%rowData);
    foreach my $hash ( @$rowMulti ) {
       $self->cleanAndAddDerivedData($hash) if $cleanFirst;
       my $primaryKey = $self->makePrimaryKey($hash);
       my $parent = $self->makeParent($hash);
       next if($self->skipIfNoParent() && !$parent);
       my $parentPrefix = $self->getParentPrefix($hash);
       my $parentWithPrefix = $parentPrefix . $parent;
       $hash->{'__PARENT__'} = $parentWithPrefix unless($parentPrefix && $parentWithPrefix eq $parentPrefix);
       next unless($primaryKey); # skip rows that do not have a primary key
       if(defined($rowExcludes->{lc($primaryKey)}) && ($rowExcludes->{lc($primaryKey)} eq $fileBasename) || ($rowExcludes->{lc($primaryKey)} eq '__ALL__')){
         next;
       }
       my $prefix = $self->getPrimaryKeyPrefix($hash);
       if($prefix) { $primaryKey = $prefix . $primaryKey; }
       if(defined($rowExcludes->{lc($primaryKey)}) && ($rowExcludes->{lc($primaryKey)} eq $fileBasename) || ($rowExcludes->{lc($primaryKey)} eq '__ALL__')){
         next;
       }
       $self->cleanAndAddDerivedData($hash) unless $cleanFirst;
       foreach my $key (keys %$hash) {
         next if(defined($colExcludes->{$fileBasename}) && $colExcludes->{$fileBasename}->{$key} || $colExcludes->{'__ALL__'}->{$key});
         next unless defined $hash->{$key}; # skip undef values
         next if($hash->{$key} eq '');
         next if($self->seen($parsedOutput->{$primaryKey}->{$key}, $hash->{$key}));
         push @{$parsedOutput->{$primaryKey}->{$key}}, $hash->{$key};
       }
    }
  }
  close FILE;
  my $rv = {};
	my $skipped = 0;
	my $skipEmpty = $self->skipIfEmpty();
	my $skipVars = $self->skipMask();
	my $minVars = $self->reportMinimumVariables();
  foreach my $primaryKey (keys %$parsedOutput) {
		my $row = {};
		my %usedVars;
    foreach my $var (keys %{$parsedOutput->{$primaryKey}}) {
      my @values = @{$parsedOutput->{$primaryKey}->{$var}};
      for(my $i = 0; $i < scalar @values; $i++) {
        my $value = $values[$i];
        my $newVar = $i == 0 ? $var : "${var}_$i";
       #$rv->{$primaryKey}->{$newVar} = $values[$i];
        $row->{$newVar} = $values[$i];
				next unless($skipEmpty &! $skipVars->{$var});
				if($values[$i] =~ /.+/){
					$usedVars{$var} = 1;
				}
      }
    }
		if($skipEmpty){
			my $count = scalar keys %usedVars;
			if( $count eq 0 ){
				$skipped++;
				next;
			}
	  	if($count < $minVars){
	  		printf STDERR ("LOW NUMBER OF VALUES %s: %s\n", $primaryKey, join(" ", sort keys %usedVars));
	  	}
		}
		$rv->{$primaryKey} = $row;
  }
  $self->setParsedOutput($rv);
	printf STDERR ("Skipped %d empty nodes\n", $skipped) if $skipped || $skipEmpty;
}

sub makePrimaryKey {
  die "SUBCLASS must override makePrimaryKey method";
} 
sub makeParent {
  die "SUBCLASS must override makeParent method";
}

sub getPrimaryKeyPrefix {
  return undef;
}

sub getParentPrefix {
  return undef;
}

sub skipIfEmpty {
## omit rows from output when they have no data
  return undef;
}

sub skipMask {
## use with skipIfEmpty()
## hash of variables to disregard when checking if a merged row is empty
## override to add more variables 
  return { __PARENT__ => 1 };
}

sub reportMinimumVariables {
## use with skipIfEmpty()
## print a warning if number of values is below this number
	return 0;
}

sub skipRow {
  my ($self, $hash) = @_;
	delete $hash->{$_} for keys %$hash;
}

sub countValues {
  my ($self, $hash, @cols) = @_;
	my %clone = ( %$hash );
	delete $clone{$_} for @cols;
	my @vals = grep { /.+/ } values %clone;
	unless ( 0 < @vals ){ $self->skipRow($hash); }
	return scalar @vals;
}

sub formatDate {
  my ($self, $date, $format) = @_;
	return unless $date;
  unless($format){
    if($date =~ /^\d{1,2}\/\d{1,2}\/\d{2,4}$/){ $format = "US" }
  }
	else{ $format ||= "non-US" }
  Date_Init("DateFormat=$format"); 
  my $formattedDate = UnixDate(ParseDate($date), "%Y-%m-%d");

  unless($formattedDate) {
    warn "Date Format not supported for [$date]\n";
    return undef;
  }

  return $formattedDate;
}

sub dateDiff {
  my($self, $var1, $var2) = @_;
  if($var1 && $var2){
    my $start = ParseDate($var1);
    my $end = ParseDate($var2);
    my @delta = split(/:/, DateCalc($start,$end));
    return int(($delta[4] / 24) + 0.5);
  }
  return undef;
}

sub debug { printf STDERR ("DEBUG: %s\n", $_[1]) }

1;






