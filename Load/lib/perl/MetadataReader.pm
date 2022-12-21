package ClinEpiData::Load::MetadataReader;

use strict;

use File::Basename;
use Date::Manip qw(Date_Init ParseDate UnixDate DateCalc);
use Text::CSV;
use Data::Dumper;
use open ':std', ':encoding(UTF-8)';

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
  die "Ancillary File provided but no method implemented to read it.";
}

sub applyAncillaryData {
  die "Ancillary File provided but no method implemented to use it.";
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
    $ar->[$i] =~ s/^\s+|\s+$//g;
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
  return ($self->{_delimiter}) if defined($self->{_delimiter});
  my $csv = $self->getLineParser();
  my $delim;
  if($header) {
    if($header =~ /\t/) {
      $csv->sep_char("\t");
      $delim = "\t";
    }
    else {
      $csv->sep_char(",");
      $delim =  ",";
    }
  }
  my $file = $self->getMetadataFile();
  die "ERROR reading $file: Must provide header row to determine the delimiter OR override this function" unless defined($delim);
  $self->{_delimiter} = $delim;
  return $delim;
}

sub close{
  my ($self) = @_;
  close($self->getFH());
  delete ($self->{_fileHandle});
}

sub getFH {
  my ($self) = @_;
  return $self->{_fileHandle} if(defined($self->{_fileHandle}) && ref($self->{_fileHandle} eq 'GLOB'));
  my $file = $self->getMetadataFile();
  open($self->{_fileHandle}, "<$file") or die "Cannot open $file for reading: $!";
  return $self->{_fileHandle};
}
sub readHeaders {
  my ($self,$fh) = @_;
  $fh //= $self->getFH(); 
  my $header = <$fh>;
  $header =~ s/[\n\r\l]//g;
  $header =~ s/\x{FEFF}//;
  $header =~ s/\N{U+FEFF}//;
  $header =~ s/\N{ZERO WIDTH NO-BREAK SPACE}//;
  $header =~ s/\N{BOM}//;
  my $delimiter = $self->getDelimiter($header);
  my @headers = $self->splitLine($delimiter, $header);
  return \@headers;
}


sub new {
  my ($class, $metadataFile, $rowExcludes, $colExcludes, $parentParsedOutput, $ancillaryInputFiles, $config) = @_;

  my $self = bless {}, $class;

  $self->setMetadataFile($metadataFile);
  $self->setRowExcludes($rowExcludes);
  $self->setColExcludes($colExcludes);
  $self->setParentParsedOutput($parentParsedOutput);

  my @ancfiles;
  foreach my $ancf (@$ancillaryInputFiles ){ 
    if( $ancf && -e $ancf ){ push(@ancfiles, $ancf) }
  }
  if(0 < scalar @ancfiles){
    my $ancillaryData = $self->readAncillaryInputFile(\@ancfiles);
    $self->setAncillaryData($ancillaryData);
  }
  $self->{_CONFIG} = $config;

  my $csv = Text::CSV->new({ binary => 1, sep_char => "\t", quote_char => '"', allow_loose_quotes => 1 }) 
      or die "Cannot use CSV: ".Text::CSV->error_diag ();  

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
    die "Could not parse line: $error\nin file " . $self->getMetadataFile();
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
  my $category = $self->getConfig('category');
  if($self->getConfig('idMap') && $category){
    unless($self->getConfig('idMap')->{$fileBasename}->{$category}){
      printf STDERR ("Skipping %s: %s not defined for category %s\n", $metadataFile,$fileBasename,$category);
      return;
    }
  }
  printf STDERR ("Parsing: $metadataFile\n");
  my $colExcludes = $self->getColExcludes();
  my $rowExcludes = $self->getRowExcludes();
  my $forceFilePrefix = $self->getConfig('forceFilePrefix');
  my $cleanFirst = $self->getConfig('cleanFirst');
# open(FILE, $metadataFile) or die "Cannot open file $metadataFile for reading: $!";
# my $header = <FILE>;
# $header =~ s/\n|\r//g;
# $header =~ s/^\x{FEFF}//;
# $header =~ s/^\N{U+FEFF}//;
# $header =~ s/^\N{ZERO WIDTH NO-BREAK SPACE}//;
# $header =~ s/^\N{BOM}//;
# my $delimiter = $self->getDelimiter($header);
# my @headers = $self->splitLine($delimiter, $header);
  my $fh = $self->getFH();
  my @headers = @{ $self->readHeaders() };
  my $delimiter = $self->getDelimiter();
  my $headersAr = $self->adjustHeaderArray(\@headers);
  $headersAr = $self->clean($headersAr);
  my $parsedOutput = {};
  my $LINE = 0; # input file line number
  #my %dupcheck;
  while(my $row = <$fh>) {
    $LINE++;
    $row =~ s/\n|\r//g;
    my @values = $self->splitLine($delimiter, $row);
    my $valuesAr = $self->clean(\@values);
    my %rowData;
    for(my $i = 0; $i < scalar @$headersAr; $i++) {
      my $key = lc($headersAr->[$i]);
      next if ($key eq ''); ## empty column header (usually row number, 1st column)
      if($forceFilePrefix){ $key = join("::", $fileBasename, $key) }
      my $value = lc($valuesAr->[$i]);
      next if($value eq '[skipped]');
      $rowData{$key} = $value if(defined $value);
      $rowData{$key} =~ s/^\s*|\s*$//g; # trim ALWAYS
    }
    my $rowMulti = $self->rowMultiplier(\%rowData);
    foreach my $hash ( @$rowMulti ) {
       $hash->{'__line__'} = $LINE;
       $self->cleanAndAddDerivedData($hash) if $cleanFirst;
       my $primaryKey = $self->makePrimaryKey($hash);
 # die "$primaryKey\n" . Dumper $hash if ($primaryKey =~ /:/ );
       my $parent = $self->makeParent($hash);
       next if($self->skipIfNoParent() && !$parent);
       my $parentPrefix = $self->getParentPrefix($hash);
       my $parentWithPrefix = $parentPrefix . $parent;
       $hash->{'__PARENT__'} = $parentWithPrefix unless($parentPrefix && $parentWithPrefix eq $parentPrefix);
       delete $hash->{'__line__'};
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
         $hash->{$key} =~ s/^\s*|\s*$//g; # trim whitespace
         next if($self->seen($parsedOutput->{$primaryKey}->{$key}, $hash->{$key}));
         push @{$parsedOutput->{$primaryKey}->{$key}}, $hash->{$key};
       }
    }
  }
  $self->close();
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
        my $newVar = $i == 0 ? $var : "${var}!!$i";
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

sub formatTime {
  my ($self, $time, $format) = @_;
	return unless $time;
	$time =~ s/^0:/12:/;
	$time = UnixDate(ParseDate($time), "%H%M");
  my $formattedTime = UnixDate(ParseDate($time), "%H%M");
  unless($formattedTime) {
    warn "Time Format not supported for [$time]\n";
    return undef;
  }
  return $formattedTime;
}

sub formatDate {
  my ($self, $date, $format) = @_;
	return unless $date;
 #unless($format){
 #  if($date =~ /^\d{1,2}\/\d{1,2}\/\d{2,4}$/){ $format = "US" }
 #}
 #else{ $format ||= "non-US" }
  Date_Init("DateFormat=$format") if $format; 
  my $formattedDate = UnixDate($date, "%Y-%m-%d");

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

sub dateDiffWeek {
  my($self, $var1, $var2) = @_;
  if($var1 && $var2){
    my $start = ParseDate($var1);
    my $end = ParseDate($var2);
    my @delta = split(/:/, DateCalc($start,$end));
    return $delta[3];
  }
  return undef;
}

sub debug { printf STDERR ("DEBUG: %s\n", $_[1]) }

1;






