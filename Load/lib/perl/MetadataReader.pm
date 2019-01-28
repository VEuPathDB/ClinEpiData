package ClinEpiData::Load::MetadataReader;

use strict;

use File::Basename;

use Data::Dumper;

sub getParentParsedOutput { $_[0]->{_parent_parsed_output} }
sub setParentParsedOutput { $_[0]->{_parent_parsed_output} = $_[1] }

sub getMetadataFile { $_[0]->{_metadata_file} }
sub setMetadataFile { $_[0]->{_metadata_file} = $_[1] }

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

sub cleanAndAddDerivedData {}

sub readAncillaryInputFile {
  die "Ancillary File provided bun no method implemented to read it.";
}

sub applyAncillaryData {
  die "Ancillary File provided bun no method implemented to use it.";
}

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

  if($header) {
    if($header =~ /\t/) {
      return qr/\t/;
    }
    else {
      return qr/,/;
    }
  }

  die "Must provide header row to determine the delimiter OR override this function";
}


sub new {
  my ($class, $metadataFile, $rowExcludes, $colExcludes, $parentParsedOutput, $ancillaryInputFile) = @_;

  my $self = bless {}, $class;

  $self->setMetadataFile($metadataFile);
  $self->setRowExcludes($rowExcludes);
  $self->setColExcludes($colExcludes);
  $self->setParentParsedOutput($parentParsedOutput);

  if(-e $ancillaryInputFile) {

    my $ancillaryData = $self->readAncillaryInputFile($ancillaryInputFile);

    $self->setAncillaryData($ancillaryData);
  }

  return $self;
}


sub splitLine {
  my ($self, $delimiter, $line) = @_;

  my @a = split($delimiter, $line);

  return wantarray ? @a : \@a;
}


sub read {
  my ($self) = @_;

  my $metadataFile = $self->getMetadataFile();

  my $colExcludes = $self->getColExcludes();
  my $rowExcludes = $self->getRowExcludes();

  my $fileBasename = basename $metadataFile;

  open(FILE, $metadataFile) or die "Cannot open file $metadataFile for reading: $!";

  my $header = <FILE>;
  $header =~s/\n|\r//g;

  my $delimiter = $self->getDelimiter($header);

  my @headers = $self->splitLine($delimiter, $header);

  my $headersAr = $self->adjustHeaderArray(\@headers);

  $headersAr = $self->clean($headersAr);



  my $parsedOutput = {};

  while(<FILE>) {
    $_ =~ s/\n|\r//g;

    my @values = $self->splitLine($delimiter, $_);
    my $valuesAr = $self->clean(\@values);

    my %hash;
    for(my $i = 0; $i < scalar @$headersAr; $i++) {
      my $key = lc($headersAr->[$i]);
      my $value = lc($valuesAr->[$i]);

      # TODO: move this to PRISM class clean method
      next if($value eq '[skipped]');

      $hash{$key} = $value if(defined $value);
    }

    my $primaryKey = $self->makePrimaryKey(\%hash);
    my $parent = $self->makeParent(\%hash);

    next if($self->skipIfNoParent() && !$parent);

    my $parentPrefix = $self->getParentPrefix(\%hash);
    my $parentWithPrefix = $parentPrefix . $parent;

    $hash{'__PARENT__'} = $parentWithPrefix unless($parentPrefix && $parentWithPrefix eq $parentPrefix);

    next unless($primaryKey); # skip rows that do not have a primary key
    if(($rowExcludes->{$primaryKey} eq $fileBasename) || ($rowExcludes->{$primaryKey} eq '__ALL__')){
			next;
		}

    $primaryKey = $self->getPrimaryKeyPrefix(\%hash) . $primaryKey;

    if(($rowExcludes->{$primaryKey} eq $fileBasename) || ($rowExcludes->{$primaryKey} eq '__ALL__')){
			next;
		}

    $self->cleanAndAddDerivedData(\%hash);

    foreach my $key (keys %hash) {
      next if($colExcludes->{$fileBasename}->{$key} || $colExcludes->{'__ALL__'}->{$key});
      next unless defined $hash{$key}; # skip undef values
      next if($hash{$key} eq '');

      next if($self->seen($parsedOutput->{$primaryKey}->{$key}, $hash{$key}));

      push @{$parsedOutput->{$primaryKey}->{$key}}, $hash{$key};
    }

  }

  close FILE;

  my $rv = {};

  foreach my $primaryKey (keys %$parsedOutput) {

    foreach my $key (keys %{$parsedOutput->{$primaryKey}}) {

      my @values = @{$parsedOutput->{$primaryKey}->{$key}};

      for(my $i = 0; $i < scalar @values; $i++) {
        my $value = $values[$i];

        my $newKey = $i == 0 ? $key : "${key}_$i";
        $rv->{$primaryKey}->{$newKey} = $values[$i];
      }
    }
  }


  $self->setParsedOutput($rv);
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

sub skipRow {
  my ($self, $hash) = @_;
	delete $hash->{$_} for keys %$hash;
}

1;






