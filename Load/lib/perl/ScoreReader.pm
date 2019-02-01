package ClinEpiData::Load::ScoreReader;
use base qw(ClinEpiData::Load::MetadataReader);

#sub cleanAndAddDerivedData {
#  my ($self, $hash) = @_;
# $self->fixDate($hash);
#}
#
#sub fixDate {
#  my ($self, $hash) = @_;
# if($hash->{today} =~ /^(\d\d)-(...)-(\d\d)$/){
#   $hash->{today} = join("", $1, $2, '20', $3);
# }
#}

1;

package ClinEpiData::Load::ScoreReader::HouseholdReader;
use base qw(ClinEpiData::Load::ScoreReader);

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
#   $self->SUPER::cleanAndAddDerivedData($hash);
# }

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "hh";
}

1;

package ClinEpiData::Load::ScoreReader::ParticipantReader;
use base qw(ClinEpiData::Load::ScoreReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return "";
  }
  return "hh";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
}

1;

package ClinEpiData::Load::ScoreReader::ObservationReader;
use base qw(ClinEpiData::Load::ScoreReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "ob";
}

1;

package ClinEpiData::Load::ScoreReader::SampleReader;
use base qw(ClinEpiData::Load::ScoreReader);
use strict;
use File::Basename;


sub read {
  my ($self) = @_;
  my $mdfile = $self->getMetadataFile();
  my $fileBasename = basename($mdfile);
  my $rowExcludes = $self->getRowExcludes();
  my $colExcludes = $self->getColExcludes();
  open(FILE, $mdfile) or die "Cannot open file $mdfile for reading: $!";
  my $header = <FILE>;
  chomp $header;
  my $delimiter = $self->getDelimiter($header);
  my @headers = $self->splitLine($delimiter, $header);
  my $headersAr = $self->adjustHeaderArray(\@headers);
  $headersAr = $self->clean($headersAr);
  my $parsedOutput = {};
  while(<FILE>) {
    chomp;
    my @values = $self->splitLine($delimiter, $_);
    my $valuesAr = $self->clean(\@values);
    foreach my $specnum (qw/1 2 3/){
      foreach my $abslide (qw/a b/){
        my %hash;
        for(my $i = 0; $i < scalar @$headersAr; $i++) {
          my $key = lc($headersAr->[$i]);
          my $value = lc($valuesAr->[$i]);
          next if($value eq '[skipped]');
          $hash{$key} = $value if(defined $value);
        }
        $hash{'specimen number'} = $specnum;
        $hash{'a or b slide'} = $abslide;
        my $primaryKey = $self->makePrimaryKey(\%hash);
        my $parent = $self->makeParent(\%hash);
        next if($self->skipIfNoParent() && !$parent);
        my $parentPrefix = $self->getParentPrefix(\%hash);
        my $parentWithPrefix = $parentPrefix . $parent;
        $hash{'__PARENT__'} = $parentWithPrefix unless($parentPrefix && $parentWithPrefix eq $parentPrefix);
        next unless($primaryKey); # skip rows that do not have a primary key
        if(defined($rowExcludes->{lc($primaryKey)}) && ($rowExcludes->{lc($primaryKey)} eq $fileBasename) || ($rowExcludes->{lc($primaryKey)} eq '__ALL__')){
          next;
        }
				my $prefix = $self->getPrimaryKeyPrefix(\%hash);
        if($prefix) { $primaryKey = $prefix . $primaryKey; }
        if(defined($rowExcludes->{lc($primaryKey)}) && ($rowExcludes->{lc($primaryKey)} eq $fileBasename) || ($rowExcludes->{lc($primaryKey)} eq '__ALL__')){
          next;
        }
        $self->cleanAndAddDerivedData(\%hash);
        foreach my $key (keys %hash) {
          next if(defined($colExcludes->{lc($primaryKey)}) && $colExcludes->{$fileBasename}->{$key} || $colExcludes->{'__ALL__'}->{$key});
          next unless defined $hash{$key}; # skip undef values
          next if($hash{$key} eq '');
          next if($self->seen($parsedOutput->{$primaryKey}->{$key}, $hash{$key}));
          push @{$parsedOutput->{$primaryKey}->{$key}}, $hash{$key};
        }
      }
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

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  my $sample = join("", $hash->{'specimen number'}, $hash->{'a or b slide'});
  for my $specnum (qw/1 2 3/){
    for my $abslide (qw/a b/){
      my $fieldset = join("",$specnum, $abslide);
      unless($sample eq $fieldset){
        for my $col ( qw/sm hook asc trich/ ){
          delete $hash->{ sprintf("%s%s", $col, $sample) };
          delete $hash->{ sprintf("%s%s_count", $col, $sample) };
        }
      }
    }
  }
}

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if(defined($hash->{parent})) {
    return $hash->{parent};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return "";
  }
  return "ob";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{village_id},$hash->{person_id}, $hash->{'specimen number'}, $hash->{'a or b slide'}); 
}

1;
