package ClinEpiData::Load::HbgdReader;
use base qw(ClinEpiData::Load::MetadataReader);

use strict;


use Data::Dumper;

sub clean {
  my ($self, $ar) = @_;

  my $clean = $self->SUPER::clean($ar);

  for(my $i = 0; $i < scalar @$clean; $i++) {

    my $v = $clean->[$i];

    if(lc($v) eq 'na') {
      $clean->[$i] = undef;
    }
  }
  return $clean;

}

sub adjustHeaderArray { 
  my ($self, $ha) = @_;

  my @headers = map { $_ =~ s/\"//g; $_;} @$ha;

  unless($headers[0] eq "PRIMARY_KEY") {
    unshift @headers, "R_PRIMARY_KEY";
  }
  return \@headers;
}

1;

package ClinEpiData::Load::HbgdReader::SitesReader;
use base qw(ClinEpiData::Load::HbgdReader);

use strict;


sub makePrimaryKey {
  my ($self, $hash) = @_;

  return $hash->{siteid};
}

sub makeParent {}


1;

package ClinEpiData::Load::HbgdReader::DwellingReader;
use base qw(ClinEpiData::Load::HbgdReader);

use strict;

use Data::Dumper;

sub makeParent {
  return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return uc $hash->{"primary_key"};
  }

  return $hash->{subjid};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;

  unless($hash->{"primary_key"}) {
    return "HBGDHH_";
  }

  return "";
}


sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;

  my $country = $hash->{country};
  my $citytown = $hash->{citytown};

  if($citytown) {
    $country = "united republic of tanzania" if($country eq "tanzania, united republic of");

    my $ucCountry = join(" ", map { length($_) > 2 ? ucfirst : $_ } split(/\s+/, $country));
    my $ucCityTown = join(" ", map { length($_) > 2 ? ucfirst : $_ } split(/\s+/, $citytown)) . ", $ucCountry";

    $hash->{citytown} = $ucCityTown;
  }
}


1;



package ClinEpiData::Load::HbgdReader::SSReader;
use base qw(ClinEpiData::Load::HbgdDwellingReader);

use strict;

use ClinEpiData::Load::MetadataReader;


sub readAncillaryInputFile {
  my ($self, $file) = @_;

  my %rv;

  open(FILE, $file) or die "Cannot open file $file for reading:$!";

  my $header = <FILE>;
  $header =~s/\n|\r//g;

  my $delimiter = $self->getDelimiter($header);
  my @headers = split($delimiter, $header);
  my $headersAr = $self->adjustHeaderArray(\@headers);
  $headersAr = $self->clean($headersAr);

  my ($prevSubjid, %firstAgedays);

  while(<FILE>) {
    $_ =~ s/\n|\r//g;

    my @values = split($delimiter, $_);
    my $valuesAr = $self->clean(\@values);

    my %hash;
    for(my $i = 0; $i < scalar @$headersAr; $i++) {
      my $header = $headersAr->[$i];
      my $value = $valuesAr->[$i];

      $hash{$header} = $value;
    }

    my $subjid = $hash{subjid};
    my $agedays = $hash{agedays};

    # newsubjid
    if($subjid ne $prevSubjid) {
      $firstAgedays{$subjid} = $agedays;
    }

    if($firstAgedays{$subjid} == $agedays) {
      if($hash{ssstresc} ne "") {
        $rv{$subjid}->{$hash{sstestcd}} = $hash{ssstresc};
      }
      else {
        $rv{$subjid}->{$hash{sstestcd}} = $hash{ssstresn};
        $rv{$subjid}->{$hash{sstestcd}} .= " " . $hash{ssstresu} if($hash{ssstresu});
      }
    }
    $prevSubjid = $subjid;
  }

  return \%rv;
}


sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;

  my $ancillaryData = $self->getAncillaryData();

  my $subjid = $hash->{subjid};

  my $ssData = $ancillaryData->{$subjid};

  foreach my $key(keys %$ssData) {
    next if($key eq '__PARENT__' || $key eq 'primary_key');

    my $value = $ssData->{$key};

    $hash->{$key} = $value;
  }
}



1;

package ClinEpiData::Load::HbgdReader::ParticipantSitesReader;
use base qw(ClinEpiData::Load::HbgdDwellingReader);

use strict;

use ClinEpiData::Load::MetadataReader;

sub readAncillaryInputFile {
  my ($self, $file) = @_;

  my $sitesReader = ClinEpiData::Load::HbgdSitesReader->new($file, undef, undef, undef, undef);  
  return $sitesReader->read();
}



sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;

  my $ancillaryData = $self->getAncillaryData();

  my $siteid = $hash->{siteid};

  my $site = $ancillaryData->{$siteid};
  foreach my $key(keys %$site) {
    next if($key eq '__PARENT__' || $key eq 'primary_key');

    my $value = $site->{$key};

    $hash->{$key} = $value;
  }
}


1;



package ClinEpiData::Load::HbgdReader::ParticipantReader;
use base qw(ClinEpiData::Load::HbgdReader);

use strict;


sub getParentPrefix {
  my ($self, $hash) = @_;

  unless($hash->{"parent"}) {
    return "HBGDHH_";
  }
  return "";


}

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{parent}) {
    return $hash->{parent};
  }

  return $hash->{subjid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;


  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{subjid};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;


  unless($hash->{"primary_key"}) {
    return "HBGDP_";
  }

  return "";
}


1;

package ClinEpiData::Load::HbgdReader::EventReader;
use base qw(ClinEpiData::Load::HbgdReader);

use strict;

use File::Basename;


sub eventType {
  my ($self) = @_;


  if($self->{_event_type}) {
    return $self->{_event_type};
  }

  my $rv;

  my $metadataFile = $self->getMetadataFile();

  my $baseMetaDataFile = basename $metadataFile;


  if($baseMetaDataFile eq 'episodes.txt' || $baseMetaDataFile eq 'DAILY.txt') {
    $rv = "DE";
  }
  elsif($baseMetaDataFile eq 'ANTHRO.txt') {
    $rv = "V";
  }
  else {
    $rv = "TR";
  }

  $self->{_event_type} = $rv;

  return $rv;
}



sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;

  my $eventType = $self->eventType();

  my %eventTypes = ( "DE" => "Diarrhea Episode",
                     "V" => "Anthropometry",
                     "TR" => "Laboratory Test",
      );

  # Event Type
  $hash->{event_type} = $eventTypes{$eventType};

  # MB File
  if($hash->{mbstresc}) {
    my $value = $hash->{mbstresc};
    my $key = $hash->{mbtestcd};

    if($value eq 'positive') {
      $value = 'yes';
    }

    $hash->{$key} = $value;



#    $hash->{$key."_mbspec"} = $hash->{mbspec};
#    $hash->{$key."_mbmethod"} = $hash->{mbmethod};
  }

  # LB File
  if($hash->{lbstresn}) {
    my $value = $hash->{lbstresn};
    my $key = $hash->{lbtestcd};


    if($hash->{'lbspec'} eq 'plasma') {
      $hash->{'lbspec'} = 'blood';
    }

    $hash->{$key} = $value;
#    $hash->{$key."_lbspec"} = $hash->{lbspec};
  }


  # GF File
  if($hash->{gfstresc}) {
    $hash->{'specimentype'} = 'stool';
  }

}


sub getParentPrefix {
  my ($self, $hash) = @_;

  unless($hash->{"parent"}) {
    return "HBGDP_";
  }
  return "";
}

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{parent}) {
    return $hash->{parent};
  }

  return $hash->{subjid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  # no events after 2 years
  return undef if($hash->{agedays} > 745);

  return $hash->{subjid} . "_" . $hash->{agedays};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;

  unless($hash->{"primary_key"}) {
    return "HBGD_" . $self->eventType() . "_";
  }
  return "";
}


1;


package ClinEpiData::Load::HbgdReader::DailyReader;
use base qw(ClinEpiData::Load::HbgdEventReader);

use strict;
use ClinEpiData::Load::MetadataReader;


sub read {
  my ($self) = @_;

  my $metadataFile = $self->getMetadataFile();

  open(FILE, $metadataFile) or die "Cannot open file $metadataFile for reading: $!";

  my $rv = {};

  my $header = <FILE>;
  $header =~s/\n|\r//g;

  my $delimiter = $self->getDelimiter($header);
  my @headers = split($delimiter, $header);
  my $headersAr = $self->adjustHeaderArray(\@headers);
  $headersAr = $self->clean($headersAr);

  my $parsedOutput = {};

  my ($prevSubjid, $prevDay, %episode);

  while(<FILE>) {
    $_ =~ s/\n|\r//g;

    my @values = split($delimiter, $_);
    my $valuesAr = $self->clean(\@values);

    my %hash;
    for(my $i = 0; $i < scalar @$headersAr; $i++) {
      my $key = lc($headersAr->[$i]);
      my $value = lc($valuesAr->[$i]);

      $hash{$key} = $value;
    }

    my $diarfl = $hash{"diarfl"};

    if($hash{subjid} ne $prevSubjid || !$diarfl) {
      if($prevDay) {

        my $primaryKeyPrefix = $self->getPrimaryKeyPrefix(\%episode);
        my $primaryKey = $self->makePrimaryKey(\%episode);

        $primaryKey = $primaryKeyPrefix . $primaryKey;

        my %episodeCopy = %episode;
        $episodeCopy{bldstlfl} = 0 unless($episodeCopy{bldstlfl});
        $episodeCopy{numls} = 0 unless($episodeCopy{numls});
        $episodeCopy{avg_numls} = 0 unless($episodeCopy{avg_numls});
        
        $rv->{$primaryKey} = \%episodeCopy;
      }

      %episode = ();
    }

    if($diarfl) {

      if(!$prevDay) {
        $episode{agedays} = $hash{agedays};
        $episode{subjid} = $hash{subjid};
      }


      $episode{duration}++;
      $episode{bldstlfl}++ if($hash{bldstlfl});
      $episode{numls} = $episode{numls} + $hash{numls};
      $episode{avg_numls} = $episode{numls} / $episode{duration};
    }

#          'subjid' => '1',
#          'agedays' => '20',
#          'diarfl' => '0',
#         'bldstlfl' => '0',
#          'numls' => '0'



    $prevDay = $hash{diarfl};
    $prevSubjid = $hash{subjid};
  }

  if($prevDay) {
    my $primaryKeyPrefix = $self->getPrimaryKeyPrefix(\%episode);
    my $primaryKey = $self->makePrimaryKey(\%episode);

    $primaryKey = $primaryKeyPrefix . $primaryKey;

    my %episodeCopy = %episode;
    $episodeCopy{bldstlfl} = 0 unless($episodeCopy{bldstlfl});
    $episodeCopy{numls} = 0 unless($episodeCopy{numls});
    $episodeCopy{avg_numls} = 0 unless($episodeCopy{avg_numls});

    $rv->{$primaryKey} = \%episodeCopy;
  }

  $self->setParsedOutput($rv);
}




1;

