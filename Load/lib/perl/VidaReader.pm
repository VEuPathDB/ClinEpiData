package ClinEpiData::Load::VidaReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	foreach my $col (keys %$hash){
		if($hash->{$col} eq "."){
			delete($hash->{$col});
		}
	}
}

1;

package ClinEpiData::Load::VidaReader::HouseholdReader;
use base qw(ClinEpiData::Load::VidaReader);

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $hash->{childid};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "hh";
}

1;

package ClinEpiData::Load::VidaReader::ParticipantReader;
use base qw(ClinEpiData::Load::VidaReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{childid};
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
  return $hash->{childid};
}

1;

package ClinEpiData::Load::VidaReader::ObservationReader;
use base qw(ClinEpiData::Load::VidaReader);


sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{childid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	my $date;
	
  return join("_", $hash->{childid},$hash->{age});
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "ob";
}

1;

package ClinEpiData::Load::VidaReader::SubobservationReader;
use base qw(ClinEpiData::Load::VidaReader);

sub rowMultiplier {   
  my ($self, $hash) = @_;
  my @multi;
	my @dates;
  foreach my $obs (qw/f4a f4b f5 f6 f7/){
    # my %clone = ( %$hash );
		my @cols = grep { !/_(dpt|ipv|opv|rot)(1|2|3)/ } grep {/^${obs}_/ } keys %$hash;
		next unless (@cols);
		my %clone = (
			obsid => $obs,
			childid => $hash->{childid},
			age => $hash->{age},
		);
		my @vals = map { $hash->{$_} } @cols;
		@clone{@cols} = @vals;
    push(@multi, \%clone);
		for my $vac ( qw/dpt ipv opv rot/ ){
			for my $ct ( 1..3 ){
				my @vcols = grep { /^${obs}_${vac}${ct}/ } keys %$hash;
				next unless (@vcols);
				my %vclone = (
					obsid => sprintf("%s_%s%d", $obs, $vac, $ct),
					childid => $hash->{childid},
					age => $hash->{age},
				);
				my @vvals = map { $hash->{$_} } @vcols;
				@vclone{@vcols} = @vvals;
  		  push(@multi, \%vclone);
			}
		}
  }
	$self->skipRow($hash);
  return \@multi;
}

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return join("_", $hash->{childid},$hash->{age});
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
	my $date;
  return join("_", $hash->{childid},$hash->{age},$hash->{obsid});
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "ob";
}

1;

package ClinEpiData::Load::VidaReader::SampleReader;
use base qw(ClinEpiData::Load::VidaReader);
use strict;

sub makeParent {
  my ($self, $hash) = @_;
  if(defined($hash->{parent})) {
    return $hash->{parent};
  }
  return $self->makePrimaryKey($hash);
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
  return join("_", $hash->{childid},$hash->{age});
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "s";
}

1;
