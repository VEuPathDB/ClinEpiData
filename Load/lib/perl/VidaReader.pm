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
use strict;
use warnings;

use Data::Dumper;
my %map = (
# en => '',        # Enrollment
  fup => '^f5_',    # 60-day follow-up
  fin => '_find_|f4b_rehyd_hosp',  # Enrollment, outcome 4 hours after rehydration
  reh => '_rehyd_', # Enrollment, outcome if additional rehydration needed
  out => '_out_',   # Enrollment, outcome leaving hospital/health center
);

sub rowMultiplier {   
  my ($self, $hash) = @_;
	return [$hash] if(defined($hash->{primary_key}));
  my @clones;
	my %used;
  while(my ($type,$pattern) = each %map){
		my %clone = (childid => $hash->{childid});
    my @cols = grep { /$pattern/ } keys %$hash;
    $clone{$_} = $hash->{$_} for @cols;
    $clone{observation_type} = $type;
    push(@clones, \%clone);
    $used{$_} = 1 for @cols;
  }
  $hash->{observation_type} = 'enr';
	delete($hash->{$_}) for keys %used;
  push(@clones, $hash);
  return \@clones;
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$self->SUPER::cleanAndAddDerivedData($hash);
  my %clone = ( %$hash );
	my $type = $hash->{observation_type};
  delete($clone{$_}) for qw/__PARENT__ observation_type childid f5_not_conduct f5_date f4b_out_muac_na f4b_rehyd_ref/;
  my $test = 0 ;
	my @stuff;
  while(my ($col,$val) = each %clone){
		$val =~ s/^\s*|\s*$//g;
  	next if $val eq "";
  	$test++;
		push(@stuff, $col);
  }
  unless($test){ $self->skipRow($hash); printf STDERR ("SKIP %s: %s\n", $type , join(",",sort @stuff) || "null") if @stuff; return; }
 	elsif($type eq 'fup' && $hash->{f5_not_conduct}){
 		$self->skipRow($hash);
 	}
 	elsif($type eq 'reh'){
		if(defined($hash->{f4b_rehyd_hosp}) && ($hash->{f4b_rehyd_hosp} eq '1')){
			delete($hash->{f4b_rehyd_hosp});
		}
		else{
 			$self->skipRow($hash);
		}
 	}
	if($type eq 'enr' && $hash->{f4a_date} && $hash->{f4b_date}) {
		delete $hash->{f4b_date};
	}
}


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
  return join("_", $hash->{childid},$hash->{observation_type});
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
  #foreach my $obs (qw/f4a f4b f5 f6 f7/){
  foreach my $obs (qw/f4a f7/){
    # my %clone = ( %$hash );
    my @cols = grep { !/_(dpt|ipv|opv|rot)(1|2|3)/ } grep {/^${obs}_/ } keys %$hash;
    next unless (@cols);
   # my %clone = (
   #   obsid => $obs,
   #   childid => $hash->{childid},
   #   age => $hash->{age},
   # );
   # my @vals = map { $hash->{$_} } @cols;
   # @clone{@cols} = @vals;
   # push(@multi, \%clone);
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
				my @nonempty = grep { /.+/ } @vvals;
				next unless @nonempty;
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
  # return join("_", $hash->{childid},$hash->{age});
  return join("_", $hash->{childid},'enr');
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
  # return join("_", $hash->{childid},$hash->{age},$hash->{obsid});
  return join("_", $hash->{childid},$hash->{obsid});
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
  return join("_", $hash->{childid},'enr');
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "s";
}

1;
