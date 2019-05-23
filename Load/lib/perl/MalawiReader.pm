package ClinEpiData::Load::MalawiReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{study_id};
}
sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $hash->{study_id};
}
# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
# 	$hash->{existence} = 1;
# }

1;

package ClinEpiData::Load::MalawiReader::HouseholdReader;
use base qw(ClinEpiData::Load::MalawiReader);

sub makeParent {
  return undef; 
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "hh";
}

1;

package ClinEpiData::Load::MalawiReader::EntoReader;
use base qw(ClinEpiData::Load::MalawiReader);
use strict;
use warnings;

sub skipIfEmpty { return 1; }
sub skipMask {
	my @vars = qw/__PARENT__ trap_type mostime asprooms/;
	my %mask;
	$mask{ $_ } = 1 for @vars;
	return \%mask;
}

sub rowMultiplier {
  my ($self, $hash) = @_;
	my @clones;
	my %trap = ( study_id => $hash->{study_id}, trap_type => 'lt' );
	my @vars = grep { /_trap$/ } keys %$hash;
	my @values = map { $hash->{$_} } @vars;
	if(@values){
		@trap{@vars} = @values;
		push(@clones, \%trap);
	}
		
	$trap{$_} = $hash->{$_} for @vars;
	my %aspi = ( study_id => $hash->{study_id}, trap_type => 'as' );
	@vars = grep { ! /_trap$/ } keys %$hash;
	@values = map { $hash->{$_} } @vars;
	if(@values){
		@aspi{@vars} = @values;
		push(@clones, \%aspi);
	}
	return \@clones;
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return "";
  }
  return "hh";
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return $hash->{trap_type};
}

1;

package ClinEpiData::Load::MalawiReader::InsectReader;
use base qw(ClinEpiData::Load::MalawiReader::EntoReader);

sub skipIfEmpty { return 1; }

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return "";
  }
  return $hash->{trap_type};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
	return $hash->{trap_type} . "e";
}

1;
package ClinEpiData::Load::MalawiReader::ParticipantReader;
use base qw(ClinEpiData::Load::MalawiReader);

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return "";
  }
  return "hh";
}

1;

package ClinEpiData::Load::MalawiReader::ObservationReader;
use base qw(ClinEpiData::Load::MalawiReader);
use strict;
use warnings;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	unless($hash->{parent}){
		my $parent = $self->getParentParsedOutput()->{$hash->{study_id}};
		$self->skipRow($hash) unless $parent; # triggers skipIfNoParent = 1
	}
}
sub skipIfNoParent { return 1; }
sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "ob";
}

1;

package ClinEpiData::Load::MalawiReader::SampleReader;
use base qw(ClinEpiData::Load::MalawiReader);
use strict;
use warnings;
use Data::Dumper;

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
		return $hash->{parent};
	}
	my $pid = $self->SUPER::makeParent($hash);
	my $fullpid = join("", $self->getParentPrefix($hash),$pid);
	my $pout = $self->getParentParsedOutput();
	if($pout){
		my $parent = $pout->{$pid};
		unless($parent){
			return "";
		}
	}
	return $pid;
}
sub skipIfNoParent { return 1; }

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return "";
  }
  return "ob";
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "s";
}

1;
