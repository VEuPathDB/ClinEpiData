package ClinEpiData::Load::MetadataReaderXT;
use base qw(ClinEpiData::Load::MetadataReader);
# XT = Extended: use valueMap.txt as ancillary input file 
# apply value mapping and/or replace variables with IRI (Source ID)
use Data::Dumper;
use strict;
use warnings;

sub readAncillaryInputFile {
# use for 1. value mapping, and 2. IRI mapping
  my ($self, $ancFiles) = @_;
  my $anc = {};
  foreach my $ancFile (@$ancFiles){
    next unless $ancFile;
    open(FH, "<$ancFile") or die "Cannot read $ancFile:$!\n";
    while(my $row = <FH>){
      chomp $row;
      next unless $row;
      next if ($row =~ /^\s*#/);
      $row =~ s/(\r|\l)$//g;
      my($col, $iri, $val, $mapVal) = split(/\t/,$row);
      if(defined($val) && defined($mapVal)){
        if($val eq ':::append:::'){ # add a static value to every row in a metadatafile
          # where $col is the metadatafile, $iri is the variable
          $anc->{$val}->{$col}->{$iri}=$mapVal;
        }
        elsif($val =~ /^:::/){
          $anc->{$val}->{$col} ||= [];
          push(@{$anc->{$val}->{$col}},$mapVal);
        }
        else {
          $anc->{var}->{lc($col)}->{lc($val)} =$mapVal;
          $anc->{var2}->{lc($iri)}->{lc($val)} = $mapVal;
        }
      }
      ## For applyMappedIRI
      unless($col && $iri && (lc($col) eq lc($iri))){
      # Only map if they are different (avoids overwriting from another line
        $anc->{iri}->{lc($col)}=lc($iri);
      }
    }
    close(FH);
  }
  return $anc;
}

sub applyMappedValues {
  my ($self,$hash) = @_;
  my $anc = $self->getAncillaryData();
  my $type = 'var';
  if($self->getConfig('applyMappedIRI')){
    $type = 'var2';
  }
  while(my ($col, $v0) = each %$hash){
    my $origKey = $col;
    $col = lc($col);
    if($col !~ /^$origKey$/){ $hash->{$col} = $v0; $hash->{$origKey} = undef }
    #next unless(defined($v0));
    $v0 = lc($v0);
    $v0 =~ s/^\s*|\s*$//g;
    ## do basic value mapping
    if(defined($anc->{$type}->{$col})) {
      if(defined($anc->{$type}->{$col}->{$v0})){ # value match
        if(uc($anc->{$type}->{$col}->{$v0}) eq ':::UNDEF:::'){
          delete($hash->{$col});
          $v0 = undef;
          next;
        }
        else {
          $v0 = $hash->{$col} = $anc->{$type}->{$col}->{$v0};
          # printf STDERR ("Matched [$col] = [$v0] ... %s => %s\n", $v0, $hash->{$col});
        }
      }
    }
    if(defined($anc->{':::regex:::'}->{$col})){
      foreach my $regex (@{$anc->{':::regex:::'}->{$col}}){
        my $newVal = $v0;
        my $oper = sprintf("\$newVal =~ %s", $regex);
        eval $oper;
        # printf STDERR ("REGEX %s => %s\n", $hash->{$col}, $v0) if($hash->{$col} =~ /:/);
        $v0 = $hash->{$col} =$newVal;
      }
    }
    if(defined($anc->{':::eval:::'}->{$col})){
      foreach my $eval (@{$anc->{':::eval:::'}->{$col}}){
        # printf STDERR ("EVAL: %s\n", $anc->{$type}->{$col}->{':::eval:::'});
        eval $eval; 
        if($@){ die "$@" }
        $v0 = $hash->{$col};
      }
    }
    if(defined($anc->{':::function:::'}->{$col})){
      foreach my $func (@{$anc->{':::function:::'}->{$col}}){
        my $newVal = $self->$func($v0);
        if(defined($newVal)){
          $v0 = $hash->{$col} = $newVal
        }
        # printf STDERR ("FUNCTION %s: %s => %s\n", $func, $v0, $newVal);
      }
    }
  }
  my $mdfile = $self->getMetadataFileLCB();
  if(defined($anc->{':::append:::'}->{$mdfile})){
    while(my ($k, $v) = each %{$anc->{':::append:::'}->{$mdfile}}){
      $hash->{$k} = $v;
    }
  }
}

sub applyMappedIRI {
  my ($self,$hash,$keep) = @_;
  my $anc = $self->getAncillaryData();
  while(my ($col, $val) = each %$hash){
    my $iri = $anc->{iri}->{$col};
    next unless($iri);
    next if($iri eq $col);
    if($iri){
      $hash->{$iri} = $hash->{$col};
      delete($hash->{$col}) unless $keep;
      # printf STDERR ("rename %s => %s\n", $col, $iri);
    }
  }
}

sub formatTimeHHMM {
  my($self,$val) = @_;
  my($hr,$min,$half) = ($val =~ m/^(\d{1,2}).?(\d\d).?(am|pm)?$/i);
  # $min = $min / 60; # for decimal
  if(defined($half) && ($half eq 'pm') && ($hr < 12)){
    $hr = $hr + 12;
  }
  return sprintf("%02d%02d", $hr, $min);
}

sub formatEuroDate {
  my $v = $_[1];
  if($v =~ /^\d{4}-\d{2}-\d{2}$/){
    return $v;
  }
  if($v =~ /^\d{2}\W\d{2}\W\d{4}$/){
    $v =~ s/^(\d{2})\W(\d{2})\W(\d{4})$/$3-$2-$1/;
    return $v;
  }
  return $_[0]->formatDate($_[1],"non-US");
}

1;
