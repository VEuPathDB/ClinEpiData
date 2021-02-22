package ClinEpiData::Load::PromoteReader;
use base qw(ClinEpiData::Load::GenericReader);
1;

package ClinEpiData::Load::PromoteReader::OutputReader;
use base qw(ClinEpiData::Load::GenericReader::OutputReader);

1;
package ClinEpiData::Load::PromoteReader::DeliveryReader;
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);

use strict;
use warnings;
use Data::Dumper;

sub  rowMultiplier {
  my ($self, $row) = @_;
  my $mdfile = $self->getMetadataFileLCB();
  return [$row] if $mdfile ne 'bc-3 mothers delivery database final';
  my @rows;
  foreach my $inc ( 1 .. 7 ){
  # id+hospdate+ldate1+ltimehrs+ltimemin
    my %clone;
    map { $clone{$_} = $row->{$_} } grep { !/ldate|ltime/ } keys %$row;
    foreach my $var (qw/ldate ltimehrs ltimemin lampm/){
      my $varN = sprintf("%s%d", $var, $inc);
      $clone{$var} = $row->{$varN} || "na";
    }
    push(@rows, \%clone);
  }
  return \@rows;
}

1;
