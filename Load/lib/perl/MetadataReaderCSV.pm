package ClinEpiData::Load::MetadataReaderCSV;
use base qw(ClinEpiData::Load::MetadataReader);

use strict;

use ClinEpiData::Load::MetadataReader;

use Data::Dumper;

use Text::CSV_XS;

sub getLineParser {
  my ($self) = @_;

  return $self->{_line_parser};
}

sub setLineParser {
  my ($self, $lp) = @_;

  $self->{_line_parser} = $lp;
}


sub new {
  my $class = shift;
  my $self = $class->SUPER::new(@_);

  my $csv = Text::CSV_XS->new({ binary => 1, 
                               sep_char => ",", 
				   quote_char => '"', allow_loose_quotes => 1
                           }) 
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



sub adjustHeaderArray { 
  my ($self, $ha) = @_;

  my @headers = map { $_ =~ s/\"//g; $_;} @$ha;

  return \@headers;
}

1;
