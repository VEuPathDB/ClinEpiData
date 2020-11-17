package ClinEpiData::Load::Utilities::File;
require Exporter;
our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/csv2tab/;
use Text::CSV;
use open ':std', ':encoding(UTF-8)';

sub csv2tab {
  my ($file, $out, $delim) = @_;
  $delim ||= ",";
  my $csv = Text::CSV->new({binary => 1, sep_char => $delim, quote_char => '"' }) or die "Cannot use CSV: " . Text::CSV->error_diag ();  
  open(my $ifh, "<$file") or die "$@\n";
  open(my $ofh, ">$out") or die "$@\n";
  while (my $row = $csv->getline( $ifh )) {
  	$lines++;
  	#die if grep { /\t/ } @$row;
  	my @out;
  	foreach my $val (@$row){
      $val =~ s/^\x{FEFF}//;
  		$val =~ s/[\n\r\l]/ /g;
  		$val =~ s/\t/ /g;
  		push(@out, $val);
  	}
  	printf $ofh ("%s\n", join("\t", @$row));
  }
  close($ifh);
  close($ofh);
}

1;
