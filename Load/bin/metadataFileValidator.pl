#!/usr/bin/perl
#
# ISASimple metadata file validator
#
# Input args: list of files
# Exit code: 0 success, 1+ number of failurs
# STDERR lists PASS/FAIL + failure messages
#
# Validator tests for:
# - Supported file format / extension
# - File exists
# - Can be parsed by Perl package Text::CSV_XS (same as Text::CSV)
# - Minimum number of columns
#   - failure may indicate parse error, e.g. extention:delimiter mismatch
# - Minimum number of rows
#

use strict;
use warnings;
use lib "$ENV{GUS_HOME}/lib/perl";
use File::Basename;
use Text::CSV_XS;
use Data::Dumper;

my %FORMAT = (
  '.TXT' => "\t",
  '.TSV' => "\t",
  '.CSV' => ",",
);

# args: list of files
# or, get list from stdin
#
my @files = @ARGV;
unless(@files){
  while(<>){
    chomp;
    push(@files, $_);
  }
}

unless(@files){
  printf STDERR ("No files to validate\n");
  exit 0
}

# print Dumper \@files;

my $exitcode = 0;

my $results = { FAIL => {}, PASS => {}};

foreach my $filepath (@ARGV){
  my ($path, $name, $ext) = fileparse($filepath, qr/\..*/);
  my $file = basename($filepath);
  unless( $ext && $FORMAT{ uc($ext) } ){
    $results->{FAIL}->{$file} = "Unsupported file extension $ext";
    # printf STDERR ("FAIL $file Unsupported file extension $ext\n");
    next;
  }
  my $delim = $FORMAT{uc($ext)};
  # printf STDERR ("OK $file, extension $ext expecting delimiter [$delim]\n");
  unless( -e $filepath ){
    $results->{FAIL}->{$file} = "File not found";
    next;
  }

  my $csv = Text::CSV_XS->new({binary => 1, sep_char => $delim, quote_char => '"' }) or die "Cannot use CSV: " . Text::CSV->error_diag ();  
  my $ifh;
  unless( open($ifh, "<$filepath")){
    $results->{FAIL}->{$file} = "Cannot read file: $@";
    next;
  }
  my $headers = $csv->getline( $ifh );

  if(@$headers < 2 ){ ## check headers
    $results->{FAIL}->{$file} = "Only one column header found; check file format and delimiter";
    close($ifh);
    next;
  }
  my $lines = 0;
  eval {
    while($csv->getline( $ifh )) { $lines++ }
  };
  unless($lines){ ## Text::CSV_XS threw an error
    # printf STDERR ("FAIL $file parse error: File is empty (0 lines of data\n");
    $results->{FAIL}->{$file} = "Empty (0 lines of data";
  }
  else {
    $results->{PASS}->{$file} = "OK";
  }
  close($ifh);
}

while( my ($file, $message) = each %{$results->{FAIL}}){
  printf("%s failed validation: %s\n", $file, $message);
  $exitcode++;
}
while( my ($file, $message) = each %{$results->{PASS}}){
  printf("%s passed validation: %s\n", $file, $message)
}

exit $exitcode;
