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
# - Can be parsed by Perl package Text::CSV
# - Minimum number of columns
#   - failure may indicate parse error, e.g. extention:delimiter mismatch
# - Minimum number of rows
#

use strict;
use warnings;
# use lib "$ENV{GUS_HOME}/lib/perl";
use File::Basename;
use Text::CSV;
use Data::Dumper;
use open ':std', ':encoding(UTF-8)';

my $MAXCOLS = 999;
my $MAXLENGTH = 1000;

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
  exit 0;
}

my $exitcode = 0;

my $results = { FAIL => {}, PASS => {}};

foreach my $filepath (@files){
  my ($path, $name, $ext) = fileparse($filepath, qr/\.[^\.]*$/);
  my $filename = basename($filepath);
  unless( $ext && $FORMAT{ uc($ext) } ){
    #testResult('FAIL', $filename, "Unsupported file extension $ext";
    testResult('FAIL', $filename, "Unsupported file extension $ext");
    next;
  }
  my $delim = $FORMAT{uc($ext)};
  unless( -e $filepath ){
    #testResult('FAIL', $filename, "File not found";
    testResult('FAIL', $filename, "File not found");
    next;
  }
  unless( -f $filepath ){
    #testResult('FAIL', $filename, "File not a plain file (may be link, pipe, directory, etc.)";
    testResult('FAIL', $filename, "File not a plain file (may be link, pipe, directory, etc.)");
    next;
  }

  if(-B $filepath){
    testResult('FAIL', $filename, "Binary file; only text is supported");
    next;
  }
  my $ifh;
  unless( open($ifh, "<$filepath")){
    testResult('FAIL', $filename, "Cannot read file: $@");
    next;
  }
  
  my $csv = Text::CSV->new({binary => 1, sep_char => $delim, quote_char => '"' }) or die "Cannot use CSV: " . Text::CSV->error_diag ();  
  my $headers = $csv->getline( $ifh );
  unless($headers) {
    testResult('FAIL', $filename, "unreadable, possibly not a plain text file");
    close($ifh);
    next;
  }

  if(@$headers < 2 ){ ## check headers
    testResult('FAIL', $filename, "Only one column header found; check file format and delimiter");
    close($ifh);
    next;
  }
  if(@$headers > $MAXCOLS ){ ## check headers
    my $numcols = scalar @$headers;
    testResult('FAIL', $filename, "Too many columns ($numcols), limit is $MAXCOLS columns");
    close($ifh);
    next;
  }

  # Validate file headers
  foreach my $header (@$headers){
    unless($header =~ m{^(.[A-Za-z][A-Za-z_.0-9]*|[A-Za-z][A-Za-z_.0-9]*)$}){
      testResult('WARN', $filename, "Illegal column header: must start with letter, or dot '.' not followed by a number, and contains only letters, numbers, underscore '_', or period '.' (illegal characters include space, dash, parentheses, etc.) - illegal characters will be automatically converted to underscore '_'");
    }
  }
  my $lines = 0;
  my %maxlen;
  eval {
    while(my $row = $csv->getline( $ifh )) {
      $lines++;
      foreach my $col (@$headers){
        my $len = length( shift(@$row) );
        $maxlen{$col} ||= $len;
        if($len > $MAXLENGTH){ 
          $maxlen{$col} = $len;
        }
      }
    }
  };
  close($ifh);
  unless($lines){ ## Text::CSV threw an error
    testResult('FAIL', $filename, "Empty (no data rows)");
  }
  else {
    foreach my $col (@$headers){
      if($maxlen{$col} > $MAXLENGTH){
        testResult('FAIL', $filename, "Column \"$col\" values have a maximum length of $maxlen{$col} characters, limit is $MAXLENGTH");
      }
    }
  }
}

while( my ($filename, $failures) = each %{$results->{FAIL}}){
  foreach my $message (@$failures){
    printf STDERR ("FAIL: %s\t%s\n", $filename, $message);
    $exitcode++;
  }
}
# while( my ($filename, $message) = each %{$results->{PASS}}){
#   printf STDOUT ("%s\t%s\tPASS\n", $filename, $message)
# }
while( my ($filename, $warnings) = each %{$results->{WARN}}){
  foreach my $message (@$warnings){
    printf STDOUT ("WARNING: %s\t%s\n", $filename, $message)
  }
}

exit ($exitcode > 0);


sub testResult {
  my ($type, $filename, $message) = @_;
  $results->{$type}->{$filename} //= [];
  push( @{ $results->{$type}->{$filename} }, $message ); 
}
