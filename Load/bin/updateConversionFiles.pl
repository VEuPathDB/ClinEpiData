#!/usr/bin/env perl

# AD-HOC UTILITY SCRIPT
# run this in a new workspace dir
# create new/ and current/
# use the dataset-owl-conversion list (columns 2-3) as STDIN
# https://github.com/VEuPathDB/ApiCommonData/blob/master/Load/ontology/clinEpi/dataset_owl_conversion.csv
# direct STDOUT to "run.sh" and run it
# compare current/ and new/
# diff -rq current/ new/
# copy updated conversion files to their original locations

use strict;
use warnings;
use File::Basename;
use Digest::MD5;

my $queryfile = $ENV{PROJECT_HOME} . "/ApiCommonData/Load/ontology/SPARQL/owl_to_conversion.rq";
while(<>){
  chomp;
  my ($owlfile, $csvfile) = split /,/;
  $owlfile = sprintf("%s/%s", $ENV{PROJECT_HOME}, $owlfile);
  $csvfile = sprintf("%s/%s", $ENV{PROJECT_HOME}, $csvfile);
  my $csvbase = basename($csvfile);
  my $owlbase = basename($owlfile);
  system(sprintf("owlConvert.pl -o %s -c %s >conv.out 2>conv.err \n", basename($owlfile, ".owl"), $csvfile));
  if(-e $owlbase) {
    my $newmd5 = getmd5($owlbase);
    my $oldmd5 = getmd5($owlfile);
    if($newmd5 ne $oldmd5){
      link( $csvfile, "current/" . $csvbase ) unless (-e "current/" . $csvbase );
      printf("robot query --use-graphs true --input %s --query %s new/%s\n", $owlfile, $queryfile, $csvbase);
    }
    unlink($owlbase);
    unlink(glob("*_settings.txt"));
    unlink($csvbase);
  }
  else {
    printf STDERR ("error owl not created: $owlfile\n");
  }
}

sub getmd5 {
	my ($file) = @_;
	my $ctx = Digest::MD5->new;
	open(my $fh, "<$file") or die "Cannot open $file:\n$!\n";
	$ctx->addfile($fh);
  close($fh);
	return $ctx->hexdigest();
}
