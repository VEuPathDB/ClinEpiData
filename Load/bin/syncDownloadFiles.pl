#!/usr/bin/env perl
use strict;
use warnings;

use Digest::SHA qw/sha1_hex/;
use Config::Std; # for workflow.prop

my ($datasetName, $rel, $test) = @ARGV;

unless( $ENV{WORKFLOW_DIR} ){ print "WORKFLOW_DIR not set\n"; exit 1; }
read_config( $ENV{WORKFLOW_DIR} . "/config/workflow.prop", my %config );
my $version = $config{''}->{version};

unless(length($version)){ print "Cannot read version from WORKFLOW_DIR/config/workflow.prop"; exit 1; }

my $dsId = sha1_hex($datasetName);

printf STDERR "Syncing $datasetName $dsId\n";

my $downloadSource = "/eupath/data/apiSiteFilesStaging/ClinEpiDB/$version/real/downloadSite/ClinEpiDB/release-CURRENT/$dsId";

if(-d $downloadSource){ 
  my @targets = (
#    "elm:/eupath/data/apiSiteFilesStaging/ClinEpiDB/4.0/real/downloadSite/ClinEpiDB/release-CURRENT/",
#   sprintf(  "elm:/eupath/data/apiSiteFiles/downloadSite/ClinEpiDB/release-%s/",$rel),
    sprintf(  "/eupath/data/apiSiteFiles/downloadSite/ClinEpiDB/release-%s",$rel),
#    sprintf("melon:/var/www/Common/apiSiteFilesMirror/downloadSite/ClinEpiDB/release-%s/",$rel)
  );
  foreach my $target (@targets){
    my $cmd = sprintf("rsync -r -i --delete %s %s %s",
      $test ? "-n" : "",
      $downloadSource,
      $target);
    printf ("$cmd\n");
    # rename with release number
    # $cmd = "for file in `ls $target/$dsId`; do mv \'$target/$dsId/\$file\' \'$target/$dsId/rls00${rel}_\$file\'; done";
    $cmd = "rename $dsId/ $dsId/rls00${rel}_ $target/$dsId/*";
    printf ("$cmd\n");
    printf "ls -1 $target/$dsId/";
  }
}
else {
  printf STDERR ("Directory not found: $downloadSource\n");
}
=cut
my $webServicesSource = "/eupath/data/apiSiteFilesStaging/ClinEpiDB/4.0/real/webServices/ClinEpiDB/release-CURRENT/$datasetName";

if(-d $webServicesSource){ 
  my @targets = (
    "elm:/eupath/data/apiSiteFilesStaging/ClinEpiDB/4.0/real/webServices/ClinEpiDB/release-CURRENT/",
    sprintf(  "elm:/eupath/data/apiSiteFiles/webServices/ClinEpiDB/release-%s/",$rel),
    sprintf("melon:/var/www/Common/apiSiteFilesMirror/webServices/ClinEpiDB/release-%s/",$rel)
  );
  foreach my $target (@targets){
    my $cmd = sprintf("rsync -r -i %s %s %s",
      $test ? "-n" : "",
      $webServicesSource,
      $target);
    printf STDERR ("$cmd\n");
  }
}
else {
  printf STDERR ("Directory not found: $webServicesSource\n");
}
=cut

1;
