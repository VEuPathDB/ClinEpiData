#!/usr/bin/env perl
use strict;
use warnings;

use Digest::SHA qw/sha1_hex/;
use Config::Std; # for workflow.prop
use Getopt::Long qw/:config no_ignore_case/;

my ($execute);
GetOptions ( 'x|execute!' => \$execute );
my ($datasetName, $rel, $test) = @ARGV;

unless( $ENV{WORKFLOW_DIR} ){ print "WORKFLOW_DIR not set\n"; exit 1; }
read_config( $ENV{WORKFLOW_DIR} . "/config/workflow.prop", my %config );
my $version = $config{''}->{version};
my $proj = $config{''}->{name};

unless(length($version)){ print "Cannot read version from WORKFLOW_DIR/config/workflow.prop"; exit 1; }

my $dsId = sha1_hex($datasetName);

printf STDERR "Syncing $datasetName $dsId\n";

my $downloadSource = "/eupath/data/apiSiteFilesStaging/$proj/$version/real/downloadSite/$proj/release-CURRENT/$dsId";

if(-d $downloadSource){ 
  my @targets = (
#    "elm:/eupath/data/apiSiteFilesStaging/$proj/4.0/real/downloadSite/$proj/release-CURRENT/",
#   sprintf(  "elm:/eupath/data/apiSiteFiles/downloadSite/$proj/release-%s/",$rel),
    sprintf(  "/eupath/data/apiSiteFiles/downloadSite/$proj/release-%s",$rel),
#    sprintf("melon:/var/www/Common/apiSiteFilesMirror/downloadSite/$proj/release-%s/",$rel)
  );
  foreach my $target (@targets){
    my $cmd = sprintf("rsync -r -i --delete %s %s %s",
      $test ? "-n" : "",
      $downloadSource,
      $target);
    printf ("$cmd\n");
    if($execute){
      my $status = system($cmd);
      die if $status;
    }
    # rename with release number
    # $cmd = "for file in `ls $target/$dsId`; do mv \'$target/$dsId/\$file\' \'$target/$dsId/rls00${rel}_\$file\'; done";
    $cmd = "rename $dsId/ $dsId/rls00${rel}_ $target/$dsId/*";
    printf ("$cmd\n");
    if($execute){
      my $status = system($cmd);
      die if $status;
    }
    $cmd = "ls -1 $target/$dsId/";
    if($execute){
      my $status = system($cmd);
      die if $status;
    }
  }
}
else {
  printf STDERR ("Directory not found: $downloadSource\n");
}
=cut
my $webServicesSource = "/eupath/data/apiSiteFilesStaging/$proj/4.0/real/webServices/$proj/release-CURRENT/$datasetName";

if(-d $webServicesSource){ 
  my @targets = (
    "elm:/eupath/data/apiSiteFilesStaging/$proj/4.0/real/webServices/$proj/release-CURRENT/",
    sprintf(  "elm:/eupath/data/apiSiteFiles/webServices/$proj/release-%s/",$rel),
    sprintf("melon:/var/www/Common/apiSiteFilesMirror/webServices/$proj/release-%s/",$rel)
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
