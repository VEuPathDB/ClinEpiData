#!/usr/bin/env perl
use strict;
use warnings;

use lib "$ENV{GUS_HOME}/lib/perl";

use Getopt::Long;
use GUS::Model::Core::UserInfo;
use GUS::Supported::GusConfig;
use GUS::ObjRelP::DbiDatabase;
use Config::Std;
use Digest::MD5 qw/md5_hex/;
use Data::Dumper;

my ($auto,$datasetName, $iniFile, $undo, $workDir, $cache);
my %options = (
  'a|autoMode!' => \$auto, 'w|workDir=s' => \$workDir, 'd|datasetName=s' => \$datasetName, 'c|cache=s' => \$cache, 'u|undo!' => \$undo, 'i|ini=s' => \$iniFile);

unless(@ARGV){
  printf("\t%s\n", join("\n\t", keys %options));
  exit;
}

GetOptions(%options);

$cache ||= '.updateStudyCharacteristics.ini';

my $gusconfig = GUS::Supported::GusConfig->new();

my $dbVendor = $gusconfig->getDatabaseVendor;
my $dsn = $gusconfig->getDbiDsn();
my $db = GUS::ObjRelP::DbiDatabase->new($dsn,
                                        $gusconfig->getDatabaseLogin(),
                                        $gusconfig->getDatabasePassword(),
                                        0,0,1,
                                        $gusconfig->getCoreSchemaName());

my $dbh = $db->getQueryHandle(0);

########################################################################################

my $login = $gusconfig->getUserName();
my $ui = GUS::Model::Core::UserInfo->new({login => $login});
$ui->retrieveFromDB();
my $userId = $ui->getId();

########################################################################################

# fetch all 

my %name2study;

my $sql = <<SQL;
SELECT ed.name NAME, s1.study_id study_id, s1.external_database_release_id
FROM study.study s1
LEFT JOIN study.study s0 ON s1.INVESTIGATION_ID=s0.STUDY_ID
LEFT JOIN sres.EXTERNALDATABASERELEASE edr ON s1.EXTERNAL_DATABASE_RELEASE_ID=edr.EXTERNAL_DATABASE_RELEASE_ID
LEFT JOIN sres.EXTERNALDATABASE ed ON edr.EXTERNAL_DATABASE_ID=ed.EXTERNAL_DATABASE_ID
WHERE s0.STUDY_ID!=s1.STUDY_ID
SQL

my $results = selectHashRef($dbh,$sql);

if($datasetName && !$results->{$datasetName}){
  printf STDERR ("Study %s is not currently loaded. These are loaded:\n%s\n\t", $datasetName, join("\n\t",sort keys %{$results}));
  exit;
}


if($workDir && -d $workDir){ chdir($workDir) }

my $prev = {};
if(-f $cache){
  ($prev) = readConfig($cache); 
}

my ($curr,$config) = readConfig($iniFile);

while(my ($study,$md5) = each %$curr){
  if(!defined($prev->{$study}) || $prev->{$study} ne $md5){
    printf STDERR "Update needed: $study\n";
    my $cmd = sprintf("ga ApiCommonData::Load::Plugin::InsertStudyCharacteristics  --datasetName %s --file /home/jaycolin/workspace/clinepi/gus_home/ontology/General/study_classifications/ini --owlFile /home/jaycolin/workspace/clinepi/gus_home/ontology/release/production/classifications.owl --commit > logs/%s 2>&1", $study, $study);
    `$cmd`;
  }
  else{
  # printf STDERR "$study is up to date\n";
  }
}

write_config(%$config, $cache);

if($undo){
}





########################################################################################
#
sub selectHashRef {
  my ($dbh, $sql, $args) = @_;
  my $sth = $dbh->prepare($sql);
  if(defined($args)){ $sth->execute(@$args) }
  else { $sth->execute() }
  my @cols = @{$sth->{NAME}}; 
# printf STDERR ("%s\n", join(", ", @cols));
  return $sth->fetchall_hashref($cols[0]);
}

sub readConfig {
  my ($ini) = @_;
  my %checksums;
  my @inifiles;
  my %cfg;
  if(-d $ini){ # a directory containing .ini files
    opendir(DH, "$ini") or die "Cannot open directory $ini$!\n";
    @inifiles = map { "$ini/$_" } grep { /.+\.ini$/i } readdir(DH);
  }
  elsif(-f $ini){
    @inifiles =($ini);
  }
  foreach my $inifile (@inifiles){
    read_config($inifile, %cfg);
    while( my ($datasetName,$chars) = each %cfg ){
      $checksums{$datasetName} = md5_hex(Dumper $chars);
    }
  }
  return \%checksums, \%cfg;
}

1;
