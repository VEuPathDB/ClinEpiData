#!/usr/bin/env perl
use strict;
use warnings;

use feature qw/switch/;
use lib $ENV{GUS_HOME} . "/lib/perl";
use ClinEpiData::Load::Utilities::File qw/csv2array tabWriter getPrefixedHeaders getValidValues getHeaders arr2csv/;
use Getopt::Long qw/:config no_ignore_case/;

my ($infile,$outfile,$index);
GetOptions( 'i|inFile=s' => \$infile, 'o|outFile=s' => \$outfile, 's|startIndex=i' => \$index);
my $uritemplate = 'http://purl.obolibrary.org/obo/CLINEPIDB_%05d';
my %newiri;

### get existing IRIs, parents

my $ont = getFromDb();

my $data = csv2array($infile);

arr2csv($data, "${infile}.bak");

printf STDERR ("Read file %s: %d lines\n output to %s\n", $infile, scalar @$data, $outfile);

my $hr = shift @$data;

my @keyindices = getKeyHeaderIndex($hr);
my ($iri,$lab,$piri,$plab,$cat) = @keyindices;


# set new piri first
foreach my $row (@$data){
  next if( $row->[$piri] ne "" );
  if($ont->{label}->{lc($row->[$plab])}){ 
    $row->[$piri] = $ont->{label}->{lc($row->[$plab])}->{iri};
    next;
  }
  # need new piri
  my $nextpiri = $newiri{$row->[$plab]};
  $nextpiri ||= sprintf($uritemplate, $index);
  unless( $newiri{$row->[$plab]} ){ $index++ } # new iri created
  $newiri{$row->[$plab]} ||= $nextpiri;
  $row->[$piri] = $nextpiri;
}
####################################
# set new iri
foreach my $row (@$data){
  next if( $row->[$iri] ne "" );
  if($ont->{label}->{lc($row->[$lab])}){ 
    $row->[$iri] = $ont->{label}->{lc($row->[$lab])}->{iri};
    next;
  }
  # need new piri
  my $nextiri = $newiri{$row->[$lab]};
  $nextiri ||= sprintf($uritemplate, $index);
  unless( $newiri{$row->[$lab]} ){ $index++ } # new iri created
  $newiri{$row->[$lab]} ||= $nextiri;
  $row->[$iri] = $nextiri;
}
####################################
#All blanks in IRI and parentIRI now filled
#Next: validate categories and set up parent hash for adding rows
####################################
my %seen;
my %category;
my %addIRI;
my %childOf;
foreach my $row (@$data){
  $seen{$row->[$iri]} = $row->[$lab]; # row exists, don't duplicate 
  $category{$row->[$iri]} = lc $row->[$cat];
  $category{$row->[$piri]} = lc $row->[$cat];
  $addIRI{$row->[$piri]} = $row->[$plab];
  $childOf{$row->[$iri]} = $row->[$piri];
}
####################################
############ validate
my $line = 0;
foreach my $row (@$data){
  $line++;
  validateRow($row, \@keyindices);
  if(($category{$row->[$iri]} !~ /^\s*$/) && ($category{$row->[$iri]} ne lc($row->[$cat]))){
    printf STDERR ("Failed validation: category mismatch at %d: {%s}: %s != %s\n",  $line,$row->[$iri],$category{$row->[$iri]}, lc $row->[$cat] );
  }
  if(($category{$row->[$piri]} !~ /^\s*$/) && ($category{$row->[$piri]} ne lc($row->[$cat]))){
    printf STDERR ("Failed validation: (parent) category mismatch at %d: %s: %s != %s\n",  $line, $row->[$piri], $category{$row->[$piri]}, lc $row->[$cat] );
  }
  $category{$row->[$iri]} = lc($row->[$cat]); # fallbacks for undefined parent
  $category{$row->[$piri]} = lc($row->[$cat]);
## Add to total ontology
  $ont->{iri}->{ $row->[$iri] } ||= { label => $row->[$lab], parent => $row->[$piri] };
  $ont->{iri}->{ $row->[$iri] }->{parent} ||= $row->[$piri];
  $ont->{label}->{ lc($row->[$lab]) } ||= { iri => $row->[$iri], parent => $row->[$piri] };
  $ont->{label}->{ lc($row->[$lab]) }->{parent} ||= $row->[$piri];
}
####################################
# write rows for all parent terms until exhausted
my $iter = 0;
my $total = scalar %addIRI;
printf STDERR ("Checking %d parent terms, adding rows as needed\n", $total);
while(scalar keys %addIRI){
  $iter++;
  printf STDERR "Adding parents: iteration $iter\n";
  foreach my $id (sort keys %addIRI){
    if($seen{$id}){
      $total--;
      printf STDERR ("Total $total -1 seen $id\n");
      delete $addIRI{$id};
    }
    my $pid = $ont->{iri}->{$id}->{parent}; # this term's parent
   #if($seen{$pid}){
   #  $total--;
   #  printf STDERR ("Total $total -1 seen $pid\n");
   #  #delete $addIRI{$pid};
   #  # both iri and parentiri rows have been written, done
   #}
    unless($pid){ # parentiri not in ontology; final, use category
      $pid = $ont->{label}->{ $category{$id} }->{parent};
      printf STDERR ("Final parent %s => %s\n", $id, $pid);
    }
    unless($pid){ # somehow no category???
      printf STDERR ("WARNING: no parent defined for $id\n");
      $pid = 'http://purl.obolibrary.org/obo/Thing';
    }
    my @newrow;
    for (my $i = 0; $i < @$hr; $i++){  push(@newrow, "") }
    $newrow[ $iri ] = $id;
    $newrow[ $lab ] = $ont->{ iri }->{ $id }->{label};
    if($pid !~ /Thing$/){ $newrow[ $piri ] =  $pid }
    $newrow[ $plab ] = $ont->{ iri }->{ $pid }->{label};
    if($id ne $pid && ! $seen{$id}){
      # different things, write row
      push(@$data, \@newrow);
    }
    $seen{$id} = 1;
    if(! $seen{$pid} && $pid !~ /Thing$/ ){
      $addIRI{$pid} = $newrow[ $plab ];
      $total++;
    }
  }
}

unshift(@$data, $hr);

## one more pass: clean up parent columns
foreach my $row (@$data){
  if( $row->[$iri] eq $row->[$piri]){ $row->[$piri] = ""; $row->[$plab] = "" }
  for(my $i = 0; $i < scalar @$row; $i++){
    $row->[$i] =~ s/^NA$//;
  }
}

arr2csv($data, $outfile);

# print all new IRIs
# while(my ($l, $i) = each %newiri){
#   printf("%s\t%s\n", $l, $i);
# }
  


###################END

sub getKeyHeaderIndex {
  my ($headerRow) = @_;
  my @hr = @$headerRow;
  my ($iri,$lab,$piri,$plab,$cat);
  for(my $i = 0; $i <= $#hr; $i++){
    given(lc($hr[$i])) {
      when("iri") { $iri = $i }
      when("label") { $lab = $i }
      when("parentiri") { $piri = $i }
      when("parentlabel") { $plab = $i }
      when("category") { $cat = $i }
    }
  }
  unless($cat){ die "Category missing" }
  return ($iri, $lab, $piri, $plab, $cat )
}

sub getFromDb {
  my $tmp = ".terms_from_db.tsv";
  if( -e $tmp){ goto USEOLD; }
  my $query =  <<SQL;
  SELECT DISTINCT 'http://purl.obolibrary.org/obo/'|| a.STABLE_ID iri, a.DISPLAY_NAME, \
  'http://purl.obolibrary.org/obo/' || a.PARENT_STABLE_ID parent_IRI, p.DISPLAY_NAME parent_label \
  FROM eda.ATTRIBUTEGRAPH a  \
  LEFT JOIN eda.attributegraph p ON a.PARENT_ONTOLOGY_TERM_ID =p.ONTOLOGY_TERM_ID AND a.STUDY_ID =p.STUDY_ID \
  WHERE a.ROW_PROJECT_ID =2 \
  ORDER BY iri
SQL
  system(sprintf('makeFileWithSql --outDelimiter="\t" --outFile %s --sql "%s"', $tmp, $query)) && die "Query failed";
USEOLD:
  my $ont = {};
  open(FH, "<$tmp") or die "Cannot read $tmp: $!\n";
  while(<FH>){
    chomp;
    my ($iri, $label, $piri, $plabel) = split("\t");
    $ont->{iri}->{$iri} = { label => $label, parent => $piri };
    $ont->{label}->{lc($label)} = { iri => $iri, parent => $piri };
  }
  close(FH);
  return $ont;
}

sub validateRow {
  my ($row, $ind) = @_;
  my @a = map { $$row[$_] } @$ind;
  foreach my $x (@a){
    if($x =~ /^\s*$/){
      printf STDERR ("Invalid row: %s\n", join(" | ", @a));
      die;
    }
  }
}
