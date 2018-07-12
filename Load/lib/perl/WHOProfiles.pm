
package ClinEpiData::Load::WHOProfiles;
use base qw(CBIL::TranscriptExpression::DataMunger::NoSampleConfigurationProfiles);

# Use if you have a tab file and don't want to code the samples property in the configuration file
# The output file will equal the input file;  You must specify whether or not to calculate the percentiles

use strict;

use CBIL::TranscriptExpression::Error;

use Data::Dumper;

use File::Basename;

use File::Temp;


sub new {
  my ($class, $args) = @_; 

  my $mainDirectory = $args->{mainDirectory};

  my $inputfile = $mainDirectory. "/" . $args->{inputFile};

  my $transposedfile = &transposeFile($inputfile,$mainDirectory);
 

  $args->{inputFile} = $transposedfile;

  $args->{sourceIdType} = "literal";
  my $self = $class->SUPER::new($args) ;          
  
  
  return $self;

}


sub transposeFile {

    my ($file,$mainDirectory)=@_;
    my $outputFile = "Transposed_" . basename($file);

    open(FH, "<$file" ) or die "Couldn't open file $file for reading, $!"; 
    open(OUT,">$mainDirectory/$outputFile" ) or die "Couldn't open file $mainDirectory/$outputFile for writing, $!"; 

    my @transposed;
  
    while ( my $linedata = <FH> ) {                                       
	chomp($linedata);
	my @row = split("\t", $linedata);
	for(my $i=0;$i<scalar @row; $i++){
	    push @{$transposed[$i]}, $row[$i];
	}
   
    }
    close  FH;


    for my $new_row (@transposed) {
   
	next if ($new_row->[0] eq "L" || $new_row->[0] eq "M" || $new_row->[0] eq "S" || $new_row->[0] eq "SD");
	print OUT join("\t", @$new_row) . "\n";	
    
    }
    close OUT;

    #return "$mainDirectory/". "Transposed_" . basename($file);
    return  $outputFile;
}



sub munge {
  my ($self) = @_;
  
  $self->SUPER::munge();

}


1;
 




























