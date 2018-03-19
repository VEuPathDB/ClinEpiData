package ClinEpiData::Load::Plugin::InsertInvestigations;

@ISA = qw(ApiCommonData::Load::Plugin::InsertInvestigations);

use ApiCommonData::Load::Plugin::InsertInvestigations;

use strict;


# ClinEpi Doesn't have existing database results so all edges are new
sub checkDatabaseProtocolApplicationsAreHandledAndMark {}

1;
