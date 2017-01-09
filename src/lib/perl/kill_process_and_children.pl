#!XXX_PERL_PATH_XXX

use strict;
use warnings;

use Sys::Hostname;

if ($#ARGV == -1)
{
	print "Usage: kill_process_and_children <process name>\n";
	exit 1;
}

my (@columns);
my (%processes);
my ($user, $pid, $ppid, $command);

my $filename = $ARGV[0];
my $hostname = hostname;

my $myScriptName="kill_process_and_children.pl";
my $workerScript="TwoLevelParallel";
my $skfsLauncherScript="CheckSKFS.sh";
my $starterScript="SKAdmin.sh";

open(PS, "ps -eo user,pid,ppid,command |");
my @ps = <PS>;
close(PS);


# First pass to grab the parents.
for (@ps)
{
	next if (/gvim/ or /$$/ or !/\s$filename\s/ or /$myScriptName\s/ or /$workerScript/ or /$starterScript/ or /$skfsLauncherScript/ );
	
	@columns = split / +/;

	$user = $columns[0];
	$pid = $columns[1];
	$ppid = $columns[2];
	$command = $columns[3];

	kill_process($pid, \%processes);
}

recursive_kill(%processes);

sub recursive_kill
{
	my (%parentList) = @_;
	my %newParentList;

	if (keys %parentList == 0 )
	{
		return;
	}
	
	for (@ps)
	{
		@columns = split / +/;
	
		$pid = $columns[1];
		$ppid = $columns[2];
	
		if (defined $parentList{$ppid})
		{
			kill_process($pid, \%newParentList);
		}
	}

	recursive_kill(%newParentList);
}

sub kill_process
{
	my $pid = shift;
	my $processes = shift;
	
	print "Killing process $pid on $hostname ... \n";

	if (-e "/proc/$pid")
	{
		system("kill -9 $pid");
		print "done!\n";
		$processes->{$pid} = 1;
	}
	else
	{
		print "process already dead.\n";
	}
}
