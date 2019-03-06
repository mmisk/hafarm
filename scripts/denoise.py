#!/usr/bin/python
import os, shutil
import sys, os
import time
import shutil
import tempfile
from optparse import OptionParser

REZ_FOUND = False

if not os.getenv("REZ_CONFIG_FILE", None):
    try:
        import rez
        REZ_FOUND = True
    except ImportError, e:
       pass
else:
    from glob import glob
    rez_path = os.environ['REZ_CONFIG_FILE']
    rez_path = os.path.dirname(rez_path)
    rez_candidate = os.path.join(rez_path, "lib64/python2.7/site-packages/rez-*.egg")
    rez_candidate = glob(rez_candidate)
    if rez_candidate:
        sys.path.append(rez_candidate[0])
        import rez
        REZ_FOUND = True



DENOISERS = ('altus', 'bcd', 'iodn')



ALTUS_CMD_TEMPLATE =  " --{pass_}-0=$pass_one::{aov} --{pass_}-1=$pass_two::{aov}"




def parseOptions(argv):
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    #Options:
    parser.add_option("-i",   "--pass_one",       dest="pass_one",      action="store",      default='*.exr', help="Image's pattern (first pass).")
    parser.add_option("-j",   "--pass_two",       dest="pass_two",      action="store",      default='*.exr', help="Image's pattern to proceed (second pass).")
    parser.add_option("-s",   "--start_frame",    dest="start_frame",   action="store",      default=0,       help="Start frame")
    parser.add_option("-e",   "--end_frame",      dest="end_frame",     action="store",      default=1,       help="End frame")
    parser.add_option("-f",   "--frame_radius",   dest="frame_radius",  action="store",      default=3,       help="Altus frame radius.")
    parser.add_option("",     "--aov_names",      dest="aov_names",     action="store",      default='pos=P,nrm=N',   help="AOV names mapping between EXR and denoiser: pos=P,nrm=N")    
    parser.add_option("-o",   "--output",         dest="output",        action="store",      default='',       help="AOV names passed to Altus.")
    parser.add_option("",     "--denoiser",       dest="denoiser",      action="store",      default='altus', help="Denoising backend: {backends}".format(backends=','.join(DENOISERS)))
        

    (opts, args) = parser.parse_args(argv)
    return opts, args, parser



def run_rez_shell(cmd,  vars, rez_pkg=None):
    """Runs provided command inside rez-resolved shell.

        Args:
            cmd_key (str): Key in self defined dict of shell commands
                or custom command
            vars   (dict): Dictionary of command line args names and values
                to be replaced inside command. This usually is: input, output,
                but could be anything else, which will be recognized by command line.

        Returns:
            pid: Process object of subshell.

        Note: pid runs in separate process and needs to be waited with wait()
            command outside this function, if commad takes time.
    """
    from rez.resolved_context import ResolvedContext

    def resolve_vars(cmd, vars):
        """Expands vars into cmd."""
        from string import Template
        try:
            template = Template(cmd)
            convert_command = template.substitute(vars)
        except:
            print "[ERROR] can't expand %s: %s" % (vars, cmd)
            return None
        return convert_command

    
    command = resolve_vars(cmd, vars)
    print command

    context = ResolvedContext( rez_pkg )
    rez_pid = context.execute_command(command)

    return rez_pid



def main():
    # Options:
    opts, args, parser = parseOptions(sys.argv[1:])

    parms = {'pass_one':     opts.pass_one,
             'pass_two':     opts.pass_two,
             'start_frame':  opts.start_frame,
             'end_frame':    opts.end_frame,
             'frame_radius': opts.frame_radius,
             'output':       opts.output,
             }

    aov_mappings  = [aov.split("=") for aov in opts.aov_names.split(',')]
    aov_argv = ''
    for mapping in aov_mappings:
        pass_, aov = mapping
        tmp = ALTUS_CMD_TEMPLATE.format(pass_=pass_, aov=aov)
        aov_argv += tmp
    
    altus_command = 'altus-cli -s $start_frame -e $end_frame -f $frame_radius --rgb-0=$pass_one --rgb-1=$pass_two {0} --out-path=$output'.format(aov_argv)
    popen = run_rez_shell(altus_command, parms, rez_pkg=['altus'])
    print popen.communicate()


if __name__ == "__main__": main()