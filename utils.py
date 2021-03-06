import os
import pwd
import sys
import const
import fileseq

def get_email_address(user = None):
    """Here goes anything but not current hack."""
    return get_email_address_from_uid()


def get_email_address_from_uid(uid = None):
    """Returns email address of currenly logged user. 
    FIXME: Is should depend on ldap module instead if monkey patching...
    """
    if not uid:
        uid = os.getuid()
    user = pwd.getpwuid(uid)[4]
    user = user.split()
    if len(user) == 2:
        email = user[0][0] + '.' + user[1] + '@human-ark.com'
    elif len(user) == 1:
        email = user[0][0] + '.' + user[0][1:] + '@human-ark.com'
    else:
        email = ''
    return email.lower()


def parse_frame_list(frames):
    """frames: 
            string of frames in usual form: 1 2 3 4-6 7-11:2
        return: list of frames [1,2,3,4,5,6,7,9,11]
    """
    from fileseq import FrameSet
    frames = frames.split()
    frames = ','.join(frames)
    fs = FrameSet(frames)
    return list(fs)


def get_ray_image_from_ifd(filename):
    """grep ray_image from ifd file with gnu grep tool.
    Rather hacky."""
    image_name = ''
    if sys.platform in ('linux2',):
        result = os.popen('grep -a ray_image %s' % filename).read()
        image_name = result.split()[-1]
        image_name = image_name.replace('"', '')
    return image_name


def convert_seconds_to_SGEDate(seconds):
    """Converts time in seconds to [[CC]YY]MMDDhhmm[.SS] format."""
    from datetime import datetime
    from time import localtime, strftime
    date = localtime(seconds)
    format = '%Y%m%d%H%M.%S'
    return strftime(format, date)


def convert_asctime_to_seconds(time_string):
    """Converts time in asc format to seconds"""
    from time import strptime, mktime
    format = '%a %b %d %H:%M:%S %Y'
    try:
        time_struct = strptime(time_string, format)
    except:
        return

    return mktime(time_struct)


def compute_time_lapse(start, end = None):
    """Taking two inputs in seconds, compute a difference
        and return as pretty string.
    """
    from datetime import timedelta
    from time import time
    if not end:
        end = time()
    if not isinstance(start, type(0.0)) or not isinstance(end, type(0.0)):
        return
    result = timedelta(seconds=int(end)) - timedelta(seconds=int(start))
    return str(result)


def compute_delay_time(hours, now = None):
    """Computes delay from now to now+hours. Returns time in seconds from epoch."""
    from datetime import datetime, timedelta
    from time import mktime
    if not now:
        now = datetime.now()
    delta = timedelta(hours=hours)
    delay = now + delta
    return mktime(delay.timetuple())


def compute_crop(crop_parms):
    hsize = 1.0 / crop_parms[0]
    vsize = 1.0 / crop_parms[1]
    h = crop_parms[2] % crop_parms[0]
    v = crop_parms[2] / crop_parms[0]
    left = h * hsize
    right = (1 + h) * hsize
    lower = v * vsize
    upper = (1 + v) * vsize
    return (left,
     right,
     lower,
     upper)


def padding(file, format = None, _frame = None):
    """ Recognizes padding convention of a file.
    Returns: (host_specific_name, frame number, length, extension) See _formats in this module.
    TODO: Should be able to transcode between hosts. %04d <-> $F4 """
    import re
    _formats = {'nuke': '%0#d',
     'houdini': '$F#',
     'shell': '*'}
    frame, length = (None, None)
    base, ext = os.path.splitext(file)
    if not base[-1].isdigit():
        return (os.path.splitext(file)[0],
         0,
         0,
         os.path.splitext(file)[1])
    l = re.split('(\\d+)', file)
    if l[-2].isdigit():
        frame, length = int(l[-2]), len(l[-2])
    if format in _formats.keys():
        format = _formats[format].replace('#', str(length))
        return (''.join(l[:-2]) + format + ext,
         frame,
         length,
         ext)
    if _frame:
        return (''.join(l[:-2]) + str(_frame).zfill(length) + ext,
         frame,
         length,
         ext)
    return (''.join(l[:-2]),
     frame,
     length,
     ext)


def parse_qacct(output, db = None):
    """Parses SGE qacct utility output to Python dictonary.
    """
    if not db:
        db = {}
    if 'frames' not in db:
        db['frames'] = {}
    frames_blocks = output.split('==============================================================')
    for block in frames_blocks[1:]:
        lines = block.split('\n')
        frame = {}
        for line in lines:
            line = line.strip()
            if not line:
                continue
            line_list = line.split()
            if len(line_list) >= 2:
                var, value = line_list[0], ' '.join(line_list[1:])
                var = var.strip()
                value = value.strip()
                frame[var] = value

        db['frames'][int(frame['taskid'])] = frame

    return db


def collapse_digits_to_sequence(frames):
    """ Given frames is a list/tuple of digits [1,2,4],  
        collapse it into [(1,2), (4,..) ...]
    """
    frames = sorted(list(frames))
    frames = [ int(x) for x in frames ]
    sequence = []
    start = True
    for f in frames:
        if start:
            part = [f, f]
            sequence += [part]
            start = False
            continue
        if f - 1 == frames[frames.index(f) - 1]:
            part[1] = f
            continue
        else:
            part = [f, f]
            sequence += [part]

    sequence = [ tuple(x) for x in sequence ]
    return sequence


def expand_sequence_into_digits(sequence_string):
    """ Given a string representing frameseq like: '1,2,3-5,6-10x2,11-20:2' 
        returns an expanded a list with suscessive frames.
    """
    assert isinstance(sequence_string, str)

    sequence = fileseq.FrameSet(sequence_string)
    return sequence



def parse_sstat(output, db = None):
    """Parses Slurm sstat utility output to Python dictonary.
    """
    if not db:
        db = {}
    if 'frames' not in db:
        db['frames'] = {}
    return db
