"""Exclusive operation lock and mirrored, fsynced, append-only private journals."""
import fcntl
import os
import re
import stat
from pathlib import Path
from interface import Refused, need
from inventory import canonical, digest, private_parents

MAX_ARTIFACT = 256 * 1024 * 1024
MAX_PHASE_BYTES = 512 * 1024 * 1024
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z')

def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try: os.fsync(fd)
    finally: os.close(fd)

def directory(path, expected=None):
    path = Path(path); private_parents(path)
    s = path.lstat()
    need(path.resolve() == path and stat.S_ISDIR(s.st_mode) and s.st_uid == s.st_gid == 0 and stat.S_IMODE(s.st_mode) == 0o700, 'journal_directory')
    if expected is not None:
        need((s.st_dev, s.st_ino) == (expected['dev'], expected['ino']), 'journal_directory_rebound')
    return {'dev': s.st_dev, 'ino': s.st_ino}

def readback(fd, name, expected):
    child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=fd)
    with os.fdopen(child, 'rb') as stream:
        a = os.fstat(stream.fileno())
        need(stat.S_ISREG(a.st_mode) and a.st_uid == a.st_gid == 0 and a.st_nlink == 1 and stat.S_IMODE(a.st_mode) == 0o600 and a.st_size == len(expected), 'journal_file_metadata')
        value = stream.read(MAX_ARTIFACT + 1); z = os.fstat(stream.fileno()); now = os.stat(name, dir_fd=fd, follow_symlinks=False)
    need(value == expected and all(getattr(a,k) == getattr(z,k) == getattr(now,k) for k in ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns')), 'journal_readback')

def new_file(path, raw):
    need(type(raw) is bytes and len(raw) <= MAX_ARTIFACT, 'artifact_bound')
    parent = Path(path).parent; expected = directory(parent)
    fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        s = os.fstat(fd); need((s.st_dev,s.st_ino) == (expected['dev'],expected['ino']), 'journal_parent_changed')
        out = os.open(Path(path).name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=fd)
        with os.fdopen(out, 'wb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.fsync(fd); readback(fd, Path(path).name, raw); directory(parent, expected)
    finally: os.close(fd)

def new_directory(path,parent_identity=None):
    path=Path(path);parent=path.parent;before=directory(parent,parent_identity)
    fd=os.open(parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:
        held=os.fstat(fd);need((held.st_dev,held.st_ino)==(before['dev'],before['ino']),'journal_parent_changed')
        os.mkdir(path.name,0o700,dir_fd=fd);os.fsync(fd);directory(parent,before)
        child=os.open(path.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
        try:
            identity=os.fstat(child);actual=directory(path)
            need((identity.st_dev,identity.st_ino)==(actual['dev'],actual['ino']),'journal_directory_rebound')
            return actual
        finally:os.close(child)
    finally:os.close(fd)

class SharedLock:
    def __init__(self, record):
        self.path = Path(record['path']); self.identity = record['identity']; self.fd = None
    def acquire(self):
        private_parents(self.path)
        fd = os.open(self.path, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        try:
            s = os.fstat(fd)
            need(stat.S_ISREG(s.st_mode) and s.st_uid == s.st_gid == 0 and s.st_nlink == 1 and stat.S_IMODE(s.st_mode) == 0o600 and (s.st_dev,s.st_ino) == (self.identity['dev'],self.identity['ino']), 'shared_lock_identity')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.fd = fd; self.check(); return self
        except BaseException:
            self.fd = None; os.close(fd); raise
    def check(self):
        need(self.fd is not None, 'shared_lock_not_held')
        s = self.path.lstat(); z = os.fstat(self.fd)
        need(stat.S_ISREG(s.st_mode) and s.st_uid == s.st_gid == 0 and s.st_nlink == 1 and stat.S_IMODE(s.st_mode) == 0o600 and (s.st_dev,s.st_ino) == (z.st_dev,z.st_ino) == (self.identity['dev'],self.identity['ino']), 'shared_lock_rebound')
    def close(self):
        if self.fd is not None: os.close(self.fd); self.fd = None
    def __enter__(self): return self.acquire()
    def __exit__(self,*_): self.close()

class DualJournal:
    """No media fallback, overwrite, retry or rollback of a partial journal."""
    def __init__(self, roots, operation, binding):
        need(len(roots) == 2 and roots[0]['identity']['dev'] != roots[1]['identity']['dev'], 'independent_journals')
        self.base = [(Path(r['path']),r['identity']) for r in roots]
        self.paths = []; self.operation = operation; self.binding = binding
        for path,pin in self.base:
            directory(path,pin)
            op = path / operation
            op_pin = new_directory(op)
            self.paths.append((op,op_pin))
        self.phase_paths = None; self.bytes = 0; self.count = 0; self.artifacts = {}; self.history = []; self.phase_names = set()
        self.save('operation.json',binding)
    def check(self):
        for path,pin in self.base + self.paths + (self.phase_paths or []): directory(path,pin)
        for paths,artifacts in self.history + [(self.current(),self.artifacts)]:
            for path,pin in paths:
                directory(path,pin)
                files=set(); dirs=set()
                for entry in path.iterdir():
                    mode=entry.lstat().st_mode
                    need(stat.S_ISREG(mode) or stat.S_ISDIR(mode), 'journal_entry_type')
                    (dirs if stat.S_ISDIR(mode) else files).add(entry.name)
                need(files == set(artifacts) and dirs == (self.phase_names if path in {p for p,_ in self.paths} else set()), 'journal_inventory_changed')
                for name,record in artifacts.items():
                    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                    try: readback(fd,name,record['raw'])
                    finally: os.close(fd)
    def current(self): return self.phase_paths if self.phase_paths is not None else self.paths
    def begin(self,phase):
        need(NAME.fullmatch(phase) is not None, 'phase_name')
        self.check()
        self.history.append((self.current(),self.artifacts))
        self.phase_paths = None; self.artifacts = {}
        phase_paths=[]
        for path,pin in self.paths:
            directory(path,pin); child=path/phase; phase_paths.append((child,new_directory(child)))
        self.phase_paths=phase_paths
        self.phase_names.add(phase)
        self.save('intent.json',dict(self.binding,phase=phase,status='started',automatic_retry=False))
    def save(self,name,value): return self.save_bytes(name,canonical(value))
    def save_bytes(self,name,raw):
        need(type(name) is str and NAME.fullmatch(name) is not None and type(raw) is bytes and name not in self.artifacts and len(raw) <= MAX_ARTIFACT and self.bytes+len(raw) <= MAX_PHASE_BYTES and self.count < 4096, 'journal_artifact_bound')
        self.check()
        for path,pin in self.current():
            directory(path,pin); new_file(path/name,raw)
        # Publish the in-memory receipt only after BOTH media were acknowledged.
        self.artifacts[name]={'sha256':digest(raw),'bytes':len(raw),'raw':raw}; self.bytes+=len(raw); self.count+=1
        self.check(); return digest(raw)
    def output_hashes(self):
        self.check(); return {n:r['sha256'] for n,r in self.artifacts.items()}

    def read_phase_bytes(self,phase,name,pin):
        """Return only previously acknowledged bytes after BOTH media revalidate."""
        need(type(phase)is str and NAME.fullmatch(phase)is not None and type(name)is str and NAME.fullmatch(name)is not None,'journal_read_name')
        self.check()
        matches=[artifacts[name]for paths,artifacts in self.history+[(self.current(),self.artifacts)]if paths and paths[0][0].name==phase and name in artifacts]
        need(len(matches)==1 and matches[0]['sha256']==pin,'journal_read_unbound')
        return matches[0]['raw']
