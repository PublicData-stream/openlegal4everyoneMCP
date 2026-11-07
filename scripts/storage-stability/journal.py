"""Exclusive operation lock and mirrored, fsynced, append-only private journals."""
import fcntl
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from interface import Refused, need
from inventory import canonical, digest, private_parents

MAX_ARTIFACT = 256 * 1024 * 1024
MAX_PHASE_BYTES = 512 * 1024 * 1024
MAX_RESERVATION_RECEIPT = 4096
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
        # Size is already pinned above. One extra byte detects growth without
        # allocating the global 256 MiB allowance for every small history file.
        value = stream.read(min(len(expected) + 1, MAX_ARTIFACT + 1)); z = os.fstat(stream.fileno()); now = os.stat(name, dir_fd=fd, follow_symlinks=False)
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

@dataclass(frozen=True, eq=False, slots=True)
class _Reservation:
    journal: object
    owner_pid: int
    phase: tuple
    label: str
    records: int
    byte_limit: int
    start_count: int
    start_bytes: int
    begin_name: str
    begin_raw: bytes
    finish_name: str
    finish_raw: bytes

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
        self._reservation_not_poisoned()
        try: self._check()
        except BaseException:
            self._poison_reservation()
            raise
    def _check(self):
        if getattr(self,'_reservation',None) is not None:
            self._reservation_phase(self._reservation)
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
        try:
            self._reservation_not_poisoned()
            need(getattr(self,'_reservation',None) is None,'journal_reservation_active')
        except BaseException:
            self._poison_reservation()
            raise
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
    def save(self,name,value,*,reservation=None):
        try: return self.save_bytes(name,canonical(value),reservation=reservation)
        except BaseException:
            self._poison_reservation()
            raise
    def save_bytes(self,name,raw,*,reservation=None):
        return self._save_bytes(name,raw,reservation,None)
    def _save_bytes(self,name,raw,reservation,metadata):
        try:
            self._reservation_not_poisoned()
            active=getattr(self,'_reservation',None)
            if active is not None:
                self._reservation_token(reservation)
            else: need(reservation is None,'journal_reservation_token')
            need(type(name) is str and NAME.fullmatch(name) is not None and type(raw) is bytes and name not in self.artifacts and len(raw) <= MAX_ARTIFACT and self.bytes+len(raw) <= MAX_PHASE_BYTES and self.count < 4096, 'journal_artifact_bound')
            if active is not None:
                if metadata is None:
                    need(name not in (active.begin_name,active.finish_name),'journal_reservation_metadata')
                else:
                    need((metadata=='begin' and name==active.begin_name and raw==active.begin_raw)
                         or (metadata=='finish' and name==active.finish_name and raw==active.finish_raw),
                         'journal_reservation_metadata')
                tail_records=0 if metadata=='finish' else 1
                tail_bytes=0 if metadata=='finish' else len(active.finish_raw)
                need(self._reservation_records+1+tail_records<=active.records
                     and self._reservation_bytes+len(raw)+tail_bytes<=active.byte_limit,
                     'journal_reservation_bound')
            self.check()
            for path,pin in self.current():
                directory(path,pin); new_file(path/name,raw)
            # Publish the in-memory receipt only after BOTH media acknowledged.
            self.artifacts[name]={'sha256':digest(raw),'bytes':len(raw),'raw':raw}; self.bytes+=len(raw); self.count+=1
            if active is not None:
                self._reservation_records+=1; self._reservation_bytes+=len(raw)
            self.check(); return digest(raw)
        except BaseException:
            self._poison_reservation()
            raise

    def _reservation_not_poisoned(self):
        need(not getattr(self,'_reservation_poisoned',False),'journal_reservation_poisoned')
    def _poison_reservation(self):
        if getattr(self,'_reservation',None) is not None: self._reservation_poisoned=True
    def _phase_identity(self):
        return tuple((str(path),pin['dev'],pin['ino']) for path,pin in self.current())
    def _reservation_phase(self,token):
        need(token.phase==self._phase_identity()
             and type(self.count) is int and self.count==token.start_count+self._reservation_records
             and type(self.bytes) is int and self.bytes==token.start_bytes+self._reservation_bytes,
             'journal_reservation_phase_or_accounting')
    def _reservation_token(self,token):
        need(type(token) is _Reservation and getattr(self,'_reservation',None) is token
             and token.journal is self and token.owner_pid==os.getpid(),'journal_reservation_token')
        self._reservation_phase(token)

    def reserve(self,label,records,byte_limit):
        """Exclusive same-process input scope; not a cross-fork budget/authority.

        Begin/finish receipts consume the allowance. Unused capacity is released
        only by a successful finish. An interrupted scope cannot be resumed.
        """
        try:
            self._reservation_not_poisoned()
            need(getattr(self,'_reservation',None) is None,'journal_reservation_active')
            need(type(label) is str and NAME.fullmatch(label) is not None
                 and label not in getattr(self,'_reservation_labels',set()),'journal_reservation_label')
            need(type(records) is int and 2<=records<=4096
                 and type(byte_limit) is int and 0<=byte_limit<=MAX_PHASE_BYTES
                 and type(self.count) is int and type(self.bytes) is int
                 and self.count+records<=4096 and self.bytes+byte_limit<=MAX_PHASE_BYTES,
                 'journal_reservation_capacity')
            self.check()
            phase=self._phase_identity(); prefix='reservation-'+digest(label.encode())
            begin_name=prefix+'-begin.json';finish_name=prefix+'-finish.json'
            common={'operation_id':self.operation,'label':label,'owner_pid':os.getpid(),
                    'phase':self.current()[0][0].name,'records':records,'byte_limit':byte_limit}
            begin_raw=canonical(dict(common,kind='openlegal_journal_reservation_begin'))
            finish_raw=canonical(dict(common,kind='openlegal_journal_reservation_finished',
                                      begin={'name':begin_name,'sha256':digest(begin_raw)}))
            need(len(begin_raw)<=MAX_RESERVATION_RECEIPT and len(finish_raw)<=MAX_RESERVATION_RECEIPT
                 and len(begin_raw)+len(finish_raw)<=byte_limit,'journal_reservation_overhead')
            token=_Reservation(self,os.getpid(),phase,label,records,byte_limit,self.count,self.bytes,
                               begin_name,begin_raw,finish_name,finish_raw)
            self._reservation=token; self._reservation_records=0;self._reservation_bytes=0
            self._reservation_labels=set(getattr(self,'_reservation_labels',set()))|{label}
            self._save_bytes(begin_name,begin_raw,token,'begin')
            return token
        except BaseException:
            self._poison_reservation()
            raise

    def finish_reservation(self,token):
        try:
            self._reservation_not_poisoned();self._reservation_token(token);self.check()
            sha=self._save_bytes(token.finish_name,token.finish_raw,token,'finish')
            self.check()
            ref={'phase':self.current()[0][0].name,'name':token.finish_name,'sha256':sha}
            self._reservation=None
            return ref
        except BaseException:
            self._poison_reservation()
            raise

    def fail_reservation(self,token):
        try:
            self._reservation_not_poisoned();self._reservation_token(token)
        finally: self._poison_reservation()
    def output_hashes(self):
        self.check(); return {n:r['sha256'] for n,r in self.artifacts.items()}

    def read_phase_bytes(self,phase,name,pin):
        """Return only previously acknowledged bytes after BOTH media revalidate."""
        need(type(phase)is str and NAME.fullmatch(phase)is not None and type(name)is str and NAME.fullmatch(name)is not None,'journal_read_name')
        self.check()
        matches=[artifacts[name]for paths,artifacts in self.history+[(self.current(),self.artifacts)]if paths and paths[0][0].name==phase and name in artifacts]
        need(len(matches)==1 and matches[0]['sha256']==pin,'journal_read_unbound')
        return matches[0]['raw']
