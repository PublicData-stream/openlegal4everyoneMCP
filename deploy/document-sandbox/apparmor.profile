# Install with apparmor_parser on each qualified worker node. No mount into Pods.
#include <tunables/global>
profile openlegal-document flags=(attach_disconnected,mediate_deleted) {
  #include <abstractions/base>
  deny network,
  deny capability,
  deny mount,
  deny ptrace,
  /usr/local/bin/openlegal-document-worker rix,
  /usr/lib/** mr,
  /lib/** mr,
  /opt/fonts/** r,
  /opt/tessdata/** r,
  /opt/notices/** r,
  /opt/probe/ rw,
  /opt/probe/** rw,
  /usr/share/fonts/** r,
  /etc/fonts/** r,
  /etc/ld.so.cache r,
  /proc/** r,
  /sys/fs/cgroup/** r,
  /scratch/ rw,
  /scratch/** rwk,
  /dev/null rw,
  /dev/urandom r,
  /dev/random r,
}
