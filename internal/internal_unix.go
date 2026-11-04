//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris
// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris

package internal

import (
	"os"
	"syscall"
)

// Fileinfo returns syscall fields from a FileInfo object.
func Fileinfo(fi os.FileInfo) (uid, gid int) {
	if fi == nil {
		return -1, -1
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return -1, -1
	}
	return int(stat.Uid), int(stat.Gid)
}

func fixRootDirectory(p string) string {
	return p
}

// Fsync is fsync(2), which on macOS leaves the drive's cache unflushed where
// (*os.File).Sync issues F_FULLFSYNC and stalls every writer on the drive.
func Fsync(f *os.File) error {
	return syscall.Fsync(int(f.Fd()))
}

// FsyncUploadDir syncs a directory with Fsync. It is for upload files, which a
// power cut may lose because they are rebuilt from the replica. The compacted
// log and a restored database's directories keep FsyncDir.
func FsyncUploadDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := Fsync(dir); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

// FsyncDir syncs a directory so a preceding rename within it is durable.
func FsyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}
